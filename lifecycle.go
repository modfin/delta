package delta

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

// testPanicHookMu guards testPanicHook to satisfy the race detector in tests.
var testPanicHookMu sync.RWMutex

// testPanicHook is called at the start of each background goroutine iteration
// during tests. It is nil in production. Set it via setTestPanicHook in internal
// tests (package delta) to inject a panic and verify that the goroutine's
// deferred recover() catches it.
var testPanicHook func()

func setTestPanicHook(h func()) {
	testPanicHookMu.Lock()
	testPanicHook = h
	testPanicHookMu.Unlock()
}

func getTestPanicHook() func() {
	testPanicHookMu.RLock()
	h := testPanicHook
	testPanicHookMu.RUnlock()
	return h
}

func init() {
	// adding special driver for sqlite3
	// with support for specific glob match function for topic matching
	// seems like it slows things down somewhat, from ~4 µs/msg to ~6 µs/msg
	// probably hard for sqlite to optimize with indexes and so on
	sql.Register("sqlite3_delta-v",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("match_glob", globMatcher, true)
			},
		})
}

func New(uri string, op ...Op) (*MQ, error) {

	db, err := sql.Open("sqlite3_delta-v", uri)
	if err != nil {
		return nil, fmt.Errorf("could not open db, %w", err)
	}

	// Ensure db is closed on any error after sql.Open succeeds.
	var initErr error
	defer func() {
		if initErr != nil {
			_ = db.Close()
		}
	}()

	// Test the connection
	if initErr = db.Ping(); initErr != nil {
		return nil, fmt.Errorf("could not ping db, %w", initErr)
	}

	c := &MQ{
		base: &base_{
			uri: uri,
			db:  db,
			log: slog.New(discardLogger{}),

			closeOnce:     sync.Once{},
			closed:        make(chan struct{}),
			removeOnClose: false,
			streamMu:      sync.RWMutex{},
			streams:       make(map[string]*MQ),
		},
		stream: stream_{
			name:    DEFAULT_STREAM,
			written: 0,
			read:    0,

			inform: make(chan struct{}),

			subs: newGlobber[*Subscription](),

			writeMu: sync.Mutex{},
			groupMu: sync.Mutex{},
			groups:  map[string]*group{},
		},
	}
	c.base.streams[c.stream.name] = c

	ops := append([]Op{dbDefault()}, op...)

	c.base.log.Debug("[delta] applying options")
	for _, op := range ops {
		if initErr = op(c); initErr != nil {
			return nil, fmt.Errorf("could not exec op, %w", initErr)
		}
	}

	c.base.log.Debug("[delta] creating schema")
	if initErr = schema(c); initErr != nil {
		return nil, fmt.Errorf("failed in creating schema, %w", initErr)
	}

	c.base.log.Debug("[delta] optimizing database")
	if initErr = optimize(c); initErr != nil {
		return nil, fmt.Errorf("failed in optimizing, %w", initErr)
	}

	written, read, err := metrics(c.base.db, c.tbl)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("could not get written, %w", initErr)
	}

	c.base.log.Info("[delta] starting stream_ at", "written", written, "read", read, "stream_", c.stream.name)

	atomic.SwapUint64(&c.stream.written, written)
	atomic.SwapUint64(&c.stream.read, read) // Probably load some sort of read counter instead?

	c.readloop()
	c.vacuumloop()

	return c, nil
}

func (c *MQ) Stream(stream string, ops ...Op) (*MQ, error) {
	c.base.streamMu.Lock()
	defer c.base.streamMu.Unlock() // write lock: mutates base.streams

	stream, err := checkStreamName(stream)
	if err != nil {
		return nil, fmt.Errorf("could not check stream_ name, %w", err)
	}

	if _, found := c.base.streams[stream]; found {
		return c.base.streams[stream], nil
	}

	cc := &MQ{
		base: c.base,
		stream: stream_{
			name:    stream,
			written: 0,
			read:    0,

			inform: make(chan struct{}),

			writeMu: sync.Mutex{},

			subs:    newGlobber[*Subscription](),
			groupMu: sync.Mutex{},
			groups:  map[string]*group{},
		},
	}

	err = schema(cc)
	if err != nil {
		return nil, fmt.Errorf("could not create schema for namespace, %w", err)
	}

	for _, op := range ops {
		err := op(cc)
		if err != nil {
			return nil, fmt.Errorf("could not exec op, %w", err)
		}
	}

	written, read, err := metrics(cc.base.db, cc.tbl)
	if err != nil {
		return nil, fmt.Errorf("could not get metrics, %w", err)
	}
	c.base.log.Info("[delta] starting stream_ at", "written", written, "read", read, "stream_", cc.stream.name)

	atomic.SwapUint64(&cc.stream.written, written)
	atomic.SwapUint64(&cc.stream.read, read)

	// Only register the stream after full initialization succeeds, so a failed
	// Stream() call does not leave a stale, partially-initialized entry in the map.
	cc.base.streams[stream] = cc
	cc.readloop()

	return cc, nil
}

func optimize(c *MQ) error {
	return exec(c.base.db, `pragma vacuum;`, `pragma optimize;`)
}

func schema(c *MQ) error {
	return errors.Join(
		exec(c.base.db, fmt.Sprintf(base_schema, c.tbl())),
		exec(c.base.db, fmt.Sprintf(base_schema_idx, c.tbl(), c.tbl())),
		exec(c.base.db, base_metadata_schema),
	)
}

// Close closes the cache and all its namespaces
func (c *MQ) Close() error {

	defer func() {
		if c.base.removeOnClose {
			_ = c.removeStore()
		}
	}()

	c.base.closeOnce.Do(func() {
		close(c.base.closed)
	})

	c.base.streamMu.RLock()
	streams := make([]*MQ, 0, len(c.base.streams))
	for _, cc := range c.base.streams {
		streams = append(streams, cc)
	}
	c.base.streamMu.RUnlock()

	// Always close the underlying DB connection, even if bookkeeping fails.
	defer func() {
		_ = c.base.db.Close()
	}()

	var errs []error
	for _, cc := range streams {
		err := ackWritten(cc.base.db, atomic.LoadUint64(&cc.stream.written), cc.tbl)
		if err != nil {
			errs = append(errs, fmt.Errorf("could not ack written, %w", err))
			continue
		}
		w, r, err := metrics(cc.base.db, cc.tbl)
		if err != nil {
			errs = append(errs, fmt.Errorf("could not get metrics, %w", err))
			continue
		}
		c.base.log.Info("[delta] closing stream_", "stream_", cc.CurrentStream(), "written", w, "read", r)
	}

	return errors.Join(errs...)
}

func RemoveStore(uri string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(discardLogger{})
	}

	schema, uri, found := strings.Cut(uri, ":")
	if !found {
		return fmt.Errorf("could not find file in uri")
	}
	if schema != "file" {
		return fmt.Errorf("not a file uri")
	}

	file, query, _ := strings.Cut(uri, "?")

	logger.Info("[delta] remove store", "db", file, "shm", fmt.Sprintf("%s-shm", file), "wal", fmt.Sprintf("%s-wal", file))

	removeIgnoreNotExist := func(path string) error {
		err := os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	err := errors.Join(
		os.Remove(file),
		removeIgnoreNotExist(fmt.Sprintf("%s-shm", file)),
		removeIgnoreNotExist(fmt.Sprintf("%s-wal", file)),
	)
	if strings.Contains(query, "tmp=true") {
		logger.Info("[delta] remove store dir", "dir", filepath.Dir(file))
		err = errors.Join(os.Remove(filepath.Dir(file)), err)
	}
	return err
}

func (c *MQ) removeStore() error {

	select {
	case <-c.base.closed:
	default:
		return fmt.Errorf("db is not closed")
	}

	return RemoveStore(c.base.uri, c.base.log)
}

func (c *MQ) tbl() string {
	return fmt.Sprintf("_mq_delta_stream_%s", c.CurrentStream())
}

func (c *MQ) CurrentStream() string {
	return c.stream.name
}
