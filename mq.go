package delta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

type Op func(*MQ) error

func dbPragma(pragma string) Op {
	return func(c *MQ) error {
		return exec(c.base.db,
			fmt.Sprintf(`pragma %s;`, pragma),
		)
	}
}

// DBSyncOff is a helper function to set
//
//	synchronous = off
//
// this is useful for write performance but effects read performance and durability
func DBSyncOff() Op {
	return func(c *MQ) error {
		return dbPragma("synchronous = off")(c)
	}
}

// DBRemoveOnClose is a helper function to remove the database files on close
func DBRemoveOnClose() Op {
	return func(mq *MQ) error {
		mq.base.removeOnClose = true
		return nil
	}
}

// WithLogger sets the logger for the cache
func WithLogger(log *slog.Logger) Op {
	return func(c *MQ) error {
		if log == nil {
			log = slog.New(discardLogger{})
		}
		c.base.log = log
		return nil
	}
}

// WithVacuum sets the logger for the cache
func WithVacuum(vacuum VacuumFunc, interval time.Duration) Op {
	return func(c *MQ) error {
		c.base.vacuum = vacuum
		c.base.vacuumInterval = interval
		return nil
	}
}

func dbDefault() Op {
	return func(c *MQ) error {
		return errors.Join(
			dbPragma("journal_mode = WAL")(c),
			dbPragma("synchronous = normal")(c),
			dbPragma(`auto_vacuum = incremental`)(c),
			dbPragma(`incremental_vacuum`)(c),
		)
	}
}

type VacuumFunc func(*MQ)
type base_ struct {
	//shards
	uri string

	db            *sql.DB
	closeOnce     sync.Once
	closed        chan struct{}
	removeOnClose bool
	log           *slog.Logger
	streamMu      sync.RWMutex
	streams       map[string]*MQ

	vacuumInterval time.Duration
	vacuum         VacuumFunc
}

type stream_ struct {
	name string // aka namespace, shard or whatever

	written uint64
	read    uint64

	writeMu sync.Mutex

	inform chan struct{}

	subs *globber[*Subscription]

	groupMu sync.Mutex
	groups  map[string]*group
}

type MQ struct {
	base   *base_
	stream stream_
}

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

func (c *MQ) tbl() string {
	return fmt.Sprintf("_mq_delta_stream_%s", c.CurrentStream())
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

type Msg struct {
	mq        *MQ
	MessageId uint64
	Topic     string
	Payload   []byte
	At        time.Time
}

func (m *Msg) Reply(payload []byte) (Msg, error) {
	if m.mq == nil {
		return Msg{}, fmt.Errorf("message does not support reply")
	}
	reply := Msg{
		Topic:   fmt.Sprintf("_inbox.%d", m.MessageId),
		Payload: payload,
	}
	return m.mq.write(reply)
}

// Ack acknowledges the message by advancing the read-ack watermark to at
// least this message's MessageId. This allows VacuumOnReadAck to clean up
// messages up to and including this one. Returns an error if the message was
// not received through a subscriber (i.e. mq is nil).
func (m *Msg) Ack() error {
	if m.mq == nil {
		return fmt.Errorf("message does not support ack")
	}
	return ackRead(m.mq.base.db, m.MessageId, m.mq.tbl)
}

func (mq *MQ) write(m Msg) (Msg, error) {
	// wow, the write lock and espessaly the select statment
	// in this section  seems to slow down the write operation from 33 000msg/s to 13 000 msg/s

	// since write operations are not concurrent a lock here is probably fine
	//   and helps us out lot that things are written in order.
	mq.stream.writeMu.Lock()
	defer mq.stream.writeMu.Unlock()

	m.MessageId = atomic.LoadUint64(&mq.stream.written) + 1 // probably not needed due to lock
	defer atomic.AddUint64(&mq.stream.written, 1)

	m.At = time.Now()
	err := persist(mq.base.db, m, mq.tbl)
	if err != nil {
		return m, fmt.Errorf("could not persist, %w", err)
	}

	// this is very slow, reduces throughput by a factor, from 10 000 msg/s to 1 000 msg/s
	// good to reduce latency but not throughput.
	// should probably allow for some option. this becomes useless if we are processing a lot of writes.
	select {
	case mq.stream.inform <- struct{}{}:
	default:
	}

	if m.MessageId%1000 == 1000-1 {
		err = ackWritten(mq.base.db, m.MessageId, mq.tbl)
		if err != nil {
			return m, fmt.Errorf("could not ack written, %w", err)
		}
	}

	return m, nil

}

func (mq *MQ) vacuumloop() {
	go func() {
		if mq.base.vacuum == nil {
			mq.base.log.Warn("[delta] no vacuum strategy configured; messages will accumulate in the database without bound")
			return
		}
		if mq.base.vacuumInterval < 100*time.Millisecond {
			mq.base.vacuumInterval = 100 * time.Millisecond
		}
		sleep := mq.base.vacuumInterval
		vacuum := mq.base.vacuum

		mq.base.log.Info("[delta] starting vacuum loop", "interval", sleep)
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in vacuum loop, recovering", "panic", r)
					}
				}()
				select {
				case <-mq.base.closed:
					return
				case <-time.After(sleep):
					vacuum(mq)
					mq.base.streamMu.RLock()
					streams := make([]*MQ, 0, len(mq.base.streams))
					for _, s := range mq.base.streams {
						streams = append(streams, s)
					}
					mq.base.streamMu.RUnlock()
					for _, s := range streams {
						vacuum(s)
					}
				}
			}()

			// Exit the outer loop only after the MQ is closed. The inner func()
			// returns both on normal ticks and on panic recovery, so we must check.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}()
}
func (mq *MQ) readloop() {
	mq.base.log.Debug("[delta] starting loop", "stream_", mq.CurrentStream())
	var size = max(uint64(runtime.NumCPU()), 4)

	var jobs = make(chan uint64)

	producer := func() {
		var dirty bool
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in read-loop producer, recovering", "panic", r)
					}
				}()

				if dirty {
					dirty = false
					// TODO ack read is great and all, but for a vacuum, we might end up in a place where we delete a message
					// prior to it being read by the consumer.
					err := ackRead(mq.base.db, atomic.LoadUint64(&mq.stream.read), mq.tbl)
					if err != nil {
						mq.base.log.Error("[delta] could not ack read", "err", err, "stream_", mq.CurrentStream())
					}
				}

				select {
				case <-mq.base.closed:
					mq.base.log.Debug("[delta] reader, stopping loop", "stream_", mq.CurrentStream())
					return
				case <-mq.stream.inform: // probably faster to remove this and have a tighter sleep
				case <-time.After(100 * time.Millisecond):
				}

				for {
					written := atomic.LoadUint64(&mq.stream.written)
					read := atomic.LoadUint64(&mq.stream.read)

					// if we have read all messages, need to wait for new messages
					if read > written {
						break
					}

					written = min(written, read+size)
					for ; read <= written; read++ {
						jobs <- read
					}
					atomic.StoreUint64(&mq.stream.read, read)
					dirty = true
				}
			}()

			// Exit the outer loop once the MQ has been closed.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}

	consumer := func(i int) {
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						mq.base.log.Error("[delta] panic in read-loop consumer, recovering", "panic", r)
					}
				}()

				if h := getTestPanicHook(); h != nil {
					h()
				}

				var messageId uint64
				select {
				case <-mq.base.closed:
					return
				case messageId = <-jobs:
				}
				m, err := message(mq.base.db, messageId, mq.tbl)
				if err != nil {
					mq.base.log.Error("[delta] could not get message", "id", messageId, "err", err, "stream_", mq.CurrentStream())
					return
				}

				subs := mq.stream.subs.Match(m.Topic)
				for _, s := range subs {
					s := s
					m := m
					m.mq = mq
					go func() { // TODO deap copy message? should it not be a go routine? then a pool of go routines?
						defer func() {
							if r := recover(); r != nil {
								mq.base.log.Error("[delta] panic in subscriber notify goroutine, recovering", "panic", r)
							}
						}()
						s.notify(m)
					}()
				}
			}()

			// Exit the outer loop once the MQ has been closed.
			select {
			case <-mq.base.closed:
				return
			default:
			}
		}
	}

	go producer()

	for i := 0; i < int(size); i++ {
		i := i
		go consumer(i)
	}

}

type Publication struct {
	Msg
	Err  error
	done chan struct{}
}

func (p *Publication) Done() <-chan struct{} {
	return p.done
}

func (mq *MQ) Publish(topic string, payload []byte) (*Publication, error) {
	pub := &Publication{
		done: make(chan struct{}),
	}
	close(pub.done)

	topic, err := checkTopic(topic)
	if err != nil {
		pub.Err = fmt.Errorf("not a valid topic, %w", err)
		return nil, pub.Err
	}

	m, err := mq.write(Msg{
		Topic:   topic,
		Payload: payload,
	})

	pub.Msg = m
	pub.Err = err
	return pub, err
}

func (mq *MQ) PublishAsync(topic string, payload []byte) *Publication {
	payloadCopy := append([]byte{}, payload...)
	pub := &Publication{
		done: make(chan struct{}),
	}
	go func() {
		p, err := mq.Publish(topic, payloadCopy)
		pub.Msg = p.Msg
		pub.Err = err
		close(pub.done)
	}()
	return pub
}

type Subscription struct {
	id          string
	topic       string
	Unsubscribe func()

	closeOnce  sync.Once
	closed     bool
	notifyChan chan Msg
	doneChan   chan struct{} // closed when the subscription is closed; lets notify() abort without holding the write lock
	notifyMu   sync.RWMutex
	mu         sync.Mutex
}

func (s *Subscription) close() {
	// Signal doneChan first, without holding the write lock, so that any
	// notify() call currently blocked in its select can wake up and release
	// the read lock. Only then acquire the write lock to mark closed and
	// close the data channel.
	s.closeOnce.Do(func() {
		close(s.doneChan)
		s.notifyMu.Lock()
		defer s.notifyMu.Unlock()
		s.closed = true
		close(s.notifyChan)
	})
}

func (s *Subscription) Topic() string {
	return s.topic
}
func (s *Subscription) Id() string {
	return s.id
}

func (s *Subscription) notify(m Msg) {
	s.notifyMu.RLock()
	defer s.notifyMu.RUnlock()
	if s.closed {
		return
	}
	// Use a select so that a concurrent close() can signal doneChan and unblock
	// this send without needing to acquire the write lock while we hold the read
	// lock, which would otherwise cause a deadlock.
	select {
	case s.notifyChan <- m:
	case <-s.doneChan:
	}
}

func (s *Subscription) tryNotify(m Msg) (written bool) {
	s.notifyMu.RLock()
	defer s.notifyMu.RUnlock()
	if s.closed {
		return false
	}
	select {
	case s.notifyChan <- m:
		return true
	default:
		return false
	}
}

func (s *Subscription) Chan() <-chan Msg {
	return s.notifyChan
}

func (s *Subscription) Next() (Msg, bool) {
	m, ok := <-s.notifyChan
	return m, ok
}

func (mq *MQ) Subscribe(topic string) (*Subscription, error) {
	uid := uid()

	topic, err := checkTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid,
		topic:      topic,
		notifyChan: make(chan Msg),
		doneChan:   make(chan struct{}),
	}
	mq.stream.subs.Insert(s)
	var unsubOnce sync.Once
	s.Unsubscribe = func() {
		unsubOnce.Do(func() {
			mq.stream.subs.Remove(s)
			close(s.doneChan)
			close(s.notifyChan)
		})
	}

	return s, nil
}

type group struct {
	mu   sync.Mutex
	main *Subscription
	subs []*Subscription
}

func (mq *MQ) Queue(topic string, key string) (*Subscription, error) {

	topic, err := checkTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}
	key = strings.ToLower(strings.TrimSpace(key))
	if len(key) == 0 {
		return nil, fmt.Errorf("key is empty")
	}
	key = fmt.Sprintf("%s[%s]", topic, key)

	mq.stream.groupMu.Lock()
	g := mq.stream.groups[key]
	if g == nil {
		mq.stream.groups[key] = &group{}
		g = mq.stream.groups[key]
	}
	mq.stream.groupMu.Unlock()

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.main == nil {
		g.main, err = mq.Subscribe(topic)
		if err != nil {
			return nil, fmt.Errorf("could not create main subscriber, %w", err)
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					mq.base.log.Error("[delta] panic in queue distributor, recovering", "panic", r)
				}
			}()
		outer:
			for m := range g.main.Chan() {
				g.mu.Lock()
				ss := append([]*Subscription{}, g.subs...)
				g.mu.Unlock()

				rand.Shuffle(len(ss), func(i, j int) {
					ss[i], ss[j] = ss[j], ss[i]
				})

				if len(ss) == 0 {
					continue
				}

				for _, s := range ss {
					if s.tryNotify(m) {
						continue outer
					}
				}
				go func(s *Subscription, m Msg) {
					defer func() {
						if r := recover(); r != nil {
							mq.base.log.Error("[delta] panic in queue notify goroutine, recovering", "panic", r)
						}
					}()
					s.notify(m)
				}(ss[0], m)

			}
		}()
	}

	sub := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
		doneChan:   make(chan struct{}),
	}

	sub.Unsubscribe = func() { // TODO can this produce a deadlock? between the main and the sub?
		g.mu.Lock()
		defer g.mu.Unlock()
		close(sub.doneChan)
		close(sub.notifyChan)
		for i, s := range g.subs {
			if s.id == sub.id {
				g.subs = append(g.subs[:i], g.subs[i+1:]...)
				break
			}
		}

		if len(g.subs) == 0 {
			mq.stream.groupMu.Lock()
			g.main.Unsubscribe()
			delete(mq.stream.groups, key)
			mq.stream.groupMu.Unlock()
		}
	}

	g.subs = append(g.subs, sub)
	return sub, nil
}

func (mq *MQ) Request(ctx context.Context, topic string, payload []byte) (*Subscription, error) {

	m, err := mq.Publish(topic, payload)
	if err != nil {
		return nil, fmt.Errorf("could not pub, %w", err)
	}

	sub, err := mq.Subscribe(fmt.Sprintf("_inbox.%d", m.MessageId))
	if err != nil {
		return nil, fmt.Errorf("could not sub, %w", err)
	}

	s := &Subscription{
		id:         sub.id,
		topic:      sub.topic,
		notifyChan: make(chan Msg),
		doneChan:   make(chan struct{}),
	}

	var unsubOnce sync.Once
	s.Unsubscribe = func() {
		unsubOnce.Do(func() {
			close(s.doneChan)
			close(s.notifyChan)
		})
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				mq.base.log.Error("[delta] panic in request goroutine, recovering", "panic", r)
			}
		}()
		defer sub.Unsubscribe()
		defer s.Unsubscribe()

		select {
		case <-ctx.Done():
		case m, ok := <-sub.Chan():
			if ok {
				s.notify(m)
			}
		}
	}()

	return s, nil

}

func (mq *MQ) SubscribeFrom(topic string, from time.Time) (*Subscription, error) {
	topic, err := checkTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
		doneChan:   make(chan struct{}),
	}

	buffer := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
		doneChan:   make(chan struct{}),
	}

	s.Unsubscribe = func() {
		mq.stream.subs.Remove(buffer)
		buffer.close()
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				mq.base.log.Error("[delta] panic in subscribe-from historical goroutine, recovering", "panic", r)
			}
		}()

		mq.stream.subs.Insert(buffer)
		splitt := atomic.LoadUint64(&mq.stream.written)

		start := sync.WaitGroup{}
		start.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					mq.base.log.Error("[delta] panic in subscribe-from live forwarding goroutine, recovering", "panic", r)
				}
			}()
			start.Wait()
			for m := range buffer.Chan() {
				if m.MessageId <= splitt {
					continue
				}
				s.notify(m)
			}
			close(s.notifyChan)
		}()

		var last time.Time
		itr := iterMessage(mq.base.db, s.topic, from, splitt, mq.tbl, mq.base.log)
		for m := range itr {
			s.notify(m)
			last = m.At
		}
		_ = last // last seen timestamp, available for future retry/resume logic
		start.Done()
	}()

	return s, nil
}

func (c *MQ) CurrentStream() string {
	return c.stream.name
}
