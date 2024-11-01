package delta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

type base_ struct {
	//shards
	uri string

	db            *sql.DB
	closeOnce     sync.Once
	closed        chan struct{}
	removeOnClose bool
	log           *slog.Logger
	streamMu      sync.Mutex
	streams       map[string]*MQ
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

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping db, %w", err)
	}

	c := &MQ{
		base: &base_{
			uri: uri,
			db:  db,
			log: slog.New(discardLogger{}),

			closeOnce:     sync.Once{},
			closed:        make(chan struct{}),
			removeOnClose: false,
			streamMu:      sync.Mutex{},
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
		err := op(c)
		if err != nil {
			return nil, fmt.Errorf("could not exec op, %w", err)
		}
	}

	c.base.log.Debug("[delta] creating schema")
	err = schema(c)
	if err != nil {
		return nil, fmt.Errorf("failed in creating schema, %w", err)
	}

	c.base.log.Debug("[delta] optimizing database")
	err = optimize(c)
	if err != nil {
		return nil, fmt.Errorf("failed in optimizing, %w", err)
	}

	written, read, err := metrics(c.base.db, c.tbl)
	if err != nil {
		return nil, fmt.Errorf("could not get written, %w", err)
	}

	c.base.log.Info("[delta] starting stream_ at", "written", written, "read", read, "stream_", c.stream)

	atomic.SwapUint64(&c.stream.written, written)
	atomic.SwapUint64(&c.stream.read, read) // Probably load some sort of read counter instead?

	c.readloop()

	return c, nil
}

func (c *MQ) tbl() string {
	return fmt.Sprintf("_mq_delta_stream_%s", c.CurrentStream())
}

func (c *MQ) Stream(stream string, ops ...Op) (*MQ, error) {
	c.base.streamMu.Lock()
	defer c.base.streamMu.Unlock()

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
	cc.base.streams[stream] = cc

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
	c.base.log.Info("[delta] starting stream_ at", "written", written, "read", read, "stream_", cc.stream)

	atomic.SwapUint64(&cc.stream.written, written)
	atomic.SwapUint64(&cc.stream.read, read)
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

	for _, cc := range c.base.streams {
		err := ackWritten(cc.base.db, atomic.LoadUint64(&cc.stream.written), cc.tbl)
		if err != nil {
			return fmt.Errorf("could not ack written, %w", err)
		}
		w, r, err := metrics(cc.base.db, cc.tbl)
		if err != nil {
			return fmt.Errorf("could not get metrics, %w", err)
		}
		c.base.log.Info("[delta] closing stream_", "stream_", cc.CurrentStream(), "written", w, "read", r)
	}

	return c.base.db.Close()
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

	err := errors.Join(
		os.Remove(file),
		os.Remove(fmt.Sprintf("%s-shm", file)),
		os.Remove(fmt.Sprintf("%s-wal", file)),
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

func (m *Msg) Ack() error {
	return nil
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

func (mq *MQ) readloop() {
	mq.base.log.Debug("[delta] starting loop", "stream_", mq.CurrentStream())
	var size = max(uint64(runtime.NumCPU()), 4)

	var jobs = make(chan uint64)

	producer := func() {
		var dirty bool
		for {

			if dirty {
				dirty = false
				err := ackRead(mq.base.db, atomic.LoadUint64(&mq.stream.read), mq.tbl)
				if err != nil {
					mq.base.log.Error("[delta] could not ack read", "err", err, "stream_", mq.CurrentStream())
				}
			}

			select {
			case <-mq.base.closed:
				mq.base.log.Debug("[delta] reader, stopping loop", "stream_", mq.CurrentStream())
				return
			case <-mq.stream.inform: // probably faster to remove this and have a righter sleep
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
		}
	}

	consumer := func(i int) {
		for {

			var messageId uint64
			select {
			case <-mq.base.closed:
				return
			case messageId = <-jobs:
			}
			m, err := message(mq.base.db, messageId, mq.tbl)
			if err != nil {
				mq.base.log.Error("[delta] could not get message", "id", messageId, "err", err, "stream_", mq.CurrentStream())
				continue
			}

			subs := mq.stream.subs.Match(m.Topic)
			for _, s := range subs {
				s := s
				m := m
				m.mq = mq
				go s.notify(m) // TODO deap copy message? should it not be a go routine? then a pool of go routines?
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

	topic, err := checkTopicPub(topic)
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
	notifyMu   sync.RWMutex
	mu         sync.Mutex
}

func (s *Subscription) close() {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	s.closeOnce.Do(func() {
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
	// TODO this could deadloack if no one is reading from the channel and we try to close it
	s.notifyChan <- m
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

	topic, err := checkTopicSub(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid,
		topic:      topic,
		notifyChan: make(chan Msg),
	}
	mq.stream.subs.Insert(s)
	s.Unsubscribe = func() {
		mq.stream.subs.Remove(s)
		close(s.notifyChan)
	}

	return s, nil
}

type group struct {
	mu   sync.Mutex
	main *Subscription
	subs []*Subscription
}

func (mq *MQ) Queue(topic string, key string) (*Subscription, error) {

	topic, err := checkTopicSub(topic)
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
					s.notify(m)
				}(ss[0], m)

			}
		}()
	}

	sub := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}

	sub.Unsubscribe = func() { // TODO can this produce a deadlock? between the main and the sub?
		g.mu.Lock()
		defer g.mu.Unlock()
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
	}

	s.Unsubscribe = func() {
		close(s.notifyChan)
	}

	go func() {
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
	topic, err := checkTopicSub(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}

	buffer := &Subscription{
		id:         uid(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}

	s.Unsubscribe = func() {
		mq.stream.subs.Remove(buffer)
		buffer.close()
	}

	go func() {

		mq.stream.subs.Insert(buffer)
		splitt := atomic.LoadUint64(&mq.stream.written)

		start := sync.WaitGroup{}
		start.Add(1)
		go func() {
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
			m.At = last
		}
		start.Done()
	}()

	return s, nil
}

func (c *MQ) CurrentStream() string {
	return c.stream.name
}
