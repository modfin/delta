package delta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
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
		return exec(c.db,
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
		*mq.removeOnClose = true
		return nil
	}
}

// WithLogger sets the logger for the cache
func WithLogger(log *slog.Logger) Op {
	return func(c *MQ) error {
		if log == nil {
			log = slog.New(discardLogger{})
		}
		c.log = log
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

type MQ struct {
	//shards
	uri string

	db            *sql.DB
	closeOnce     *sync.Once
	closed        chan struct{}
	removeOnClose *bool
	log           *slog.Logger

	written uint64
	read    uint64

	streamMu sync.Mutex
	stream   string // aka namespace, shard or whatever
	streams  map[string]*MQ

	writeMu sync.Mutex
	inform  chan uint64

	subs *globber[*Subscription]

	groupMu sync.Mutex
	groups  map[string]*group
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
		uri: uri,
		db:  db,
		log: slog.New(discardLogger{}),

		closeOnce:     &sync.Once{},
		closed:        make(chan struct{}),
		removeOnClose: new(bool),

		streamMu: sync.Mutex{},
		stream:   DEFAULT_STREAM,
		streams:  make(map[string]*MQ),

		written: 0,
		read:    0,
		inform:  make(chan uint64),

		subs: newGlobber[*Subscription](),

		groups: map[string]*group{},
	}
	*c.removeOnClose = false
	c.streams[c.stream] = c

	ops := append([]Op{dbDefault()}, op...)

	c.log.Debug("[delta] applying options")
	for _, op := range ops {
		err := op(c)
		if err != nil {
			return nil, fmt.Errorf("could not exec op, %w", err)
		}
	}

	c.log.Debug("[delta] creating schema")
	err = schema(c)
	if err != nil {
		return nil, fmt.Errorf("failed in creating schema, %w", err)
	}

	c.log.Debug("[delta] optimizing database")
	err = optimize(c)
	if err != nil {
		return nil, fmt.Errorf("failed in optimizing, %w", err)
	}

	maxSerial, err := serial(c.db, c.tbl)
	if err != nil {
		return nil, fmt.Errorf("could not get written, %w", err)
	}

	atomic.SwapUint64(&c.written, maxSerial)
	atomic.SwapUint64(&c.read, maxSerial+1) // Probably load some sort of read counter instead?

	c.readloop()

	return c, nil
}

func (c *MQ) tbl() string {
	return fmt.Sprintf("_mq_delta_stream_%s", c.stream)
}

func (c *MQ) Stream(stream string, ops ...Op) (*MQ, error) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	stream, err := checkStreamName(stream)
	if err != nil {
		return nil, fmt.Errorf("could not check stream name, %w", err)
	}

	if _, found := c.streams[stream]; found {
		return c.streams[stream], nil
	}

	cc := &MQ{
		uri: c.uri,
		db:  c.db,
		log: slog.New(discardLogger{}),

		closeOnce:     c.closeOnce,
		closed:        c.closed,
		removeOnClose: c.removeOnClose,

		stream: stream,

		written: 0,
		read:    0,
		inform:  make(chan uint64),

		subs: newGlobber[*Subscription](),

		groups: map[string]*group{},
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

	return cc, nil
}

func optimize(c *MQ) error {
	return exec(c.db, `pragma vacuum;`, `pragma optimize;`)
}

func schema(c *MQ) error {
	return errors.Join(
		exec(c.db, fmt.Sprintf(base_schema, c.tbl())),
		exec(c.db, fmt.Sprintf(base_schema_idx, c.tbl(), c.tbl())),
		exec(c.db, base_metadata_schema),
	)
}

// Close closes the cache and all its namespaces
func (c *MQ) Close() error {
	defer func() {
		if *c.removeOnClose {
			_ = c.removeStore()
		}
	}()

	defer func() {
		c.closeOnce.Do(func() {
			close(c.closed)
		})
	}()

	return c.db.Close()
}

func (c *MQ) removeStore() error {

	select {
	case <-c.closed:
	default:
		return fmt.Errorf("db is not closed")
	}

	schema, uri, found := strings.Cut(c.uri, ":")
	if !found {
		return fmt.Errorf("could not find file in uri")
	}
	if schema != "file" {
		return fmt.Errorf("not a file uri")
	}

	file, query, _ := strings.Cut(uri, "?")

	c.log.Info("[delta] remove store", "db", file, "shm", fmt.Sprintf("%s-shm", file), "wal", fmt.Sprintf("%s-wal", file))

	err := errors.Join(
		os.Remove(file),
		os.Remove(fmt.Sprintf("%s-shm", file)),
		os.Remove(fmt.Sprintf("%s-wal", file)),
	)
	if strings.Contains(query, "tmp=true") {
		c.log.Info("[delta] remove store dir", "dir", filepath.Dir(file))
		err = errors.Join(os.Remove(filepath.Dir(file)), err)
	}

	return err
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
	//   and helps us out lot when it comes to ordering
	mq.writeMu.Lock()
	defer mq.writeMu.Unlock()

	m.MessageId = atomic.LoadUint64(&mq.written) + 1 // probably not needed due to lock
	defer atomic.AddUint64(&mq.written, 1)

	m.At = time.Now()
	err := persist(mq.db, m, mq.tbl)
	if err != nil {
		return m, fmt.Errorf("could not persist, %w", err)
	}
	select { // inform of new write
	case mq.inform <- m.MessageId:
	default:
	}
	return m, nil

}

func (mq *MQ) readloop() {
	mq.log.Debug("[delta] starting loop stopping")
	var size = uint64(runtime.NumCPU() * 2)

	var jobs = make(chan uint64)

	producer := func() {
		for {

			select {
			case <-mq.closed:
				mq.log.Debug("[delta] reader, stopping loop")
				return
			case <-mq.inform:
			case <-time.After(100 * time.Millisecond):
			}

			for {
				written := atomic.LoadUint64(&mq.written)
				read := atomic.LoadUint64(&mq.read)

				// if we have read all messages, need to wait for new messages
				if read > written {
					break
				}

				written = min(written, read+size)

				for ; read <= written; read++ {
					jobs <- read
				}
				atomic.StoreUint64(&mq.read, read)
			}
		}
	}

	consumer := func(i int) {
		for {

			var messageId uint64
			select {
			case <-mq.closed:
				mq.log.Debug("[delta] reading consumer stopping loop", "id", i)
				return
			case messageId = <-jobs:
			}
			m, err := message(mq.db, messageId, mq.tbl)
			if err != nil {
				mq.log.Error("[delta] could not get message", "id", messageId, "err", err)
				continue
			}

			subs := mq.subs.Match(m.Topic)
			for _, s := range subs {
				s := s
				m := m
				m.mq = mq
				go s.notify(m) // TODO deap copy message? should it not be a go routine?
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
	id    string
	topic string
	Unsub func()

	notifyChan chan Msg
	mu         sync.Mutex
}

func (s *Subscription) Topic() string {
	return s.topic
}
func (s *Subscription) Id() string {
	return s.id
}

func (s *Subscription) notify(m Msg) {
	s.notifyChan <- m
}

func (s *Subscription) tryNotify(m Msg) bool {
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
	uid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("could not generate uuid, %w", err)
	}

	topic, err = checkTopicSub(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid.String(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}
	mq.subs.Insert(s)
	s.Unsub = func() {
		mq.subs.Remove(s)
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

	mq.groupMu.Lock()
	g := mq.groups[key]
	if g == nil {
		mq.groups[key] = &group{}
		g = mq.groups[key]
	}
	mq.groupMu.Unlock()

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

	uid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("could not generate uuid, %w", err)
	}
	sub := &Subscription{
		id:         uid.String(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}

	sub.Unsub = func() { // TODO can this produce a deadlock? between the main and the sub?
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
			mq.groupMu.Lock()
			g.main.Unsub()
			delete(mq.groups, key)
			mq.groupMu.Unlock()
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

	s.Unsub = func() {
		close(s.notifyChan)
	}

	go func() {
		defer sub.Unsub()
		defer s.Unsub()

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

func (mq *MQ) SubFrom(topic string, from time.Time) (*Subscription, error) {
	uid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("could not generate uuid, %w", err)
	}

	topic, err = checkTopicSub(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Subscription{
		id:         uid.String(),
		topic:      topic,
		notifyChan: make(chan Msg),
	}

	s.Unsub = func() {
		mq.subs.Remove(s)
		close(s.notifyChan)
	}

	go func() {

		var last time.Time
		itr := iterMessage(mq.db, s.topic, from, mq.tbl, mq.log)
		for m := range itr {
			s.notify(m)
			m.At = last
		}
		mq.subs.Insert(s) // TODO probalby insert a buffer here

	}()

	return s, nil
}

func (c *MQ) CurrentStream() string {
	return c.stream
}
