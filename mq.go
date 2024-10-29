package delta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
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

	writeMu sync.Mutex
	inform  chan uint64

	subs *globber[*Sub]

	groupMu sync.Mutex
	groups  map[string]*group
}

func New(uri string, op ...Op) (*MQ, error) {
	db, err := sql.Open("sqlite3", uri)
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

		written: 0,
		read:    0,
		inform:  make(chan uint64),

		subs: newGlobber[*Sub](),

		groups: map[string]*group{},
	}
	*c.removeOnClose = false

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
	return "_mq_delta"
}

func optimize(c *MQ) error {
	return exec(c.db, `pragma vacuum;`, `pragma optimize;`)
}

func schema(c *MQ) error {
	return exec(c.db, fmt.Sprintf(base_schema, c.tbl()))
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

type Pub struct {
	Msg
	Err  error
	done chan struct{}
}

func (p *Pub) Done() <-chan struct{} {
	return p.done
}

func checkTopicPub(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}
	for _, r := range topic {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.':
			continue
		}
		return "", fmt.Errorf("topic contains invalid character, only a-z, -, _, . are allowed, got %c", r)
	}
	return topic, nil
}

func checkTopicSub(topic string) (string, error) {
	topic = strings.TrimSpace(topic)
	topic = strings.ToLower(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("topic is empty")
	}
	for _, r := range topic {
		if 'a' <= r && r <= 'z' {
			continue
		}
		if '0' <= r && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.', '*':
			continue
		}
		return "", fmt.Errorf("topic contains invalid character, only a-z, -, _, ., * are allowed, got %c", r)
	}
	return topic, nil
}

func (mq *MQ) Pub(topic string, payload []byte) (*Pub, error) {
	pub := &Pub{
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

func (mq *MQ) PubAsync(topic string, payload []byte) *Pub {
	payloadCopy := append([]byte{}, payload...)
	pub := &Pub{
		done: make(chan struct{}),
	}
	go func() {
		p, err := mq.Pub(topic, payloadCopy)
		pub.Msg = p.Msg
		pub.Err = err
		close(pub.done)
	}()
	return pub
}

type Mode int

const (
	PubSub = iota
	Queue
	ReqRep
)

type Sub struct {
	id    string
	topic string
	Mode  Mode
	Unsub func()

	notifyChan chan Msg
	mu         sync.Mutex
}

func (s *Sub) Topic() string {
	return s.topic
}
func (s *Sub) Id() string {
	return s.id
}

func (s *Sub) notify(m Msg) {
	s.notifyChan <- m
}

func (s *Sub) tryNotify(m Msg) bool {
	select {
	case s.notifyChan <- m:
		return true
	default:
		return false
	}
}

func (s *Sub) Chan() <-chan Msg {
	return s.notifyChan
}

func (s *Sub) Next() (Msg, bool) {
	m, ok := <-s.notifyChan
	return m, ok
}

func (mq *MQ) Sub(topic string) (*Sub, error) {
	uid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("could not generate uuid, %w", err)
	}

	topic, err = checkTopicSub(topic)
	if err != nil {
		return nil, fmt.Errorf("not a valid topic, %w", err)
	}

	s := &Sub{
		id:         uid.String(),
		topic:      topic,
		Mode:       PubSub,
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
	main *Sub
	subs []*Sub
}

func (mq *MQ) Queue(topic string, key string) (*Sub, error) {

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
		g.main, err = mq.Sub(topic)
		if err != nil {
			return nil, fmt.Errorf("could not create main subscriber, %w", err)
		}

		go func() {
		outer:
			for m := range g.main.Chan() {
				g.mu.Lock()
				ss := append([]*Sub{}, g.subs...)
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
				go func(s *Sub, m Msg) {
					s.notify(m)
				}(ss[0], m)

			}
		}()
	}

	uid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("could not generate uuid, %w", err)
	}
	sub := &Sub{
		id:         uid.String(),
		topic:      topic,
		Mode:       Queue,
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

func (mq *MQ) Request(ctx context.Context, topic string, payload []byte) (*Sub, error) {

	m, err := mq.Pub(topic, payload)
	if err != nil {
		return nil, fmt.Errorf("could not pub, %w", err)
	}

	sub, err := mq.Sub(fmt.Sprintf("_inbox.%d", m.MessageId))
	if err != nil {
		return nil, fmt.Errorf("could not sub, %w", err)
	}

	s := &Sub{
		id:         sub.id,
		topic:      sub.topic,
		Mode:       ReqRep,
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
