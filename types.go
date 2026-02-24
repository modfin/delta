package delta

import (
	"fmt"
	"sync"
	"time"
)

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

type Publication struct {
	Msg
	Err  error
	done chan struct{}
}

func (p *Publication) Done() <-chan struct{} {
	return p.done
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
