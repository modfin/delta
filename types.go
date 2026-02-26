package delta

import (
	"fmt"
	"sync"
	"time"
)

// Msg is a message persisted in a stream.
//
// Msg values are delivered to subscribers and can be replied to when they
// originate from a live subscription.
type Msg struct {
	mq        *MQ
	MessageId uint64
	Topic     string
	Payload   []byte
	At        time.Time
}

// Reply publishes a reply message to this message's inbox topic.
//
// It returns an error when the message was not delivered from a live MQ.
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

// Publication is the result of a publish operation.
//
// For PublishAsync, wait on Done and then inspect Err.
type Publication struct {
	Msg
	Err  error
	done chan struct{}
}

// Done returns a channel that is closed when the publish operation finishes.
func (p *Publication) Done() <-chan struct{} {
	return p.done
}

// Subscription represents an active topic subscription.
//
// Messages are delivered on Chan until Unsubscribe is called.
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

// Topic returns the subscription's normalized topic pattern.
func (s *Subscription) Topic() string {
	return s.topic
}

// Id returns the unique identifier for this subscription.
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

// Chan returns the channel used to deliver subscription messages.
func (s *Subscription) Chan() <-chan Msg {
	return s.notifyChan
}

// Next blocks until the next message arrives or the subscription closes.
//
// The boolean return value is false when the subscription is closed.
func (s *Subscription) Next() (Msg, bool) {
	m, ok := <-s.notifyChan
	return m, ok
}
