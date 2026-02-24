package delta

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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
				// Use a three-way select so that a concurrent context cancellation
				// or an explicit Unsubscribe() (which closes s.doneChan) can unblock
				// this send even when nobody is reading from s.Chan(). Without this
				// the goroutine would block forever in s.notify() because s.doneChan
				// is only closed by s.Unsubscribe(), which is deferred in this same
				// goroutine and cannot run while the send is blocked.
				s.notifyMu.RLock()
				if !s.closed {
					select {
					case s.notifyChan <- m:
					case <-s.doneChan:
					case <-ctx.Done():
					}
				}
				s.notifyMu.RUnlock()
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
