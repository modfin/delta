package delta

import (
	"fmt"
	"sync/atomic"
	"time"
)

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
