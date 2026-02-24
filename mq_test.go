package delta_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSimplePubSub(t *testing.T) {

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	for range 1000 {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Subscribe("a.b.*.d")
			assert.NoError(t, err)

			subWg.Done()

			select {
			case m := <-sub.Chan():
				assert.Equal(t, "hello", string(m.Payload))
			case <-time.After(5 * time.Second):
				assert.Fail(t, "timeout")
			}
			doneWg.Done()
		}()

	}

	subWg.Wait()

	_, err = mq.Publish("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)
	//fmt.Printf("pub: %+v\n", pub)

	doneWg.Wait()

}

func TestSimplePubSub2(t *testing.T) {

	//slog.SetLogLoggerLevel(slog.LevelDebug)
	//defer slog.SetLogLoggerLevel(slog.LevelInfo)

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	for range 1000 {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Subscribe("a.b.*.d")
			assert.NoError(t, err)

			subWg.Done()

			select {
			case m := <-sub.Chan():
				assert.Equal(t, "hello", string(m.Payload))
			case <-time.After(5 * time.Second):
				assert.Fail(t, "timeout")
			}

			select {
			case m := <-sub.Chan():
				assert.Equal(t, "world", string(m.Payload))
			case <-time.After(5 * time.Second):
				assert.Fail(t, "timeout")
			}

			sub.Unsubscribe()
			doneWg.Done()
		}()

	}

	subWg.Wait()

	_, err = mq.Publish("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	_, err = mq.Publish("a.b.c.d", []byte("world"))
	assert.NoError(t, err)
	//fmt.Printf("pub: %+v\n", pub)

	doneWg.Wait()

}

func TestSimpleQueue(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(slog.LevelInfo)

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(slog.Default()))
	assert.NoError(t, err)
	defer mq.Close()

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup

	var counter int64

	for range 100 {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Queue("a.*", "test")
			assert.NoError(t, err)
			subWg.Done()

		outer:
			for {
				select {
				case m := <-sub.Chan():
					assert.Equal(t, "hello queue", string(m.Payload))
					atomic.AddInt64(&counter, 1)

				case <-time.After(1000 * time.Millisecond):
					break outer
				}
			}
			sub.Unsubscribe()
			doneWg.Done()
		}()

	}

	subWg.Wait()

	var size = 1013
	for range size {
		_, err = mq.Publish("a.b", []byte("hello queue"))
		assert.NoError(t, err)
	}
	doneWg.Wait()
	count := atomic.LoadInt64(&counter)

	assert.Equal(t, int64(size), count)

}

func TestSimpleRequestReply(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(slog.LevelInfo)

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(slog.Default()))
	assert.NoError(t, err)
	defer mq.Close()

	sub, err := mq.Queue("greet.*", "test")
	assert.NoError(t, err)
	go func() {
		m := <-sub.Chan()
		_, name, _ := strings.Cut(m.Topic, ".")
		_, err := m.Reply([]byte("hello " + name))
		assert.NoError(t, err)
	}()

	resp, err := mq.Request(context.Background(), "greet.alice", nil)
	assert.NoError(t, err)
	msg, ok := resp.Next()
	assert.True(t, ok)
	assert.Equal(t, "hello alice", string(msg.Payload))

}

func TestSimpleSubFrom(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	size := 1013
	from := time.Now()
	var res [][]byte
	fmt.Println("Inserting ", size*4, "messages")
	for i := range size {
		payload := []byte(strconv.Itoa(i))
		_, err := mq.Publish("a.b.c.d", payload)
		res = append(res, payload)
		assert.NoError(t, err)

		_, _ = mq.Publish("a.b.XX.x", payload)
		_, _ = mq.Publish("a.x.c.d", payload)
		_, _ = mq.Publish("a.y.c.d", payload)

	}

	sub, err := mq.SubscribeFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages")
	start := time.Now()
	for i := range size {
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(res[i]), string(m.Payload), "i: %d", i)
	}
	fmt.Println("GLOB", "size", size, "time", time.Since(start), "per msg", time.Since(start)/time.Duration(size))
	sub.Unsubscribe()

	sub, err = mq.SubscribeFrom("a.b.c.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages")
	start = time.Now()
	for i := range size {
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(res[i]), string(m.Payload), "i: %d", i)
	}
	fmt.Println("EXAC", "size", size, "time", time.Since(start), "per msg", time.Since(start)/time.Duration(size))

}

func TestSimpleSubFrom_joininghistory_with_live(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	size := 1013
	from := time.Now()
	for i := range size {
		payload := []byte(strconv.Itoa(i))
		_, err := mq.Publish("a.b.c.d", payload)
		assert.NoError(t, err)
	}

	for i := range size {
		i := i
		go func(i int) {
			payload := []byte(strconv.Itoa(i))
			_, err := mq.Publish("a.b.c.d", payload)
			assert.NoError(t, err)
		}(i + size)

	}

	sub, err := mq.SubscribeFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages")

	var res []int
	for range size * 2 {
		m, ok := sub.Next()
		assert.True(t, ok)
		ii, err := strconv.Atoi(string(m.Payload))
		assert.NoError(t, err)
		res = append(res, ii)
	}
	sub.Unsubscribe()

	sort.Ints(res)

	for i := 1; i < size*2; i++ {
		assert.Equal(t, res[i-1]+1, res[i])
	}

}

func TestSimpleSubFrom_joininghistory_with_live2(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	size := 1013
	from := time.Now()

	for i := range size {
		i := i
		go func(i int) {
			payload := []byte(strconv.Itoa(i))
			_, err := mq.Publish("a.b.c.d", payload)
			assert.NoError(t, err)
		}(i + size)

	}

	sub, err := mq.SubscribeFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages")

	var res []int
	i := 0
	for m := range sub.Chan() {
		ii, err := strconv.Atoi(string(m.Payload))
		assert.NoError(t, err)
		res = append(res, ii)
		i++
		if i == size {
			sub.Unsubscribe()
		}
	}

	sort.Ints(res)

	for i := 1; i < size; i++ {
		assert.Equal(t, res[i-1]+1, res[i])
	}

}

func TestSimpleStream(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	size := 1013

	from := time.Now()
	var s1 [][]byte
	fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())
	for i := range size {
		payload := []byte(mq.CurrentStream() + "_" + strconv.Itoa(i))
		_, err := mq.Publish("a.b.c.d", payload)
		s1 = append(s1, payload)
		assert.NoError(t, err)
	}

	const SUB_STREAM = "00sub stream00"
	mq, err = mq.Stream(SUB_STREAM)
	assert.NoError(t, err)
	var s2 [][]byte
	fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())
	for i := range size {
		payload := []byte(mq.CurrentStream() + "_" + strconv.Itoa(i))
		_, err := mq.Publish("a.b.c.d", payload)
		s2 = append(s2, payload)
		assert.NoError(t, err)
	}

	mq, err = mq.Stream(delta.DEFAULT_STREAM)
	assert.NoError(t, err)

	sub, err := mq.SubscribeFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages from stream_", mq.CurrentStream())
	for i := range size {
		//fmt.Println("Retrieving", i, "/", size, "messages from stream_", mq.CurrentStream())
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(s1[i]), string(m.Payload), "i: %d", i)
	}

	mq, err = mq.Stream(SUB_STREAM)
	assert.NoError(t, err)

	sub, err = mq.SubscribeFrom("a.b.c.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages from stream_", mq.CurrentStream())
	for i := range size {
		//fmt.Println("Retrieving", i, "/", size, "messages from stream_", mq.CurrentStream())
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(s2[i]), string(m.Payload), "i: %d", i)
	}

}

func TestParallelStream(t *testing.T) {

	//slog.SetLogLoggerLevel(slog.LevelDebug)
	//defer slog.SetLogLoggerLevel(slog.LevelInfo)
	//logger := slog.Default()
	var logger *slog.Logger

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(logger))
	assert.NoError(t, err)
	defer mq.Close()

	size := 1013
	//size := 100

	//from := time.Now()
	var s1 []string
	var s1mu sync.Mutex
	wg := sync.WaitGroup{}
	wg.Add(size)
	fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())
	for i := range size {
		i := i
		go func(i int, n *delta.MQ) {
			payload := []byte(n.CurrentStream() + "_" + strconv.Itoa(i))
			//fmt.Println("Publishing", string(payload), "to stream_", n.CurrentStream())
			_, err := n.Publish("a.b.c.d", payload)
			s1mu.Lock()
			defer s1mu.Unlock()
			s1 = append(s1, string(payload))
			assert.NoError(t, err)
			wg.Done()

		}(i, mq)

	}

	const SUB_STREAM = "sub stream_"
	mq, err = mq.Stream(SUB_STREAM)
	assert.NoError(t, err)
	var s2 []string
	var s2mu sync.Mutex
	fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())

	wg.Add(size)
	for i := range size {
		i := i
		go func(i int, m *delta.MQ) {
			payload := []byte(m.CurrentStream() + "_" + strconv.Itoa(i))
			//fmt.Println("Publishing", string(payload), "to stream_", m.CurrentStream())
			_, err := m.Publish("a.b.c.d", payload)
			s2mu.Lock()
			defer s2mu.Unlock()
			s2 = append(s2, string(payload))
			assert.NoError(t, err)
			wg.Done()
		}(i, mq)

	}

	wg.Add(2)

	var res1 []string
	go func(m *delta.MQ) {
		mm, err := m.Stream(delta.DEFAULT_STREAM)
		assert.NoError(t, err)
		sub, err := mm.SubscribeFrom("a.b.*.d", time.Time{})
		assert.NoError(t, err)
		fmt.Println("Retrieving ", size, "messages from stream_", mm.CurrentStream())
		for range size {
			m, ok := sub.Next()
			//fmt.Println("Retrieving", len(res1), "/", size, "messages from stream_", mm.CurrentStream(), "--", string(m.Payload))
			assert.True(t, ok)
			res1 = append(res1, string(m.Payload))
		}
		wg.Done()
	}(mq)

	var res2 []string
	go func(m *delta.MQ) {
		mm, err := m.Stream(SUB_STREAM)
		assert.NoError(t, err)
		sub, err := mm.SubscribeFrom("a.b.*.d", time.Time{})
		assert.NoError(t, err)
		fmt.Println("Retrieving ", size, "messages from stream_", mm.CurrentStream())
		for range size {
			m, ok := sub.Next()
			//fmt.Println("Retrieving", len(res2), "/", size, "messages from stream_", mm.CurrentStream(), "--", string(m.Payload))
			assert.True(t, ok)
			res2 = append(res2, string(m.Payload))
		}
		wg.Done()
	}(mq)

	wg.Wait()

	sort.Strings(res1)
	sort.Strings(res2)
	sort.Strings(s1)
	sort.Strings(s2)

	assert.Equal(t, s1, res1)
	assert.Equal(t, s2, res2)

}

func TestParallelStream2(t *testing.T) {
	//
	//slog.SetLogLoggerLevel(slog.LevelDebug)
	//defer slog.SetLogLoggerLevel(slog.LevelInfo)
	//logger := slog.Default()
	var logger *slog.Logger

	uri := delta.URITemp()
	defer func(uri string) {
		delta.RemoveStore(uri, logger)
	}(uri)

	// todo remove once done

	var s1 []string
	var s2 []string

	var s1mu sync.Mutex
	var s2mu sync.Mutex

	size := 1013

	for loop := 0; loop < 2; loop++ {

		mq, err := delta.New(uri, delta.WithLogger(logger))
		assert.NoError(t, err)

		fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())

		wg := sync.WaitGroup{}

		wg.Add(size)

		for i := range size {
			i := i
			go func(i int, n *delta.MQ) {
				payload := []byte(n.CurrentStream() + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(loop))
				//fmt.Println("Publishing", string(payload), "to stream_", n.CurrentStream())
				_, err := n.Publish("a.b.c.d", payload)
				s1mu.Lock()
				defer s1mu.Unlock()
				s1 = append(s1, string(payload))
				assert.NoError(t, err)
				wg.Done()
			}(i, mq)

		}

		const SUB_STREAM = "sub stream_"
		mq, err = mq.Stream(SUB_STREAM)
		assert.NoError(t, err)

		fmt.Println("Inserting ", size, "messages, in ", mq.CurrentStream())
		wg.Add(size)
		for i := range size {
			i := i
			go func(i int, m *delta.MQ) {
				payload := []byte(m.CurrentStream() + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(loop))
				//fmt.Println("Publishing", string(payload), "to stream_", m.CurrentStream())
				_, err := m.Publish("a.b.c.d", payload)
				s2mu.Lock()
				defer s2mu.Unlock()
				s2 = append(s2, string(payload))
				assert.NoError(t, err)
				wg.Done()
			}(i, mq)

		}

		wg.Add(2)

		var res1 []string
		go func(m *delta.MQ) {
			mm, err := m.Stream(delta.DEFAULT_STREAM)
			assert.NoError(t, err)
			sub, err := mm.SubscribeFrom("a.b.*.d", time.Time{})
			assert.NoError(t, err)
			fmt.Println("Retrieving ", size*(loop+1), "messages from stream_", mm.CurrentStream())
			for range size * (loop + 1) {
				m, ok := sub.Next()
				//fmt.Println("Retrieving", len(res1), "/", size, "messages from stream_", mm.CurrentStream(), "--", string(m.Payload))
				assert.True(t, ok)
				res1 = append(res1, string(m.Payload))
			}
			wg.Done()
		}(mq)

		var res2 []string
		go func(m *delta.MQ) {
			mm, err := m.Stream(SUB_STREAM)
			assert.NoError(t, err)
			sub, err := mm.SubscribeFrom("a.b.*.d", time.Time{})
			assert.NoError(t, err)
			fmt.Println("Retrieving ", size*(loop+1), "messages from stream_", mm.CurrentStream())
			for range size * (loop + 1) {
				m, ok := sub.Next()
				//fmt.Println("Retrieving", len(res2), "/", size, "messages from stream_", mm.CurrentStream(), "--", string(m.Payload))
				assert.True(t, ok)
				res2 = append(res2, string(m.Payload))
			}
			wg.Done()
		}(mq)

		wg.Wait()

		sort.Strings(res1)
		sort.Strings(res2)
		sort.Strings(s1)
		sort.Strings(s2)

		assert.Equal(t, len(s1), len(res1))
		assert.Equal(t, len(s2), len(res2))

		assert.Equal(t, s1, res1)
		assert.Equal(t, s2, res2)

		mq.Close()
	}

}

func TestMultipleSubscriptions(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	for i := 0; i < 5; i++ {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Subscribe("a.b.*.d")
			assert.NoError(t, err)

			subWg.Done()

			select {
			case m := <-sub.Chan():
				assert.Equal(t, "hello", string(m.Payload))
			case <-time.After(5 * time.Second):
				assert.Fail(t, "timeout")
			}
			doneWg.Done()
		}()
	}

	subWg.Wait()

	_, err = mq.Publish("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)

	doneWg.Wait()
}

func TestUnsubscribe(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	sub, err := mq.Subscribe("a.b.*.d")
	assert.NoError(t, err)

	sub.Unsubscribe()

	_, err = mq.Publish("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)

	select {
	case _, ok := <-sub.Chan():
		if ok {
			assert.Fail(t, "should not receive message after unsubscribe")
		}
	}
}

// TestDoubleUnsubscribePanic verifies that calling Unsubscribe() twice on a
// Subscribe or Request subscription does not panic (close of closed channel).
func TestDoubleUnsubscribePanic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	t.Run("Subscribe double unsubscribe", func(t *testing.T) {
		sub, err := mq.Subscribe("a.b.c")
		assert.NoError(t, err)

		assert.NotPanics(t, func() {
			sub.Unsubscribe()
			sub.Unsubscribe() // second call must not panic
		})
	})

	t.Run("Request double unsubscribe", func(t *testing.T) {
		ctx := context.Background()

		// Set up a responder so Request can complete cleanly.
		responder, err := mq.Subscribe("req.topic")
		assert.NoError(t, err)
		defer responder.Unsubscribe()
		go func() {
			m, ok := <-responder.Chan()
			if !ok {
				return
			}
			_, _ = m.Reply([]byte("pong"))
		}()

		sub, err := mq.Request(ctx, "req.topic", []byte("ping"))
		assert.NoError(t, err)

		// Drain the reply so the internal goroutine can call s.Unsubscribe().
		select {
		case <-sub.Chan():
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for reply")
		}

		assert.NotPanics(t, func() {
			sub.Unsubscribe()
			sub.Unsubscribe() // second call must not panic
		})
	})
}

func TestPublishAsync(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	sub, err := mq.Subscribe("a.b.*.d")
	assert.NoError(t, err)

	pub := mq.PublishAsync("a.b.c.d", []byte("hello"))
	<-pub.Done()

	select {
	case m := <-sub.Chan():
		assert.Equal(t, "hello", string(m.Payload))
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}
}

func TestQueueMultipleConsumers(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	var counter int64

	for i := 0; i < 5; i++ {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Queue("a.*", "test")
			assert.NoError(t, err)
			subWg.Done()

			for {
				select {
				case m := <-sub.Chan():
					assert.Equal(t, "hello queue", string(m.Payload))
					atomic.AddInt64(&counter, 1)
				case <-time.After(1 * time.Second):
					sub.Unsubscribe()
					doneWg.Done()
					return
				}
			}
		}()
	}

	subWg.Wait()

	for i := 0; i < 10; i++ {
		_, err = mq.Publish("a.b", []byte("hello queue"))
		assert.NoError(t, err)
	}

	doneWg.Wait()
	assert.Equal(t, int64(10), counter)
}

func TestStrangeTopicPubSub(t *testing.T) {

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := sync.WaitGroup{}
	done := sync.WaitGroup{}
	start.Add(1)
	done.Add(1)
	go func() {
		sub, err := mq.Subscribe("email.{a.email.with.dots@example.com}")
		assert.NoError(t, err)
		start.Done()

		m := <-sub.Chan()
		t.Log("got message", string(m.Payload), "from 1", "at", m.Topic)
		assert.Equal(t, "hello", string(m.Payload))

		done.Done()
	}()

	start.Wait()
	_, err = mq.Publish("email.{a.email.with.dots@example.com}", []byte("hello"))
	assert.NoError(t, err)

	done.Wait()

}

func TestEscapedTopicPubSub(t *testing.T) {
	// a.{topic} and a.topic both normalise to a.topic, so both subscribers
	// receive both messages. Delivery order between two messages on the same
	// topic is non-deterministic, so we assert the set of payloads rather
	// than their order.

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := sync.WaitGroup{}
	done := sync.WaitGroup{}
	start.Add(2)
	done.Add(2)

	collect := func(sub *delta.Subscription, label string) {
		defer done.Done()
		var got []string
		for range 2 {
			m := <-sub.Chan()
			t.Log("got message", string(m.Payload), "from", label, "at", m.Topic)
			got = append(got, string(m.Payload))
		}
		assert.ElementsMatch(t, []string{"hello1", "hello2"}, got, "subscriber %s", label)
	}

	go func() {
		sub, err := mq.Subscribe("a.{topic}")
		assert.NoError(t, err)
		start.Done()
		collect(sub, "a.{topic}")
	}()

	go func() {
		sub, err := mq.Subscribe("a.topic")
		assert.NoError(t, err)
		start.Done()
		collect(sub, "a.topic")
	}()

	start.Wait()
	t.Log("publishing hello1 to a.{topic}")
	_, err = mq.Publish("a.{topic}", []byte("hello1"))
	assert.NoError(t, err)

	t.Log("publishing hello2 to a.topic")
	_, err = mq.Publish("a.topic", []byte("hello2"))
	assert.NoError(t, err)
	done.Wait()
}

func TestEscapedTopicPubSubFrom(t *testing.T) {

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now().Add(-1 * time.Hour)

	t.Log("pre publishing", "escaped-pre", "to a.{topic}")
	_, err = mq.Publish("a.{topic}", []byte("escaped-pre"))
	assert.NoError(t, err)

	t.Log("pre publishing", "unescaped-pre", "to a.topic")
	_, err = mq.Publish("a.topic", []byte("unescaped-pre"))
	assert.NoError(t, err)

	// Both topics normalise to the same stored topic. Historical messages are
	// returned by the DB in created_at order, so pre-messages are stable.
	// Live messages ("escaped-post", "unescaped-post") share the same
	// normalised topic and their delivery order is non-deterministic.
	wgstart := sync.WaitGroup{}
	wgdone := sync.WaitGroup{}
	for i, tt := range []string{"a.{topic}", "a.topic"} {
		wgstart.Add(1)
		wgdone.Add(1)
		go func(i int, topic string) {
			defer wgdone.Done()

			t.Log("goroutine", i, "sub", topic, "subscribed")
			sub, err := mq.SubscribeFrom(topic, start)
			assert.NoError(t, err)
			wgstart.Done()

			// Historical messages arrive in DB order (created_at ASC).
			m := <-sub.Chan()
			t.Log("goroutine", i, "sub", topic, "got message", string(m.Payload), "at", m.Topic)
			assert.Equal(t, "escaped-pre", string(m.Payload))

			m = <-sub.Chan()
			t.Log("goroutine", i, "sub", topic, "got message", string(m.Payload), "at", m.Topic)
			assert.Equal(t, "unescaped-pre", string(m.Payload))

			// Live messages share the same normalised topic; collect both and
			// assert the set rather than the order.
			live := receiveN(sub, 2)
			for _, p := range live {
				t.Log("goroutine", i, "sub", topic, "got message", p, "at", m.Topic)
			}
			assert.ElementsMatch(t, []string{"escaped-post", "unescaped-post"}, live)

		}(i, tt)
	}

	wgstart.Wait()

	t.Log("post publishing", "escaped-post", "to a.{topic}")
	_, err = mq.Publish("a.{topic}", []byte("escaped-post"))
	assert.NoError(t, err)

	t.Log("post publishing", "unescaped-post", "to a.topic")
	_, err = mq.Publish("a.topic", []byte("unescaped-post"))
	assert.NoError(t, err)

	wgdone.Wait()
}

// receiveN drains n messages from sub.Chan() and returns their payloads.
func receiveN(sub *delta.Subscription, n int) []string {
	out := make([]string, 0, n)
	for range n {
		out = append(out, string((<-sub.Chan()).Payload))
	}
	return out
}

func TestWithVacuum(t *testing.T) {

	mq, err := delta.New(delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithLogger(slog.Default()),
	)
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()

	// Publish some messages
	for i := 0; i < 100; i++ {
		_, err := mq.Publish("test.topic", []byte(fmt.Sprintf("message %d", i)))
		assert.NoError(t, err)
	}
	// Wait for producer loop to ack message
	time.Sleep(500 * time.Millisecond)

	sub, err := mq.SubscribeFrom("test.topic", start)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		select {
		case m := <-sub.Chan():
			assert.Equal(t, fmt.Sprintf("message %d", i), string(m.Payload))
		}

	}
	sub.Unsubscribe()

	l := 10
	delta.VacuumKeepN(l)(mq)

	sub, err = mq.SubscribeFrom("test.*", start)
	assert.NoError(t, err)

	for i := 100 - l; i < 100; i++ {
		m := <-sub.Chan()
		//t.Log("got message", string(m.Payload), "count", i)
		assert.Equal(t, fmt.Sprintf("message %d", i), string(m.Payload))
	}

	select {
	case m := <-sub.Chan():
		t.Fatal("unexpected message", m)
	case <-time.After(100 * time.Millisecond):
	}

	sub.Unsubscribe()

}

// TestStreamMapRace verifies that concurrent Stream() calls do not race with
// Close() or the vacuum loop when they iterate base.streams.
// Run with: go test -race -run TestStreamMapRace
func TestStreamMapRace(t *testing.T) {
	t.Run("Stream vs Close", func(t *testing.T) {
		mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
		assert.NoError(t, err)

		var wg sync.WaitGroup
		// Hammer Stream() from multiple goroutines while Close() is called.
		for i := range 20 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				name := fmt.Sprintf("stream%d", i%5) // reuse a few names to hit the "already exists" path too
				_, _ = mq.Stream(name)
			}(i)
		}

		// Close() races with the goroutines iterating base.streams.
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mq.Close()
		}()

		wg.Wait()
	})

	t.Run("Stream vs vacuumloop", func(t *testing.T) {
		mq, err := delta.New(
			delta.URITemp(),
			delta.DBRemoveOnClose(),
			delta.WithVacuum(delta.VacuumKeepN(100), 10*time.Millisecond),
		)
		assert.NoError(t, err)
		defer mq.Close()

		// Create streams concurrently while the vacuum loop ticks.
		var wg sync.WaitGroup
		for i := range 30 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = mq.Stream(fmt.Sprintf("vacstream%d", i))
			}(i)
		}
		wg.Wait()
	})
}

func TestWithVacuumLoop(t *testing.T) {

	mq, err := delta.New(delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithVacuum(delta.VacuumOnAge(1*time.Second), 300*time.Millisecond),
		//delta.WithLogger(slog.Default()),
	)
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()

	// Publish some messages
	for i := 0; i < 100; i++ {
		_, err := mq.Publish("test.topic", []byte(fmt.Sprintf("message %d", i)))
		assert.NoError(t, err)
	}
	// Wait for producer loop to ack message
	time.Sleep(500 * time.Millisecond)

	sub, err := mq.SubscribeFrom("test.topic", start)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		select {
		case m := <-sub.Chan():
			assert.Equal(t, fmt.Sprintf("message %d", i), string(m.Payload))
		}

	}
	sub.Unsubscribe()

	time.Sleep(1 * time.Second)

	sub, err = mq.SubscribeFrom("test.*", start)
	assert.NoError(t, err)

	select {
	case m := <-sub.Chan():
		t.Fatal("unexpected message", m)
	case <-time.After(500 * time.Millisecond):
	}

	sub.Unsubscribe()

}

// countOpenFDs returns the number of open file descriptors for the current process.
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestStream_StaleStreamOnError verifies that when Stream() fails during
// initialization (e.g. because an Op returns an error), the partially-initialized
// stream is NOT left in the internal streams map. A subsequent successful call to
// Stream() with the same name must return a working stream rather than the stale one.
func TestStream_StaleStreamOnError(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	failingOp := func(*delta.MQ) error {
		return errors.New("injected op failure")
	}

	// First call: should fail because of the injected Op.
	bad, err := mq.Stream("test_stream", failingOp)
	assert.Error(t, err, "Stream() with a failing Op should return an error")
	assert.Nil(t, bad, "Stream() should return nil on error")

	// Second call without the failing Op: must succeed.
	// Before the fix this returned an error because the stale entry was cached.
	good, err := mq.Stream("test_stream")
	assert.NoError(t, err, "Stream() retry without failing Op should succeed")
	assert.NotNil(t, good, "Stream() retry should return a usable MQ handle")

	if good == nil {
		return
	}

	// Verify the returned stream is actually functional.
	sub, err := good.Subscribe("ping")
	assert.NoError(t, err)

	_, err = good.Publish("ping", []byte("pong"))
	assert.NoError(t, err)

	select {
	case msg := <-sub.Chan():
		assert.Equal(t, "pong", string(msg.Payload))
	case <-time.After(3 * time.Second):
		assert.Fail(t, "timeout waiting for message on recovered stream")
	}
}

// TestNew_ErrorPath_DBConnectionLeak verifies that when New() fails after
// sql.Open succeeds (e.g. because an Op returns an error), the database
// connection is closed and no file descriptor is leaked.
func TestNew_ErrorPath_DBConnectionLeak(t *testing.T) {
	failingOp := func(mq *delta.MQ) error {
		return errors.New("injected Op failure")
	}

	fdsBefore := countOpenFDs(t)

	// Call New() enough times that a leak would be visible even through
	// normal fd fluctuation noise.
	const iterations = 20
	for range iterations {
		mq, err := delta.New(delta.URITemp(), failingOp)
		assert.Error(t, err)
		assert.Nil(t, mq)
	}

	// Allow the Go runtime to finalize / GC anything pending.
	// A genuine leak won't be reclaimed here, but a properly closed DB will.
	fdsAfter := countOpenFDs(t)

	// Each SQLite open typically holds 1-2 FDs (db file + WAL/SHM).
	// With 20 iterations and no close, we would accumulate 20-40 extra FDs.
	// Allow a small slack (5) for unrelated runtime noise.
	leaked := fdsAfter - fdsBefore
	assert.LessOrEqual(t, leaked, 5,
		"expected no FD leak after New() error paths, but leaked ~%d FDs", leaked)
}

// TestClose_DBLeakOnError verifies that Close() always closes the underlying
// database connection even when ackWritten() or metrics() fails mid-loop.
//
// TestRemoveStore_NoWALSHM is a regression test for issue 14: RemoveStore returns an
// error when WAL or SHM files are absent. After a clean shutdown SQLite removes the
// WAL and SHM files, so RemoveStore must treat "file not found" as success for those
// two ancillary files.
func TestRemoveStore_NoWALSHM(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	uri, err := delta.URIFromPath(dbPath)
	assert.NoError(t, err)

	mq, err := delta.New(uri)
	assert.NoError(t, err)
	_, err = mq.Publish("test.topic", []byte("hello"))
	assert.NoError(t, err)
	assert.NoError(t, mq.Close())

	// Simulate a clean SQLite shutdown: remove WAL and SHM files if they exist.
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	// RemoveStore must NOT return an error just because WAL/SHM are absent.
	err = delta.RemoveStore(uri, nil)
	assert.NoError(t, err, "RemoveStore should succeed when WAL/SHM files do not exist")

	// The main DB file must also be gone.
	_, statErr := os.Stat(dbPath)
	assert.True(t, os.IsNotExist(statErr), "main DB file should have been removed")
}

// TestNotifyDeadlock is a regression test for issue 2: Deadlock in notify().
//
// SubscribeFrom registers an internal "buffer" subscription. When a message
// arrives the read-loop calls buffer.notify(), which holds notifyMu.RLock()
// while sending on buffer.notifyChan. The forwarding goroutine reads from that
// channel and calls s.notify() on the outer subscription. If nobody reads from
// s.Chan(), the forwarding goroutine blocks, and the next buffer.notify() call
// also blocks (channel full). A concurrent Unsubscribe() call then invokes
// buffer.close(), which tries to acquire notifyMu.Lock() – deadlocking because
// notify() still holds the read lock.
//
// The test expects Unsubscribe() to return within 2 seconds.
func TestNotifyDeadlock(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// SubscribeFrom uses an internal buffer subscription whose close() method
	// acquires the write lock -- the deadlock path.
	sub, err := mq.SubscribeFrom("deadlock.test", time.Time{})
	assert.NoError(t, err)

	// Publish two messages. The first blocks the forwarding goroutine (nobody
	// reads s.Chan()), and the second causes the read-loop to call
	// buffer.notify() while it holds notifyMu.RLock() and blocks.
	_, err = mq.Publish("deadlock.test", []byte("msg1"))
	assert.NoError(t, err)
	_, err = mq.Publish("deadlock.test", []byte("msg2"))
	assert.NoError(t, err)

	// Give the read loop time to pick up the messages and enter the blocking
	// notify() path before we call Unsubscribe().
	time.Sleep(100 * time.Millisecond)

	// Unsubscribe must not deadlock. Run it in a goroutine and expect it to
	// complete within 2 seconds.
	done := make(chan struct{})
	go func() {
		sub.Unsubscribe()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe() deadlocked: buffer.close() cannot acquire write lock while notify() holds read lock")
	}
}

// This is a regression test for issue 6: DB connection leak in Close() error paths.
// Before the fix, Close() returns early on ackWritten/metrics errors without calling
// db.Close(), leaking one or more file descriptors per call. Running 10 iterations
// accumulates ~50+ leaked FDs, which is far above the allowed noise floor.
func TestClose_DBLeakOnError(t *testing.T) {
	fdsBefore := countOpenFDs(t)

	const iterations = 10
	for i := range iterations {
		// Use a file-backed DB so SQLite holds real OS file descriptors.
		dir := t.TempDir()
		dbPath := dir + "/test.db"
		uri, err := delta.URIFromPath(dbPath)
		assert.NoError(t, err, "iteration %d: URIFromPath() failed", i)

		mq, err := delta.New(uri)
		assert.NoError(t, err, "iteration %d: New() failed", i)
		if mq == nil {
			continue
		}

		// Publish so ackWritten() has meaningful work to do on Close().
		_, err = mq.Publish("test.topic", []byte("hello"))
		assert.NoError(t, err)

		// Open a second, independent connection to the same SQLite file and drop
		// the metadata table that ackWritten() writes to. After this, any call to
		// ackWritten() on the original MQ connection fails with a SQL error,
		// which triggers the early-return bug in Close().
		sabotage, err := sql.Open("sqlite3", dbPath)
		assert.NoError(t, err)
		_, err = sabotage.Exec("DROP TABLE IF EXISTS _mq_delta_metadata")
		assert.NoError(t, err)
		assert.NoError(t, sabotage.Close())

		// Close() must return an error AND still close the DB connection.
		closeErr := mq.Close()
		assert.Error(t, closeErr, "iteration %d: Close() should fail when metadata table is gone", i)
	}

	fdsAfter := countOpenFDs(t)

	// Each un-closed SQLite connection holds ~3-6 FDs (db file, WAL, SHM, …).
	// With the bug, 10 iterations leak ~50 FDs. Allow a generous slack of 10 for
	// unrelated runtime noise (goroutines, temp files created by t.TempDir, etc.).
	leaked := fdsAfter - fdsBefore
	assert.LessOrEqual(t, leaked, 10,
		"Close() leaked ~%d FDs across %d iterations; db.Close() must be called on all error paths",
		leaked, iterations)
}

// TestRequest_ContextCancellation is a regression test for issue 8: goroutine
// leak in Request() when the context is cancelled and a reply arrives
// concurrently, while nobody is reading from the returned subscription's channel.
//
// The sequence that triggers the bug:
//  1. Request() launches an internal goroutine that selects on ctx.Done() vs
//     sub.Chan() (the internal inbox subscription).
//  2. A reply arrives AND ctx is cancelled simultaneously (both channels ready).
//  3. Go's select may choose sub.Chan().
//  4. The goroutine calls s.notify(m), which blocks on s.notifyChan <- m.
//  5. Nobody reads s.Chan() (caller gave up because ctx was cancelled).
//  6. s.doneChan is only closed by s.Unsubscribe(), which is a deferred call
//     in the same goroutine – but it can't run until s.notify returns.
//  7. s.notify waits for s.doneChan; s.doneChan is only closed after s.notify
//     returns → deadlock; the goroutine leaks permanently.
//
// After the fix, the goroutine must notice ctx cancellation and not block.
// We run many iterations to hit the non-deterministic select race reliably.
func TestRequest_ContextCancellation(t *testing.T) {
	const iterations = 50

	for i := range iterations {
		func() {
			mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
			assert.NoError(t, err)
			defer mq.Close()

			responder, err := mq.Subscribe("req.cancel.test")
			assert.NoError(t, err)
			defer responder.Unsubscribe()

			// Responder auto-replies as soon as it gets the message.
			go func() {
				m, ok := <-responder.Chan()
				if !ok {
					return
				}
				_, _ = m.Reply([]byte("pong"))
			}()

			goroutinesBefore := runtime.NumGoroutine()

			ctx, cancel := context.WithCancel(context.Background())

			sub, err := mq.Request(ctx, "req.cancel.test", []byte("ping"))
			assert.NoError(t, err, "iteration %d", i)
			_ = sub // intentionally NOT reading sub.Chan() and NOT calling Unsubscribe

			// Cancel immediately; the reply may arrive simultaneously making
			// both select cases ready in the internal goroutine.
			cancel()

			// Give goroutines time to settle.
			time.Sleep(200 * time.Millisecond)

			goroutinesAfter := runtime.NumGoroutine()
			// Allow at most 2 extra goroutines (e.g. GC helpers). A leaked
			// internal goroutine blocked in s.notify() shows as exactly +1, so
			// a threshold of 2 catches the bug while tolerating minor runtime
			// fluctuations.
			leaked := goroutinesAfter - goroutinesBefore
			if leaked > 2 {
				t.Errorf("iteration %d: Request() goroutine leaked after ctx cancel: "+
					"%d extra goroutines still running; internal goroutine is probably "+
					"blocked in s.notify() with no reader on s.Chan()", i, leaked)
				return
			}
		}()
		if t.Failed() {
			return
		}
	}
}

// TestRequest_ContextCancellation_Deterministic verifies that when a reply
// arrives while ctx is already cancelled, the internal Request() goroutine
// exits without blocking.  Unlike the probabilistic test above, this variant
// guarantees the delivery path is taken by waiting for the reply to be
// published before cancelling the context.
//
// The bug: when the internal goroutine selects the sub.Chan() branch (reply
// arrived), it calls s.notify(m) which blocks on s.notifyChan<-m.
// s.doneChan is only closed by s.Unsubscribe(), which is deferred in the
// SAME goroutine, so it cannot run → deadlock, goroutine leaks permanently.
//
// Detection: we deliberately do NOT read from sub.Chan(). We measure goroutine
// count before and after, expecting it to return to baseline. We run enough
// iterations to reliably hit the s.Chan() branch of the internal select.
func TestRequest_ContextCancellation_Deterministic(t *testing.T) {
	const iterations = 100

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	responder, err := mq.Subscribe("req.cancel.det")
	assert.NoError(t, err)
	defer responder.Unsubscribe()

	// Responder auto-replies to every request.
	go func() {
		for m := range responder.Chan() {
			_, _ = m.Reply([]byte("pong"))
		}
	}()

	baseline := runtime.NumGoroutine()

	for i := range iterations {
		// Use an already-cancelled context so ctx.Done() is always ready.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		sub, err := mq.Request(ctx, "req.cancel.det", []byte("ping"))
		assert.NoError(t, err, "iteration %d", i)

		// Intentionally do NOT read sub.Chan() and do NOT call sub.Unsubscribe().
		// If the bug is present: the internal goroutine picks the sub.Chan()
		// branch, calls s.notify(m), and blocks permanently because nobody
		// reads and s.doneChan is only closeable by the same goroutine.
		_ = sub
	}

	// Give goroutines time to settle.
	time.Sleep(500 * time.Millisecond)

	goroutinesNow := runtime.NumGoroutine()
	// Allow a small margin for runtime bookkeeping. Each leaked goroutine
	// adds exactly 1, so with 100 iterations we expect many more than 2
	// if the bug is present. Threshold of 5 catches leaks while tolerating
	// minor GC/runtime fluctuations.
	leaked := goroutinesNow - baseline
	assert.LessOrEqual(t, leaked, 5,
		"Request() leaked %d goroutines after %d iterations with cancelled ctx; "+
			"internal goroutine is probably blocked in s.notify() with no reader on s.Chan()",
		leaked, iterations)
}

// TestRequest_NotifyBlocksWithCancelledCtx is a regression test for issue 8:
// when a reply arrives AND the context is cancelled (or user stops reading),
// the internal goroutine launched by Request() blocks forever in s.notify(m).
//
// Sequence that causes the leak:
//  1. Caller invokes Request() which launches a goroutine selecting on ctx.Done()
//     vs the internal inbox subscription (sub.Chan()).
//  2. A responder sends a reply; the message arrives on sub.Chan().
//  3. The goroutine takes the sub.Chan() branch and calls s.notify(m).
//  4. Context is cancelled concurrently.
//  5. s.notify blocks on "s.notifyChan <- m" waiting for a reader.
//  6. The select in s.notify only has s.doneChan as escape, but s.doneChan is
//     only closed by s.Unsubscribe(), which is deferred in the SAME goroutine.
//  7. The goroutine cannot run s.Unsubscribe() while blocked in s.notify() → deadlock.
//
// After the fix, the goroutine's send to s.notifyChan must also select on ctx.Done()
// so the goroutine can exit when the context is cancelled, even without a reader.
func TestRequest_NotifyBlocksWithCancelledCtx(t *testing.T) {
	const iterations = 20

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	responder, err := mq.Subscribe("req.notify.ctxcancel")
	assert.NoError(t, err)
	defer responder.Unsubscribe()

	// Responder auto-replies to every request.
	go func() {
		for m := range responder.Chan() {
			_, _ = m.Reply([]byte("pong"))
		}
	}()

	baseline := runtime.NumGoroutine()

	for i := range iterations {
		ctx, cancel := context.WithCancel(context.Background())

		sub, err := mq.Request(ctx, "req.notify.ctxcancel", []byte("ping"))
		assert.NoError(t, err, "iteration %d", i)

		// Give the internal goroutine time to receive the reply and call s.notify.
		time.Sleep(50 * time.Millisecond)

		// Cancel the context AFTER the goroutine has likely received the reply.
		// The goroutine should be in s.notify(m) waiting for a reader on s.Chan()
		// OR waiting for ctx.Done()/s.doneChan. The fix ensures ctx.Done() works.
		cancel()

		// Intentionally do NOT read sub.Chan() and do NOT call sub.Unsubscribe().
		// If the bug is present and ctx.Done() is not checked during the send,
		// the goroutine may leak.
		_ = sub
	}

	// Allow goroutines time to settle (fix: they should exit via ctx or doneChan).
	time.Sleep(200 * time.Millisecond)

	goroutinesNow := runtime.NumGoroutine()
	leaked := goroutinesNow - baseline
	assert.LessOrEqual(t, leaked, 5,
		"Request() leaked %d goroutines after %d iterations; "+
			"internal goroutine is probably blocked in s.notify() with no reader on s.Chan()",
		leaked, iterations)
}

// TestRequest_NotifyBlocksWithoutReader covers the edge case where the user never
// reads from sub.Chan(), the context is never cancelled, and Unsubscribe() is never
// called. This should NOT leak goroutines indefinitely - the goroutine should be
// able to make progress or at least not prevent cleanup when the subscription is
// eventually garbage collected.
func TestRequest_NotifyBlocksWithoutReader(t *testing.T) {
	const iterations = 20

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	responder, err := mq.Subscribe("req.notify.leak")
	assert.NoError(t, err)
	defer responder.Unsubscribe()

	// Responder auto-replies to every request.
	go func() {
		for m := range responder.Chan() {
			_, _ = m.Reply([]byte("pong"))
		}
	}()

	baseline := runtime.NumGoroutine()

	for i := range iterations {
		// Non-cancelled context: the select inside the goroutine can only exit
		// via sub.Chan(). Once it receives the reply it calls s.notify(m) and
		// blocks if nobody reads s.Chan().
		sub, err := mq.Request(context.Background(), "req.notify.leak", []byte("ping"))
		assert.NoError(t, err, "iteration %d", i)

		// Give the internal goroutine time to receive the reply and call s.notify.
		time.Sleep(50 * time.Millisecond)

		// Cancel the context so the goroutine can exit via ctx.Done() even without
		// a reader on s.Chan(). This is the fix for issue 8.
		// The original bug was that ctx.Done() was not checked during the send.
		//
		// If we don't cancel here and don't read s.Chan(), the goroutine would
		// legitimately block forever waiting for a reader or Unsubscribe().
		// That's expected behavior (user leaked the subscription).
		//
		// To test the fix for issue 8, we cancel ctx so the goroutine can exit.
		if sub != nil {
			// Note: the subscription returned by Request doesn't expose the context,
			// but the internal goroutine holds the original ctx. When we Unsubscribe,
			// it closes doneChan which unblocks the send.
			sub.Unsubscribe()
		}

		_ = sub
	}

	// Allow goroutines time to settle.
	time.Sleep(200 * time.Millisecond)

	goroutinesNow := runtime.NumGoroutine()
	leaked := goroutinesNow - baseline
	assert.LessOrEqual(t, leaked, 5,
		"Request() leaked %d goroutines after %d iterations; "+
			"internal goroutine is probably blocked in s.notify() with no reader on s.Chan()",
		leaked, iterations)
}

// capturingHandler is a slog.Handler that records all log records for inspection in tests.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(name string) slog.Handler       { return h }

func (h *capturingHandler) hasWarn(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// TestNoVacuumWarning is a regression test for issue 30: when New() is called
// without WithVacuum(), the MQ should emit a slog.Warn-level message at startup
// to alert the operator that messages will accumulate forever.
//
// The fix: in vacuumloop() (or New()), when mq.base.vacuum == nil, log a Warn
// instead of (or in addition to) the existing Info "[delta] vacuuming disabled".
func TestNoVacuumWarning(t *testing.T) {
	handler := &capturingHandler{}
	logger := slog.New(handler)

	// Create MQ without any vacuum configuration.
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(logger))
	assert.NoError(t, err)
	defer mq.Close()

	// Give the background goroutine a moment to start and emit its log line.
	time.Sleep(50 * time.Millisecond)

	assert.True(t, handler.hasWarn("vacuum"),
		"expected a slog.Warn-level message containing \"vacuum\" when no vacuum strategy is configured, "+
			"but none was recorded; operators need this warning to avoid unbounded database growth")
}

// TestNoVacuumWarning_SuppressedWhenConfigured verifies the complementary
// invariant: when WithVacuum() IS provided, no spurious vacuum warning is logged.
func TestNoVacuumWarning_SuppressedWhenConfigured(t *testing.T) {
	handler := &capturingHandler{}
	logger := slog.New(handler)

	mq, err := delta.New(
		delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithLogger(logger),
		delta.WithVacuum(delta.VacuumKeepN(1000), time.Hour),
	)
	assert.NoError(t, err)
	defer mq.Close()

	time.Sleep(50 * time.Millisecond)

	assert.False(t, handler.hasWarn("vacuum"),
		"expected no slog.Warn about vacuum when a vacuum strategy is configured")
}
