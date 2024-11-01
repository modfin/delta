package delta_test

import (
	"context"
	"fmt"
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"log/slog"
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
