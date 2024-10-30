package delta_test

import (
	"context"
	"fmt"
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"log/slog"
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

			sub.Unsub()
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
			sub.Unsub()
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
		_, err = m.Reply([]byte("hello " + name))
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

	size := 5_000
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

	sub, err := mq.SubFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages")
	start := time.Now()
	for i := range size {
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(res[i]), string(m.Payload), "i: %d", i)
	}
	fmt.Println("GLOB", "size", size, "time", time.Since(start), "per msg", time.Since(start)/time.Duration(size))
	sub.Unsub()

	sub, err = mq.SubFrom("a.b.c.d", from)
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

	sub, err := mq.SubFrom("a.b.*.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages from stream", mq.CurrentStream())
	for i := range size {
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(s1[i]), string(m.Payload), "i: %d", i)
	}

	mq, err = mq.Stream(SUB_STREAM)
	assert.NoError(t, err)

	sub, err = mq.SubFrom("a.b.c.d", from)
	assert.NoError(t, err)
	fmt.Println("Retrieving ", size, "messages from stream", mq.CurrentStream())
	for i := range size {
		m, ok := sub.Next()
		assert.True(t, ok)
		assert.Equal(t, string(s2[i]), string(m.Payload), "i: %d", i)
	}

}
