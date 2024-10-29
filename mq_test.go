package delta_test

import (
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSimplePubSub(t *testing.T) {

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	for range 1000 {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Sub("a.b.*.d")
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

	_, err = mq.Pub("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)
	//fmt.Printf("pub: %+v\n", pub)

	doneWg.Wait()

}

func TestSimplePubSub2(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(slog.LevelInfo)

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(slog.Default()))
	assert.NoError(t, err)

	var subWg sync.WaitGroup
	var doneWg sync.WaitGroup
	for range 1000 {
		subWg.Add(1)
		doneWg.Add(1)
		go func() {
			sub, err := mq.Sub("a.b.*.d")
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

	_, err = mq.Pub("a.b.c.d", []byte("hello"))
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	_, err = mq.Pub("a.b.c.d", []byte("world"))
	assert.NoError(t, err)
	//fmt.Printf("pub: %+v\n", pub)

	doneWg.Wait()

}

func TestSimpleQueue(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(slog.LevelInfo)

	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.WithLogger(slog.Default()))
	assert.NoError(t, err)

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
		_, err = mq.Pub("a.b", []byte("hello queue"))
		assert.NoError(t, err)
	}
	doneWg.Wait()
	count := atomic.LoadInt64(&counter)

	assert.Equal(t, int64(size), count)

}
