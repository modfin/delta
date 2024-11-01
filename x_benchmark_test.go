package delta_test

import (
	"fmt"
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkParPub(b *testing.B) {

	do := func(ops ...delta.Op) func(b *testing.B) {
		return func(b *testing.B) {
			topic := "benchmark"
			payload := []byte("a")

			ops = append(ops, delta.DBRemoveOnClose())
			mq, err := delta.New(delta.URITemp(), ops...)
			assert.NoError(b, err)
			defer mq.Close()

			b.ResetTimer()
			start := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err = mq.Publish(topic, payload)
					assert.NoError(b, err)
				}
			})
			elapsed := time.Since(start)
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
		}
	}

	b.Run("simple", do())
	b.Run("no-sync", do(delta.DBSyncOff()))
}

func BenchmarkMultipleSubscribers(b *testing.B) {

	of := func(readers int) func(b *testing.B) {
		return func(b *testing.B) {
			mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
			if err != nil {
				b.Fatalf("could not create MQ: %v", err)
			}
			defer mq.Close()

			topic := "benchmark"

			payload := []byte("b")

			if err != nil {
				b.Fatalf("could not subscribe: %v", err)
			}

			var totatMsgRecieved int64

			wg_sub_done := sync.WaitGroup{}
			wg_sub_start := sync.WaitGroup{}
			wg_sub_done.Add(readers)
			wg_sub_start.Add(readers)
			for range readers {
				go func() {
					wg_sub_start.Done()
					sub, err := mq.Subscribe(topic)
					defer sub.Unsubscribe()
					assert.NoError(b, err)
					i := 0
					for {
						_, ok := sub.Next()
						if !ok {
							return
						}

						atomic.AddInt64(&totatMsgRecieved, 1)
						i += 1
						if i == b.N {
							break
						}
					}
					wg_sub_done.Done()
				}()

			}

			wg_sub_start.Wait()

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				_, err := mq.Publish(topic, payload)
				if err != nil {
					b.Fatalf("could not publish: %v", err)
				}
			}
			elapsed := time.Since(start)
			wg_sub_start.Wait()
			b.StopTimer()
			fullelapsed := time.Since(start)
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "write-msg/s")
			b.ReportMetric(float64(atomic.LoadInt64(&totatMsgRecieved))/fullelapsed.Seconds(), "read-msg/s")
		}
	}

	b.Run("1", of(1))
	b.Run("4", of(4))
	b.Run(fmt.Sprintf("num-cpu (%d)", runtime.NumCPU()), of(runtime.NumCPU()))
	b.Run(fmt.Sprintf("2x num-cpu (%d)", 2*runtime.NumCPU()), of(2*runtime.NumCPU()))
}

func BenchmarkMultipleSubscribersSize(b *testing.B) {

	of := func(size int) func(b *testing.B) {
		return func(b *testing.B) {

			readers := runtime.NumCPU()

			mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
			if err != nil {
				b.Fatalf("could not create MQ: %v", err)
			}
			defer mq.Close()

			topic := "benchmark"

			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = byte(rand.Int())
			}

			if err != nil {
				b.Fatalf("could not subscribe: %v", err)
			}

			var totatMsgRecieved int64

			wg_sub_done := sync.WaitGroup{}
			wg_sub_start := sync.WaitGroup{}
			wg_sub_done.Add(readers)
			wg_sub_start.Add(readers)
			for range readers {
				go func() {
					wg_sub_start.Done()
					sub, err := mq.Subscribe(topic)
					defer sub.Unsubscribe()
					assert.NoError(b, err)
					i := 0
					for {
						_, ok := sub.Next()
						if !ok {
							return
						}

						atomic.AddInt64(&totatMsgRecieved, 1)
						i += 1
						if i == b.N {
							break
						}
					}
					wg_sub_done.Done()
				}()

			}

			wg_sub_start.Wait()

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				_, err := mq.Publish(topic, payload)
				if err != nil {
					b.Fatalf("could not publish: %v", err)
				}
			}
			elapsed := time.Since(start)
			wg_sub_start.Wait()
			b.StopTimer()
			fullelapsed := time.Since(start)

			b.ReportMetric(float64(len(payload)*b.N)/elapsed.Seconds()/float64(1_000_000), "write-MB/s")
			b.ReportMetric(float64(int64(len(payload))*atomic.LoadInt64(&totatMsgRecieved))/fullelapsed.Seconds()/float64(1_000_000), "read-MB/s")

			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "write-msg/s")
			b.ReportMetric(float64(atomic.LoadInt64(&totatMsgRecieved))/fullelapsed.Seconds(), "read-msg/s")
		}
	}

	b.Run(fmt.Sprintf("_0.1mb"), of(100_000))
	b.Run(fmt.Sprintf("_1.0mb"), of(1_000_000))
	b.Run(fmt.Sprintf("10.0mb"), of(10_000_000))
}
