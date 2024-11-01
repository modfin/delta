package delta_test

import (
	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func BenchmarkParPub(b *testing.B) {

	do := func(ops ...delta.Op) func(b *testing.B) {
		return func(b *testing.B) {
			topic := "a.b"
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

func BenchmarkSeqPub(b *testing.B) {

	do := func(ops ...delta.Op) func(b *testing.B) {
		return func(b *testing.B) {
			topic := "a.b"
			payload := []byte("a")

			ops = append(ops, delta.DBRemoveOnClose())
			mq, err := delta.New(delta.URITemp(), ops...)
			assert.NoError(b, err)
			defer mq.Close()

			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				_, err = mq.Publish(topic, payload)
				assert.NoError(b, err)
			}

			elapsed := time.Since(start)
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
		}
	}

	b.Run("simple", do())
	b.Run("no-sync", do(delta.DBSyncOff()))

}
