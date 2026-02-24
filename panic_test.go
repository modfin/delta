package delta

// panic_test.go — regression tests for issue #9: no panic recovery in background goroutines.
//
// These tests are in package delta (white-box) so they can access the unexported
// setTestPanicHook / getTestPanicHook functions and inject a controlled panic into
// background goroutines.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestPanicRecoveryInConsumerGoroutine verifies that a panic inside the read-loop
// consumer goroutine does NOT crash the process, and that the MQ remains functional
// afterwards (subsequent publishes are still delivered to subscribers).
func TestPanicRecoveryInConsumerGoroutine(t *testing.T) {
	mq, err := New(URITemp(), DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var panicCount atomic.Int32
	var once sync.Once

	// Install a panic hook that fires exactly once (on the first consumer iteration).
	setTestPanicHook(func() {
		once.Do(func() {
			panicCount.Add(1)
			panic("injected test panic in consumer goroutine")
		})
	})
	defer setTestPanicHook(nil)

	// Publish a message to trigger the consumer goroutine (which will panic once).
	_, err = mq.Publish("panic.test", []byte("trigger"))
	assert.NoError(t, err)

	// Give the consumer time to panic and (with recovery) restart.
	time.Sleep(300 * time.Millisecond)

	// The panic hook should have fired exactly once.
	assert.EqualValues(t, 1, panicCount.Load(), "panic hook should have fired once")

	// Now verify the MQ is still functional: subscribe and publish a new message.
	sub, err := mq.Subscribe("panic.test.after")
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	// Remove the hook so normal operation can proceed.
	setTestPanicHook(nil)

	_, err = mq.Publish("panic.test.after", []byte("still alive"))
	assert.NoError(t, err)

	select {
	case m := <-sub.Chan():
		assert.Equal(t, "still alive", string(m.Payload),
			"MQ must remain functional after a recovered panic in a consumer goroutine")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: MQ did not deliver message after recovering from panic in consumer goroutine")
	}
}

// TestPanicRecoveryInProducerGoroutine verifies that a panic inside the read-loop
// producer goroutine does NOT crash the process, and that the MQ recovers.
func TestPanicRecoveryInProducerGoroutine(t *testing.T) {
	mq, err := New(URITemp(), DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// The producer goroutine runs continuously; verify the MQ still works
	// after the panic hook fires during its loop.
	var panicCount atomic.Int32
	var once sync.Once

	setTestPanicHook(func() {
		once.Do(func() {
			panicCount.Add(1)
			panic("injected test panic in producer goroutine")
		})
	})
	defer setTestPanicHook(nil)

	// Trigger the producer by publishing.
	_, err = mq.Publish("panic.producer.test", []byte("trigger"))
	assert.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	setTestPanicHook(nil)

	sub, err := mq.Subscribe("panic.producer.after")
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = mq.Publish("panic.producer.after", []byte("still alive"))
	assert.NoError(t, err)

	select {
	case m := <-sub.Chan():
		assert.Equal(t, "still alive", string(m.Payload),
			"MQ must remain functional after a recovered panic in a producer goroutine")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: MQ did not deliver message after recovering from panic in producer goroutine")
	}
}
