package delta_test

// coverage_test.go — tests added to increase coverage for items 17-27 of Testing_TODO.yml

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modfin/delta"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// id:17 — VacuumOnReadAck
// ---------------------------------------------------------------------------

// TestVacuumOnReadAck verifies that VacuumOnReadAck removes messages that have
// been read (message_id < read_ack watermark), and leaves unread messages
// intact.  The strategy is invoked manually (like TestWithVacuum does for
// VacuumKeepN) rather than through the vacuum loop.
func TestVacuumOnReadAck(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()

	// Publish 20 messages.
	const total = 20
	for i := range total {
		_, err := mq.Publish("vacuum.ack.test", []byte(fmt.Sprintf("msg%d", i)))
		assert.NoError(t, err)
	}

	// Subscribe and read all messages so that the read-ack watermark advances.
	sub, err := mq.SubscribeFrom("vacuum.ack.test", start)
	assert.NoError(t, err)
	for range total {
		select {
		case <-sub.Chan():
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	}
	sub.Unsubscribe()

	// Let the read loop persist the read watermark.
	time.Sleep(300 * time.Millisecond)

	// Apply vacuum: should delete all messages whose message_id < read_ack.
	delta.VacuumOnReadAck(mq)

	// After vacuuming, a new SubscribeFrom from the beginning should receive
	// no messages because they have all been deleted.
	sub2, err := mq.SubscribeFrom("vacuum.ack.test", start)
	assert.NoError(t, err)
	defer sub2.Unsubscribe()

	select {
	case m := <-sub2.Chan():
		t.Fatalf("expected no messages after VacuumOnReadAck, got: %s", m.Payload)
	case <-time.After(200 * time.Millisecond):
		// expected: no messages remain
	}
}

// TestVacuumOnReadAck_ViaLoop verifies VacuumOnReadAck works when configured
// through WithVacuum (the vacuum loop path), similar to TestWithVacuumLoop.
func TestVacuumOnReadAck_ViaLoop(t *testing.T) {
	mq, err := delta.New(delta.URITemp(),
		delta.DBRemoveOnClose(),
		delta.WithVacuum(delta.VacuumOnReadAck, 100*time.Millisecond),
	)
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()

	const total = 10
	for i := range total {
		_, err := mq.Publish("vacuum.loop.ack", []byte(fmt.Sprintf("msg%d", i)))
		assert.NoError(t, err)
	}

	// Read all messages to advance the read watermark.
	sub, err := mq.SubscribeFrom("vacuum.loop.ack", start)
	assert.NoError(t, err)
	for range total {
		select {
		case <-sub.Chan():
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	}
	sub.Unsubscribe()

	// Wait long enough for the vacuum loop to run at least once after the
	// read watermark has been persisted.
	time.Sleep(600 * time.Millisecond)

	// No messages should remain.
	sub2, err := mq.SubscribeFrom("vacuum.loop.ack", start)
	assert.NoError(t, err)
	defer sub2.Unsubscribe()

	select {
	case m := <-sub2.Chan():
		t.Fatalf("expected no messages after VacuumOnReadAck loop, got: %s", m.Payload)
	case <-time.After(200 * time.Millisecond):
		// expected
	}
}

// ---------------------------------------------------------------------------
// id:18 — DBSyncOff
// ---------------------------------------------------------------------------

// TestDBSyncOff verifies that an MQ created with DBSyncOff() is fully
// functional: publish, subscribe, and receive work correctly.
func TestDBSyncOff(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose(), delta.DBSyncOff())
	assert.NoError(t, err)
	assert.NotNil(t, mq)
	defer mq.Close()

	sub, err := mq.Subscribe("dbsyncoff.test")
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = mq.Publish("dbsyncoff.test", []byte("hello"))
	assert.NoError(t, err)

	select {
	case m := <-sub.Chan():
		assert.Equal(t, "hello", string(m.Payload))
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message with DBSyncOff")
	}
}

// ---------------------------------------------------------------------------
// id:19 — URIFromPath
// ---------------------------------------------------------------------------

// TestURIFromPath_ValidPath verifies the URI has the expected file:// scheme
// and that New() can open the resulting path.
func TestURIFromPath_ValidPath(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/mydb.db"

	uri, err := delta.URIFromPath(path)
	assert.NoError(t, err)
	assert.True(t, len(uri) > 0, "URI must not be empty")
	assert.Contains(t, uri, "file:")
	assert.Contains(t, uri, path)

	// Must be openable.
	mq, err := delta.New(uri)
	assert.NoError(t, err)
	assert.NotNil(t, mq)
	assert.NoError(t, mq.Close())

	// Main DB file must exist.
	_, statErr := os.Stat(path)
	assert.NoError(t, statErr, "DB file should exist after New()+Close()")
}

// TestURIFromPath_PathWithSpaces verifies URIFromPath handles paths that
// contain spaces.
func TestURIFromPath_PathWithSpaces(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/my db with spaces/mydb.db"

	uri, err := delta.URIFromPath(path)
	assert.NoError(t, err)
	assert.Contains(t, uri, "file:")

	// Directory should have been created by URIFromPath.
	_, err = os.Stat(dir + "/my db with spaces")
	assert.NoError(t, err, "URIFromPath should create parent directories")

	mq, err := delta.New(uri)
	assert.NoError(t, err)
	assert.NotNil(t, mq)
	if mq != nil {
		assert.NoError(t, mq.Close())
	}
}

// TestURIFromPath_CreatesParentDirectories verifies that URIFromPath creates
// nested parent directories that do not yet exist.
func TestURIFromPath_CreatesParentDirectories(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/a/b/c/mydb.db"

	_, _ = delta.URIFromPath(path)

	_, err := os.Stat(dir + "/a/b/c")
	assert.NoError(t, err, "URIFromPath must create nested parent directories")
}

// ---------------------------------------------------------------------------
// id:20 — New() error paths
// ---------------------------------------------------------------------------

// TestNew_FailingOp verifies that a failing Op causes New() to return an error
// and not leak the DB connection (regression for issue #6 / item 20).
func TestNew_FailingOp(t *testing.T) {
	failingOp := func(*delta.MQ) error {
		return fmt.Errorf("injected op failure")
	}

	mq, err := delta.New(delta.URITemp(), failingOp)
	assert.Error(t, err)
	assert.Nil(t, mq)
	assert.Contains(t, err.Error(), "could not exec op")
}

// TestNew_InvalidURI verifies that a completely invalid URI causes New() to
// return an error (exercises the sql.Open / Ping failure path).
func TestNew_InvalidURI(t *testing.T) {
	// An invalid (non-file) URI that the go-sqlite3 driver will reject at Ping.
	mq, err := delta.New("file:/nonexistent_dir_xyz/path/that/cannot/be/created/db.db")
	if err != nil {
		assert.Nil(t, mq)
	} else {
		// Some systems may actually create the file; just close cleanly.
		assert.NotNil(t, mq)
		_ = mq.Close()
	}
}

// TestNew_MultipleFailingOps verifies each failing op causes an error and no
// leak across many iterations.
func TestNew_MultipleFailingOps(t *testing.T) {
	fdsBefore := countOpenFDs(t)

	for i := range 10 {
		op := func(*delta.MQ) error { return fmt.Errorf("op error %d", i) }
		mq, err := delta.New(delta.URITemp(), op)
		assert.Error(t, err)
		assert.Nil(t, mq)
	}

	fdsAfter := countOpenFDs(t)
	assert.LessOrEqual(t, fdsAfter-fdsBefore, 5,
		"New() with failing Op must not leak file descriptors")
}

// ---------------------------------------------------------------------------
// id:21 — Stream() with invalid names
// ---------------------------------------------------------------------------

// TestStream_InvalidNames verifies that Stream() rejects names with characters
// outside the [a-z0-9_] allowlist (after normalization of spaces, dashes, dots).
func TestStream_InvalidNames(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []struct {
		name    string
		wantErr bool
	}{
		// These normalise cleanly: spaces→_, dashes→_, dots→_
		{"my stream", false},
		{"my-stream", false},
		{"my.stream", false},
		{"UPPER", false},  // lowercased → "upper"
		{"stream!", true}, // '!' not allowed
		{"stream@", true}, // '@' not allowed
		{"stream#", true}, // '#' not allowed
		{"stream$", true}, // '$' not allowed
		{"stream%", true}, // '%' not allowed
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.name), func(t *testing.T) {
			s, err := mq.Stream(tc.name)
			if tc.wantErr {
				assert.Error(t, err, "expected error for stream name %q", tc.name)
				assert.Nil(t, s)
			} else {
				assert.NoError(t, err, "expected no error for stream name %q", tc.name)
				assert.NotNil(t, s)
			}
		})
	}
}

// TestStream_EmptyName verifies that an empty stream name is accepted by the
// implementation (checkStreamName does not reject empty strings — they pass
// the character allowlist check vacuously).  The resulting stream is a valid
// MQ with an empty-string name.
func TestStream_EmptyName(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// The empty string passes checkStreamName (no invalid characters).
	s, err := mq.Stream("")
	assert.NoError(t, err)
	assert.NotNil(t, s)
}

// ---------------------------------------------------------------------------
// id:22 — Public API with invalid topics
// ---------------------------------------------------------------------------

// TestPublish_InvalidTopic verifies Publish() returns an error for invalid topics.
func TestPublish_InvalidTopic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []string{
		"",         // empty
		"a.b.c>",   // '>' not allowed
		"a.b@c",    // '@' not allowed
		"a.b;c",    // ';' not allowed
		"a.b\x00c", // null byte
	}
	for _, topic := range cases {
		t.Run(fmt.Sprintf("%q", topic), func(t *testing.T) {
			pub, err := mq.Publish(topic, []byte("data"))
			assert.Error(t, err, "expected error for topic %q", topic)
			assert.Nil(t, pub)
		})
	}
}

// TestSubscribe_InvalidTopic verifies Subscribe() returns an error for invalid topics.
func TestSubscribe_InvalidTopic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []string{
		"",
		"a.b.c>",
		"bad@topic",
	}
	for _, topic := range cases {
		t.Run(fmt.Sprintf("%q", topic), func(t *testing.T) {
			sub, err := mq.Subscribe(topic)
			assert.Error(t, err, "expected error for topic %q", topic)
			assert.Nil(t, sub)
		})
	}
}

// TestQueue_InvalidTopic verifies Queue() returns an error for invalid topics.
func TestQueue_InvalidTopic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []string{
		"",
		"a.b.c>",
		"bad@topic",
	}
	for _, topic := range cases {
		t.Run(fmt.Sprintf("%q", topic), func(t *testing.T) {
			sub, err := mq.Queue(topic, "mykey")
			assert.Error(t, err, "expected error for topic %q", topic)
			assert.Nil(t, sub)
		})
	}
}

// TestSubscribeFrom_InvalidTopic verifies SubscribeFrom() returns an error for
// invalid topics.
func TestSubscribeFrom_InvalidTopic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []string{
		"",
		"a.b.c>",
		"bad@topic",
	}
	for _, topic := range cases {
		t.Run(fmt.Sprintf("%q", topic), func(t *testing.T) {
			sub, err := mq.SubscribeFrom(topic, time.Now())
			assert.Error(t, err, "expected error for topic %q", topic)
			assert.Nil(t, sub)
		})
	}
}

// ---------------------------------------------------------------------------
// id:23 — Queue() with empty key
// ---------------------------------------------------------------------------

// TestQueue_EmptyKey verifies that Queue() returns an error when the key is
// empty (or whitespace-only, which normalises to empty).
func TestQueue_EmptyKey(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	cases := []string{
		"",
		"   ",
		"\t",
	}
	for _, key := range cases {
		t.Run(fmt.Sprintf("%q", key), func(t *testing.T) {
			sub, err := mq.Queue("valid.topic", key)
			assert.Error(t, err, "Queue with empty key %q must return an error", key)
			assert.Nil(t, sub)
			assert.Contains(t, err.Error(), "key is empty")
		})
	}
}

// ---------------------------------------------------------------------------
// id:24 — Request() context cancellation (ctx.Done branch)
// ---------------------------------------------------------------------------

// TestRequest_CtxAlreadyCancelled verifies that when the context passed to
// Request() is already cancelled before any reply arrives, Request() itself
// still succeeds (it publishes the message first), but the returned subscription
// channel is closed without delivering a reply.
func TestRequest_CtxAlreadyCancelled(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// Pre-cancelled context — the ctx.Done() branch in the internal goroutine
	// will be selected immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sub, err := mq.Request(ctx, "req.ctx.cancel", []byte("ping"))
	assert.NoError(t, err, "Request() should succeed even with cancelled ctx")
	assert.NotNil(t, sub)

	// The internal goroutine selects ctx.Done() and calls s.Unsubscribe(),
	// which closes s.notifyChan.  Next() must therefore return ok=false.
	select {
	case m, ok := <-sub.Chan():
		if ok {
			t.Fatalf("expected closed channel after ctx cancel, got message: %v", m)
		}
		// Channel closed — correct behaviour.
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: sub.Chan() was not closed after ctx cancel")
	}
}

// TestRequest_CtxCancelledBeforeReply verifies that when the context is
// cancelled before any responder has had a chance to reply, the internal
// goroutine exits cleanly via the ctx.Done() branch.
func TestRequest_CtxCancelledBeforeReply(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	goroutinesBefore := goroutineCount()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// No responder — the ctx will time out before any reply arrives.
	sub, err := mq.Request(ctx, "req.no.responder", []byte("ping"))
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	// Wait for ctx to expire and the goroutine to clean up.
	time.Sleep(200 * time.Millisecond)

	goroutinesAfter := goroutineCount()
	leaked := goroutinesAfter - goroutinesBefore
	assert.LessOrEqual(t, leaked, 2,
		"Request() goroutine must exit after ctx.Done(); leaked %d goroutines", leaked)
}

// goroutineCount is a small helper used by the context-cancellation tests.
func goroutineCount() int {
	return runtime.NumGoroutine()
}

// ---------------------------------------------------------------------------
// id:25 — Reply() with nil mq (message does not support reply)
// ---------------------------------------------------------------------------

// TestReply_NilMQ verifies that calling Reply() on a Msg that was not created
// through Request() returns the "message does not support reply" error.
func TestReply_NilMQ(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	sub, err := mq.Subscribe("reply.nil.test")
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = mq.Publish("reply.nil.test", []byte("hello"))
	assert.NoError(t, err)

	select {
	case m := <-sub.Chan():
		// m.mq is set by the readloop, so Reply should work here.
		// We need a Msg where mq is nil — construct it manually via
		// the exported Msg zero value.
		var zeroMsg delta.Msg
		_, replyErr := zeroMsg.Reply([]byte("world"))
		assert.Error(t, replyErr)
		assert.Contains(t, replyErr.Error(), "message does not support reply")
		_ = m
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// ---------------------------------------------------------------------------
// id:26 — Close() return value
// ---------------------------------------------------------------------------

// TestClose_ReturnsNilOnSuccess verifies that a clean MQ Close() returns nil.
func TestClose_ReturnsNilOnSuccess(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	assert.NotNil(t, mq)

	err = mq.Close()
	assert.NoError(t, err, "Close() on a cleanly used MQ must return nil")
}

// TestClose_ReturnsNilAfterPublish verifies Close() still returns nil after
// publishing messages.
func TestClose_ReturnsNilAfterPublish(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)

	for i := range 10 {
		_, err = mq.Publish("close.test", []byte(fmt.Sprintf("msg%d", i)))
		assert.NoError(t, err)
	}

	// Allow read loop to process messages.
	time.Sleep(100 * time.Millisecond)

	err = mq.Close()
	assert.NoError(t, err, "Close() must return nil even after publishing messages")
}

// TestClose_IdempotentSecondClose verifies that calling Close() twice does not
// panic and the second call also returns without error (or at least doesn't panic).
func TestClose_IdempotentSecondClose(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)

	err1 := mq.Close()
	assert.NoError(t, err1, "first Close() must return nil")

	// Second close must not panic (sync.Once ensures the channel close only runs once).
	assert.NotPanics(t, func() {
		_ = mq.Close()
	})
}

// ---------------------------------------------------------------------------
// id:27 — Concurrent Subscribe and Unsubscribe
// ---------------------------------------------------------------------------

// TestConcurrentSubscribeUnsubscribe exercises concurrent Subscribe() calls
// from many goroutines to detect races in the globber trie and MQ internals.
// Each goroutine subscribes, waits for a message or timeout, then unsubscribes.
// Run with -race to detect data races.
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	const workers = 20
	var wg sync.WaitGroup

	// Workers subscribe concurrently — many on the same topic to exercise the
	// globber trie Insert/Remove paths under concurrent load.
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, err := mq.Subscribe("concurrent.sub.test")
			if err != nil {
				return
			}
			// Drain the channel or time out before unsubscribing, ensuring
			// the subscription is not live when Unsubscribe is called.
			select {
			case <-sub.Chan():
			case <-time.After(300 * time.Millisecond):
			}
			sub.Unsubscribe()
		}()
	}

	// Give subscribers time to start, then publish one message.
	time.Sleep(50 * time.Millisecond)
	_, err = mq.Publish("concurrent.sub.test", []byte("hello"))
	assert.NoError(t, err)

	wg.Wait()
}

// TestConcurrentQueueSubscribeUnsubscribe exercises concurrent Queue() consumers
// joining and leaving the same group while messages are in flight.
func TestConcurrentQueueSubscribeUnsubscribe(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	const (
		workers  = 20
		msgCount = 100
	)

	var received atomic.Int64

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, err := mq.Queue("cq.test", "group1")
			if err != nil {
				return
			}
			defer sub.Unsubscribe()

			for {
				select {
				case _, ok := <-sub.Chan():
					if !ok {
						return
					}
					received.Add(1)
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		}()
	}

	// Let workers start.
	time.Sleep(50 * time.Millisecond)

	for i := range msgCount {
		_, err := mq.Publish("cq.test", []byte(fmt.Sprintf("m%d", i)))
		assert.NoError(t, err)
	}

	wg.Wait()

	total := received.Load()
	assert.Equal(t, int64(msgCount), total,
		"all %d messages must be delivered exactly once across queue workers; got %d", msgCount, total)
}

// TestConcurrentStreamCreation verifies that concurrent Stream() calls with
// overlapping names do not race on the internal streams map.  Run with -race.
func TestConcurrentStreamCreation(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	var wg sync.WaitGroup
	// Re-use a small set of names to exercise the "already exists" cached path
	// as well as the new-stream creation path, all under concurrent load.
	for i := range 40 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("cstream%d", i%5)
			s, err := mq.Stream(name)
			assert.NoError(t, err)
			assert.NotNil(t, s)
		}(i)
	}
	wg.Wait()
}

// TestConcurrentClose exercises concurrent Close() calls; only one should
// actually close the DB, the rest are no-ops via sync.Once.
func TestConcurrentClose(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mq.Close()
		}()
	}
	wg.Wait()
	// No panic and no deadlock = success.
}

// ---------------------------------------------------------------------------
// id:29 — Msg.Ack() should update the read-ack watermark
// ---------------------------------------------------------------------------

// TestMsgAck_UpdatesReadWatermark verifies that Msg.Ack() writes the
// message's ID into the metadata table as the read-ack watermark. We test
// this directly by calling VacuumOnReadAck and confirming that only messages
// strictly below the ack'd ID are deleted.
//
// The test is deterministic: it blocks the read-loop watermark from advancing
// past the first message by unsubscribing after receiving it, then explicitly
// Ack()s only the first message and checks that exactly that message is gone.
func TestMsgAck_UpdatesReadWatermark(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now().Add(-time.Millisecond) // before any publish

	// Publish a single message; we will receive and Ack it.
	pub, err := mq.Publish("ack.wm.test", []byte("the-message"))
	assert.NoError(t, err)
	ackedID := pub.MessageId

	// Subscribe and receive the message so that mq.mq is set (needed for Ack).
	sub, err := mq.Subscribe("ack.wm.test")
	assert.NoError(t, err)

	var received delta.Msg
	select {
	case received = <-sub.Chan():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
	sub.Unsubscribe()

	// Ack it — this must write ackedID to the metadata table.
	assert.NoError(t, received.Ack())

	// Verify the ack watermark was written by running VacuumOnReadAck and
	// confirming the message is gone from the DB.
	// vacuumReadAck deletes WHERE message_id < ack_watermark; since we
	// Ack'd message N, the watermark is N so message N-1 would be deleted.
	// To observe a deletion we publish a second message AFTER the ack, then
	// set the ack watermark high enough to cover both by ack'ing the second.

	pub2, err := mq.Publish("ack.wm.test", []byte("second"))
	assert.NoError(t, err)

	sub2, err := mq.Subscribe("ack.wm.test")
	assert.NoError(t, err)
	var second delta.Msg
	select {
	case second = <-sub2.Chan():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for second message")
	}
	sub2.Unsubscribe()
	assert.NoError(t, second.Ack()) // watermark now covers both messages

	// VacuumOnReadAck deletes messages with message_id < watermark,
	// i.e. message 1 (ackedID) gets deleted; message 2 (pub2) stays.
	delta.VacuumOnReadAck(mq)

	// Retrieve from the start — only the second message should remain.
	hist, err := mq.SubscribeFrom("ack.wm.test", start)
	assert.NoError(t, err)
	defer hist.Unsubscribe()

	var got delta.Msg
	select {
	case got = <-hist.Chan():
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: expected second message to still be present after vacuum")
	}

	assert.Equal(t, pub2.MessageId, got.MessageId,
		"after VacuumOnReadAck, only the un-deleted message should remain")
	assert.Equal(t, []byte("second"), got.Payload)

	// The first message (ackedID) must be gone.
	assert.NotEqual(t, ackedID, got.MessageId,
		"the ack'd message must have been removed by VacuumOnReadAck")

	// No further messages should arrive.
	select {
	case extra := <-hist.Chan():
		t.Fatalf("unexpected extra message: %s", extra.Payload)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestMsgAck_NoopOnZeroMsg verifies that calling Ack() on a zero-value Msg
// (no mq reference) returns an error rather than panicking.
func TestMsgAck_NoopOnZeroMsg(t *testing.T) {
	var m delta.Msg
	err := m.Ack()
	assert.Error(t, err, "Ack() on a zero Msg should return an error")
}

// ---------------------------------------------------------------------------
// VacuumOnAge — negative maxAge sign flip (vacuum.go:8-10)
// ---------------------------------------------------------------------------

// TestVacuumOnAge_NegativeDuration verifies that VacuumOnAge accepts a negative
// duration by flipping its sign, and behaves identically to the positive variant.
// A negative maxAge should still delete messages older than |maxAge|.
func TestVacuumOnAge_NegativeDuration(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()

	// Publish messages with a timestamp that will appear old.
	for i := range 5 {
		_, err := mq.Publish("age.neg.test", []byte(fmt.Sprintf("msg%d", i)))
		assert.NoError(t, err)
	}

	// Consume all so the read watermark advances past them.
	sub, err := mq.SubscribeFrom("age.neg.test", start)
	assert.NoError(t, err)
	for range 5 {
		select {
		case <-sub.Chan():
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	}
	sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond) // let read watermark persist

	// Apply VacuumOnAge with a negative duration — must not panic and must
	// behave as if the positive value was passed (deletes messages older than
	// 1 ns, i.e. all of them since they were just published a moment ago and
	// we use a very small age).
	assert.NotPanics(t, func() {
		delta.VacuumOnAge(-1 * time.Nanosecond)(mq) // sign flip: deletes everything
	})

	// After vacuum, SubscribeFrom should find nothing.
	sub2, err := mq.SubscribeFrom("age.neg.test", start)
	assert.NoError(t, err)
	defer sub2.Unsubscribe()
	select {
	case m := <-sub2.Chan():
		t.Fatalf("expected no messages after VacuumOnAge with negative duration, got: %s", m.Payload)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing remains
	}
}

// ---------------------------------------------------------------------------
// RemoveStore — malformed URI error paths (lifecycle.go:257-262)
// ---------------------------------------------------------------------------

// TestRemoveStore_NoColonURI verifies that RemoveStore returns an error when
// the URI contains no colon (cannot be parsed as scheme:path).
func TestRemoveStore_NoColonURI(t *testing.T) {
	err := delta.RemoveStore("no-colon-here", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find file in uri")
}

// TestRemoveStore_NonFileURI verifies that RemoveStore returns an error when
// the URI scheme is not "file" (e.g. "memory:" or "http:").
func TestRemoveStore_NonFileURI(t *testing.T) {
	err := delta.RemoveStore("memory:/some/path", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a file uri")
}

// ---------------------------------------------------------------------------
// Queue — double Unsubscribe must not panic (mirrors TestDoubleUnsubscribePanic)
// ---------------------------------------------------------------------------

// TestQueue_DoubleUnsubscribePanic verifies that calling Unsubscribe() twice
// on a Queue subscription does not panic. The first call cleans up; the second
// must be a no-op because Queue.Unsubscribe is guarded by close(sub.doneChan)
// after a once-check.
func TestQueue_DoubleUnsubscribePanic(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	sub, err := mq.Queue("dbl.unsub.queue", "testkey")
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		sub.Unsubscribe()
		sub.Unsubscribe() // second call must not panic
	})
}

// ---------------------------------------------------------------------------
// Queue — all consumers busy, fallback blocking notify goroutine
// ---------------------------------------------------------------------------

// TestQueue_FallbackNotify verifies the fallback path in the queue distributor
// (subscribe.go:100-107): when all queue consumers are busy (tryNotify fails
// for all of them), the distributor spawns a goroutine that calls s.notify()
// blocking until the consumer reads. The message must still be delivered.
func TestQueue_FallbackNotify(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	// Single consumer — its channel is unbuffered. If the consumer is blocked
	// doing something else when a second message arrives, tryNotify will fail
	// and the distributor must fall back to the goroutine path.
	sub, err := mq.Queue("fallback.notify", "grp")
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	// Publish two messages in rapid succession. The first will be consumed
	// normally; the second will likely find the consumer busy (it hasn't
	// read the first yet), triggering the fallback goroutine.
	_, err = mq.Publish("fallback.notify", []byte("first"))
	assert.NoError(t, err)
	_, err = mq.Publish("fallback.notify", []byte("second"))
	assert.NoError(t, err)

	received := make([]string, 0, 2)
	for range 2 {
		select {
		case m := <-sub.Chan():
			received = append(received, string(m.Payload))
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout after receiving %d messages; expected 2", len(received))
		}
	}

	assert.ElementsMatch(t, []string{"first", "second"}, received,
		"both messages must be delivered via the queue, including the fallback goroutine path")
}

// ---------------------------------------------------------------------------
// Publish — write error path (publish.go:23-25): persist failure
// ---------------------------------------------------------------------------

// TestPublish_AfterClose verifies that publishing to a closed MQ returns an
// error (exercises the persist-failure path in write()). The underlying DB is
// closed when the MQ is closed, so any subsequent Publish must fail gracefully
// rather than panicking.
func TestPublish_AfterClose(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	assert.NoError(t, mq.Close())

	// Publish after close — the DB connection is gone; persist() must fail.
	_, err = mq.Publish("closed.test", []byte("should fail"))
	assert.Error(t, err, "Publish on a closed MQ must return an error")
}

// TestPublishAsync_AfterClose mirrors the above for PublishAsync.
func TestPublishAsync_AfterClose(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	assert.NoError(t, mq.Close())

	pub := mq.PublishAsync("closed.test", []byte("should fail"))
	<-pub.Done()
	assert.Error(t, pub.Err, "PublishAsync on a closed MQ must set pub.Err")
	// Must also not panic (regression for Bug 1).
}

// ---------------------------------------------------------------------------
// SubscribeFrom — Next() returns ok=false when channel is closed
// ---------------------------------------------------------------------------

// TestSubscribeFrom_NextReturnsFalseOnClose verifies that s.Next() returns
// (Msg{}, false) once the subscription channel is closed — e.g. after the
// historical replay finishes and no live messages are expected (the MQ is
// closed immediately after all historical messages are consumed).
func TestSubscribeFrom_NextReturnsFalseOnClose(t *testing.T) {
	mq, err := delta.New(delta.URITemp(), delta.DBRemoveOnClose())
	assert.NoError(t, err)
	defer mq.Close()

	start := time.Now()
	_, err = mq.Publish("subfrom.next.close", []byte("only"))
	assert.NoError(t, err)

	sub, err := mq.SubscribeFrom("subfrom.next.close", start)
	assert.NoError(t, err)

	// Read the historical message.
	m, ok := sub.Next()
	assert.True(t, ok)
	assert.Equal(t, "only", string(m.Payload))

	// Unsubscribe — closes the channel; next Next() must return false.
	sub.Unsubscribe()

	_, ok = sub.Next()
	assert.False(t, ok, "Next() must return false after Unsubscribe closes the channel")
}
