package delta

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// capturingHandler is a slog.Handler that records all log messages for inspection.
type capturingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *capturingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *capturingHandler) captured() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.msgs))
	copy(out, h.msgs)
	return out
}

// TestIterMessage_ErrorPrefix_NoCoveReference is a regression test for issue 16:
// error and log messages in iterMessage (db.go) used to reference "[cove]" instead
// of "[delta]". This test verifies that when iterMessage encounters a query error,
// the logged message uses the "[delta]" prefix, not "[cove]".
func TestIterMessage_ErrorPrefix_NoCoveReference(t *testing.T) {
	h := &capturingHandler{}
	log := slog.New(h)

	// Open and immediately close a DB so that any query on it returns an error.
	db, err := sql.Open("sqlite3_delta-v", ":memory:")
	assert.NoError(t, err)
	db.Close() // closed DB causes Query to fail

	tbl := func() string { return "_mq_delta_stream_default" }

	// Consume the iterator; internally iterMessage will fail to query and log an error.
	itr := iterMessage(db, "test.topic", time.Time{}, 0, tbl, log)
	for range itr {
		// intentionally empty – we only care about the side-effect log message
	}

	msgs := h.captured()
	assert.NotEmpty(t, msgs, "expected at least one log message from iterMessage on query error")

	for _, msg := range msgs {
		assert.True(t, strings.HasPrefix(msg, "[delta]"),
			"log message should start with [delta], got: %q", msg)
		assert.NotContains(t, msg, "[cove]",
			"log message must not reference [cove] (stale project name), got: %q", msg)
		assert.NotContains(t, msg, "cove:",
			"log message must not reference 'cove:' (stale project name), got: %q", msg)
	}
}

// TestIterMessage_NilLog_StderrFallback exercises the nil-log code path in
// iterMessage: when log == nil and the query fails, the error is printed to
// stderr instead of being logged. This covers db.go:212-213.
func TestIterMessage_NilLog_StderrFallback(t *testing.T) {
	db, err := sql.Open("sqlite3_delta-v", ":memory:")
	assert.NoError(t, err)
	db.Close() // closed DB forces the query to fail

	tbl := func() string { return "_mq_delta_stream_default" }

	// Pass nil logger — exercises the fmt.Fprintf(os.Stderr, ...) branch.
	itr := iterMessage(db, "test.topic", time.Time{}, 0, tbl, nil)
	count := 0
	for range itr {
		count++
	}
	// The iterator must yield nothing and must not panic.
	assert.Equal(t, 0, count)
}

// TestIterMessage_GlobNilLog exercises the nil-log path for glob queries.
func TestIterMessage_GlobNilLog_StderrFallback(t *testing.T) {
	db, err := sql.Open("sqlite3_delta-v", ":memory:")
	assert.NoError(t, err)
	db.Close()

	tbl := func() string { return "_mq_delta_stream_default" }

	itr := iterMessage(db, "test.*", time.Time{}, 0, tbl, nil)
	count := 0
	for range itr {
		count++
	}
	assert.Equal(t, 0, count)
}

// TestIterMessage_EarlyTermination exercises the `!yield(m)` early-exit path
// in iterMessage (db.go:233-235): when the caller breaks out of the range loop
// after the first message, yield returns false and the iterator must stop.
func TestIterMessage_EarlyTermination(t *testing.T) {
	// Build a real in-memory DB with messages so the iterator actually yields.
	db, err := sql.Open("sqlite3_delta-v", ":memory:")
	assert.NoError(t, err)
	defer db.Close()

	tbl := func() string { return "msgs" }

	// Create the table and insert three messages.
	_, err = db.Exec(`CREATE TABLE msgs (
		message_id BIGINT PRIMARY KEY,
		topic TEXT,
		payload BLOB,
		created_at BIGINT
	)`)
	assert.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err = db.Exec(`INSERT INTO msgs VALUES (?, 'a.b', ?, ?)`,
			i, []byte("payload"), time.Now().UnixNano())
		assert.NoError(t, err)
	}

	itr := iterMessage(db, "a.b", time.Time{}, 10, tbl, slog.New(&capturingHandler{}))
	count := 0
	for m := range itr {
		count++
		_ = m
		break // yield returns false after this — exercises the early-exit branch
	}
	assert.Equal(t, 1, count, "iterator must stop after caller breaks")
}

// openFreshDB opens an in-memory SQLite DB, creates the delta stream schema,
// and inserts n messages. Returns the db and the table-name func.
func openFreshDB(t *testing.T, n int) (*sql.DB, func() string) {
	t.Helper()
	db, err := sql.Open("sqlite3_delta-v", ":memory:")
	assert.NoError(t, err)

	tbl := func() string { return "_mq_delta_stream_default" }

	_, err = db.Exec(`CREATE TABLE _mq_delta_stream_default (
		message_id BIGINT PRIMARY KEY,
		topic TEXT,
		payload BLOB,
		created_at BIGINT
	)`)
	assert.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE _mq_delta_metadata (
		key TEXT PRIMARY KEY,
		value TEXT,
		created_at BIGINT
	)`)
	assert.NoError(t, err)

	// Seed a read watermark well above all messages so vacuum can delete.
	_, err = db.Exec(`INSERT INTO _mq_delta_metadata (key, value, created_at)
		VALUES ('_mq_delta_stream_default_read', '9999', 0)`)
	assert.NoError(t, err)

	for i := 1; i <= n; i++ {
		_, err = db.Exec(
			`INSERT INTO _mq_delta_stream_default VALUES (?, 'a.b', ?, ?)`,
			i, []byte("p"), time.Now().Add(-2*time.Hour).UnixNano(),
		)
		assert.NoError(t, err)
	}
	return db, tbl
}

// TestVacuumReadAck_DBError exercises the error return path of vacuumReadAck
// (db.go:111-113) by closing the DB before calling it.
func TestVacuumReadAck_DBError(t *testing.T) {
	db, tbl := openFreshDB(t, 5)
	db.Close() // force all subsequent Exec calls to fail

	_, err := vacuumReadAck(db, tbl)
	assert.Error(t, err, "vacuumReadAck must return an error when the DB is closed")
}

// TestVacuumBefore_DBError exercises the error return path of vacuumBefore
// (db.go:131-133).
func TestVacuumBefore_DBError(t *testing.T) {
	db, tbl := openFreshDB(t, 5)
	db.Close()

	_, err := vacuumBefore(db, time.Now(), tbl)
	assert.Error(t, err, "vacuumBefore must return an error when the DB is closed")
}

// TestVacuumKeep_DBError exercises the error return path of vacuumKeep
// (db.go:155-157).
func TestVacuumKeep_DBError(t *testing.T) {
	db, tbl := openFreshDB(t, 5)
	db.Close()

	_, err := vacuumKeep(db, 2, tbl)
	assert.Error(t, err, "vacuumKeep must return an error when the DB is closed")
}

// TestMetrics_DBError exercises the sql.ErrNoRows and generic error paths of
// metrics() (db.go:83-86). A closed DB causes QueryRow.Scan to fail.
func TestMetrics_DBError(t *testing.T) {
	db, tbl := openFreshDB(t, 0)
	db.Close()

	_, _, err := metrics(db, tbl)
	assert.Error(t, err, "metrics must return an error when the DB is closed")
}

// TestVacuumKeepN_LogsError verifies that VacuumKeepN logs an error (and
// returns early) when the underlying DB operation fails — exercises
// vacuum.go:35-38 (the err != nil + return branch inside VacuumKeepN).
func TestVacuumKeepN_LogsError(t *testing.T) {
	h := &capturingHandler{}
	log := slog.New(h)

	mq, err := New(URITemp(), DBRemoveOnClose(), WithLogger(log))
	assert.NoError(t, err)
	defer mq.Close()

	// Close the underlying DB so the vacuum SQL will fail.
	mq.base.db.Close()

	// Must not panic; the function must log the error and return.
	assert.NotPanics(t, func() {
		VacuumKeepN(10)(mq)
	})

	msgs := h.captured()
	found := false
	for _, m := range msgs {
		if strings.Contains(m, "vacuuming error") {
			found = true
			break
		}
	}
	assert.True(t, found, "VacuumKeepN must log a vacuuming error when the DB is closed")
}

// TestVacuumOnAge_LogsError verifies VacuumOnAge handles DB errors without
// panicking and logs them — exercises vacuum.go:19-21.
func TestVacuumOnAge_LogsError(t *testing.T) {
	h := &capturingHandler{}
	log := slog.New(h)

	mq, err := New(URITemp(), DBRemoveOnClose(), WithLogger(log))
	assert.NoError(t, err)
	defer mq.Close()

	mq.base.db.Close()

	assert.NotPanics(t, func() {
		VacuumOnAge(time.Second)(mq)
	})

	msgs := h.captured()
	found := false
	for _, m := range msgs {
		if strings.Contains(m, "vacuuming error") {
			found = true
			break
		}
	}
	assert.True(t, found, "VacuumOnAge must log a vacuuming error when the DB is closed")
}

// TestVacuumOnReadAck_LogsError verifies VacuumOnReadAck handles DB errors
// without panicking — exercises vacuum.go:52-55.
func TestVacuumOnReadAck_LogsError(t *testing.T) {
	h := &capturingHandler{}
	log := slog.New(h)

	mq, err := New(URITemp(), DBRemoveOnClose(), WithLogger(log))
	assert.NoError(t, err)
	defer mq.Close()

	mq.base.db.Close()

	assert.NotPanics(t, func() {
		VacuumOnReadAck(mq)
	})

	msgs := h.captured()
	found := false
	for _, m := range msgs {
		if strings.Contains(m, "vacuuming error") {
			found = true
			break
		}
	}
	assert.True(t, found, "VacuumOnReadAck must log a vacuuming error when the DB is closed")
}
