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
