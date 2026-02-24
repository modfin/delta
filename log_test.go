package delta

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiscardLoggerEnabled(t *testing.T) {
	d := discardLogger{}
	// Enabled() must return false so callers short-circuit log argument
	// formatting and avoid wasted CPU when the discard logger is active.
	assert.False(t, d.Enabled(context.Background(), slog.LevelDebug))
	assert.False(t, d.Enabled(context.Background(), slog.LevelInfo))
	assert.False(t, d.Enabled(context.Background(), slog.LevelWarn))
	assert.False(t, d.Enabled(context.Background(), slog.LevelError))
}

func TestDiscardLoggerHandle(t *testing.T) {
	d := discardLogger{}
	err := d.Handle(context.Background(), slog.Record{})
	assert.NoError(t, err)
}

func TestDiscardLoggerWithAttrs(t *testing.T) {
	d := discardLogger{}
	h := d.WithAttrs([]slog.Attr{slog.String("key", "val")})
	assert.Equal(t, d, h)
}

func TestDiscardLoggerWithGroup(t *testing.T) {
	d := discardLogger{}
	h := d.WithGroup("grp")
	assert.Equal(t, d, h)
}
