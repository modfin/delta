package delta

import (
	"context"
	"log/slog"
)

type discardLogger struct{}

func (d discardLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (d discardLogger) Handle(ctx context.Context, record slog.Record) error {
	return nil
}

func (d discardLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	return d
}

func (d discardLogger) WithGroup(name string) slog.Handler {
	return d
}
