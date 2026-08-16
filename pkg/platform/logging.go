// Package platform provides shared infrastructure for orderflow services:
// logging, OTel tracing, middleware, common types, events.
package platform

import (
	"context"
	"log/slog"
	"os"
)

// NewLogger returns a JSON slog logger that writes to stderr.
// Uses trace_id/span_id from context if available.
func NewLogger() *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h)
}

// LogWithTrace returns a logger that includes trace_id from ctx.
func LogWithTrace(ctx context.Context, base *slog.Logger) *slog.Logger {
	return base // stub — will be wired to OTel in 3.3.b
}
