// Package platform provides shared infrastructure for orderflow services:
// logging, OTel tracing, middleware, common types, events.
package platform

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger returns a JSON slog logger that writes to stderr.
// Levels: error/warn/info/debug via env var LOG_LEVEL (default info).
func NewLogger() *slog.Logger {
	level := slog.LevelInfo
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h)
}

// LogWithTrace returns a logger that includes trace_id and span_id
// from the active OpenTelemetry span in ctx (if any).
func LogWithTrace(ctx context.Context, base *slog.Logger) *slog.Logger {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	if !sc.HasTraceID() {
		return base
	}
	return base.With(
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	)
}
