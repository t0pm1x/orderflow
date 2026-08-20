// Package platform provides shared infrastructure for orderflow services:
// logging, OTel tracing, middleware, common types, events.
package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger returns a JSON slog logger that writes to stderr.
// Levels: error/warn/info/debug via env var LOG_LEVEL (default info).
//
// v1.2 (SEC-12 fix): the JSON handler is wrapped in piiHandler so
// attribute values for known PII keys (last_four, card_number,
// customer_id, idempotency_key, password) are replaced with
// "[REDACTED]" before they reach the underlying writer. The wrapper
// is transparent for non-PII keys, so callers do not need to
// change. To opt out (e.g. in a test that needs the raw value),
// construct the handler directly with slog.NewJSONHandler.
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
	return slog.New(piiHandler{inner: h})
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

// Redact returns a stable, opaque fingerprint of s suitable for log
// lines that must not leak the original value (URLs with credentials,
// session tokens, etc.). The output is the first 8 hex chars of
// SHA-256(s) — short enough to grep, long enough to avoid trivial
// collisions on a small fleet.
//
// Pre-v1.2 the implementation returned the first 6 + last 4 chars of
// the raw string. For URLs that leaked scheme/host/port (e.g.
// "postgres://orderflow:secret@db.example.com:5432/..." →
// "postgr…:5432"); for short values it returned the literal "***",
// which collided for every input under 12 chars (audit SEC-11).
//
// The empty-string sentinel ("<unset>") is preserved so a missing
// DATABASE_URL stays distinguishable from a populated-but-redacted
// one in log greps.
func Redact(s string) string {
	if s == "" {
		return "<unset>"
	}
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])[:8]
}

// piiKeys are the attribute names whose values are masked when the
// PII redaction filter is active. Match is case-insensitive. Add new
// keys here when the audit or a code-review surfaces a new PII field
// in a log line.
//
// Note: "key" is intentionally included. The payment idempotency
// middleware emits `slog.Default().Error("idempotency: handler panic",
// "key", key, ...)` — the attribute is named "key" (short for
// "idempotency key"), not "idempotency_key". Without this entry the
// panic path would leak the key verbatim, defeating the SEC-12 fix.
var piiKeys = map[string]struct{}{
	"last_four":       {},
	"card_number":     {},
	"customer_id":     {},
	"idempotency_key": {},
	"key":             {},
	"password":        {},
}

// piiHandler is a slog.Handler wrapper that masks PII attribute
// values before they reach the underlying JSON handler. The masked
// value is "[REDACTED]" — same shape as the webhook library's
// secret-redaction pattern. The original key is preserved so
// downstream tooling can still group by it.
//
// This addresses audit SEC-12: LastFour / customer_id flowing into
// text logs. JSON-encoded logs (the production default since OBS-6)
// are easier to filter, but the redaction is defense in depth.
type piiHandler struct{ inner slog.Handler }

func (h piiHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.inner.Enabled(ctx, l) }
func (h piiHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if _, ok := piiKeys[a.Key]; ok {
			nr.AddAttrs(slog.String(a.Key, "[REDACTED]"))
		} else {
			nr.AddAttrs(a)
		}
		return true
	})
	return h.inner.Handle(ctx, nr)
}
func (h piiHandler) WithAttrs(as []slog.Attr) slog.Handler {
	masked := make([]slog.Attr, 0, len(as))
	for _, a := range as {
		if _, ok := piiKeys[a.Key]; ok {
			masked = append(masked, slog.String(a.Key, "[REDACTED]"))
		} else {
			masked = append(masked, a)
		}
	}
	return piiHandler{inner: h.inner.WithAttrs(masked)}
}
func (h piiHandler) WithGroup(name string) slog.Handler { return piiHandler{inner: h.inner.WithGroup(name)} }

// NewRedactingLogger wraps the JSON handler returned by NewLogger
// with the piiHandler redaction filter. Use it as a drop-in
// replacement for NewLogger at startup when PII fields are
// expected in attribute values.
func NewRedactingLogger() *slog.Logger {
	return slog.New(piiHandler{inner: NewLogger().Handler()})
}
