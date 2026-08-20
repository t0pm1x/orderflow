// Package kafkaprop bridges Kafka record headers with OpenTelemetry
// trace context. It exposes Inject / Extract for W3C traceparent
// propagation and SpanFromEnvelope for reconstructing a SpanContext
// from a deserialized orderflow event envelope.
package kafkaprop

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// RecordHeaderCarrier is the in-memory shape the OBS-5 producer
// and consumer share. It is a map[string]string — the same shape
// OTel's propagation.MapCarrier uses — so the standard
// TextMapCarrier interface (Get/Set/Keys) works without value
// vs pointer receiver gymnastics. The wire-format []kgo.RecordHeader
// slice is a different type (Kafka allows duplicate keys) so the
// producer-side events.PublishRaw builds the carrier from the
// outbox's record.Headers map, calls Inject, then flattens the
// carrier back to []kgo.RecordHeader. The consumer side does the
// inverse: build the carrier from rec.Headers, call Extract.
//
// Multiple headers with the same Kafka key collapse to last-wins
// inside the carrier — matches OTel's TextMapCarrier contract for
// HTTP and gRPC headers (the same single-value-per-key model) and
// is sufficient for the three W3C trace keys (traceparent,
// tracestate, baggage) that OBS-5 propagates.
type RecordHeaderCarrier map[string]string

// Get returns the value for the supplied key. Returns "" when the
// key is absent; matches propagation.TextMapCarrier contract.
func (c RecordHeaderCarrier) Get(key string) string {
	return c[key]
}

// Set writes the value for the supplied key, overwriting any
// previous value. Maps are reference types in Go so the mutation
// is visible to the caller through the value receiver.
func (c RecordHeaderCarrier) Set(key, value string) {
	c[key] = value
}

// Keys returns every key in the carrier in arbitrary order.
func (c RecordHeaderCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

// Inject writes W3C traceparent into the supplied header carrier.
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// Extract reads traceparent from the carrier into ctx.
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// SpanFromEnvelope returns the span for a deserialized Envelope
// (creates a child if trace_id present).
//
// OBS-5: prefer an existing valid SpanContext on ctx (e.g. one
// restored by kafkaprop.Extract from the Kafka record headers) so
// the W3C traceparent wins over the legacy envelope-body IDs when
// both are present. The envelope IDs remain the authoritative
// fallback for legacy producers that haven't been recompiled with
// the new wire format — SpanFromEnvelope is the single recovery
// path for both transports.
func SpanFromEnvelope(ctx context.Context, traceID, spanID, name string) (context.Context, trace.Span) {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		// Header extract already populated a valid context; the
		// consumer span is a child of the producer's remote span.
		return otel.Tracer("orderflow").Start(ctx, name)
	}
	tid, _ := trace.TraceIDFromHex(traceID)
	sid, _ := trace.SpanIDFromHex(spanID)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	if !sc.IsValid() {
		return otel.Tracer("orderflow").Start(ctx, name)
	}
	return otel.Tracer("orderflow").Start(
		trace.ContextWithSpanContext(ctx, sc),
		name,
	)
}
