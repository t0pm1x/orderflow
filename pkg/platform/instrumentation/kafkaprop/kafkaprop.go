package kafkaprop

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Inject writes W3C traceparent into the supplied header carrier.
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// Extract reads traceparent from the carrier into ctx.
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// SpanFromEnvelope returns the span for a deserialized Envelope (creates a child if trace_id present).
func SpanFromEnvelope(ctx context.Context, traceID, spanID, name string) (context.Context, trace.Span) {
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
