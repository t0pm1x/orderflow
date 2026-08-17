package kafkaprop

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtract_RoundTrip(t *testing.T) {
	if os.Getenv("SKIP_OTEL_TESTS") != "" {
		t.Skip("SKIP_OTEL_TESTS set")
	}

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceID, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	spanID, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	carrier := propagation.MapCarrier{}
	Inject(ctx, carrier)

	if carrier["traceparent"] == "" {
		t.Fatal("expected traceparent in carrier after Inject")
	}

	got := Extract(context.Background(), carrier)
	gsc := trace.SpanContextFromContext(got)
	if !gsc.IsValid() {
		t.Fatal("expected valid SpanContext from Extract")
	}
	if gsc.TraceID() != traceID {
		t.Errorf("TraceID mismatch: got %s, want %s", gsc.TraceID(), traceID)
	}
}

func TestSpanFromEnvelope_ValidTraceID(t *testing.T) {
	if os.Getenv("SKIP_OTEL_TESTS") != "" {
		t.Skip("SKIP_OTEL_TESTS set")
	}

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceID := "0af7651916cd43dd8448eb211c80319c"
	spanID := "b7ad6b7169203331"

	ctx, span := SpanFromEnvelope(context.Background(), traceID, spanID, "test-span")
	defer span.End()

	gsc := trace.SpanContextFromContext(ctx)
	if !gsc.IsValid() {
		t.Fatal("expected valid SpanContext from SpanFromEnvelope")
	}
	if gsc.TraceID().String() != traceID {
		t.Errorf("TraceID mismatch: got %s, want %s", gsc.TraceID(), traceID)
	}
}

func TestSpanFromEnvelope_InvalidTraceID(t *testing.T) {
	if os.Getenv("SKIP_OTEL_TESTS") != "" {
		t.Skip("SKIP_OTEL_TESTS set")
	}

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	ctx, span := SpanFromEnvelope(context.Background(), "not-hex", "not-hex", "test-span")
	defer span.End()

	if span == nil {
		t.Fatal("expected a span (even if non-recording) when traceID is invalid")
	}
	if ctx == context.Background() {
		t.Error("expected new context wrapping the returned span")
	}
}
