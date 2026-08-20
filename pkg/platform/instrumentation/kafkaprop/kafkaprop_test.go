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

// TestRecordHeaderCarrier_GetSet verifies the kafkaprop.RecordHeaderCarrier
// implements TextMapCarrier so Inject/Extract can be called with a
// Kafka-header-shaped value. OBS-5 wires this into both
// pkg/platform/events PublishRaw and pkg/consumer dispatch; without
// the carrier, the production header round-trip would not exist.
func TestRecordHeaderCarrier_GetSet(t *testing.T) {
	c := make(RecordHeaderCarrier)
	c.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	if got := c.Get("traceparent"); got != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("Get after Set: got %q want traceparent header value", got)
	}
	if len(c) != 1 {
		t.Errorf("len after Set: got %d want 1", len(c))
	}
	c.Set("tracestate", "vendor=value")
	if len(c) != 2 {
		t.Errorf("len after second Set: got %d want 2", len(c))
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get on missing key: got %q want empty", got)
	}
}

// TestInjectExtract_RoundTripViaRecordHeaderCarrier asserts that
// the W3C traceparent header round-trips through the carrier used
// by the OBS-5 wiring (Kafka record headers in
// []kgo.RecordHeader shape). The carrier is the bridge between
// the OTel propagator's TextMapCarrier interface and the
// franz-go RecordHeader type — without it, kafkaprop.Inject cannot
// reach the kgo.Record.Headers slice.
func TestInjectExtract_RoundTripViaRecordHeaderCarrier(t *testing.T) {
	if os.Getenv("SKIP_OTEL_TESTS") != "" {
		t.Skip("SKIP_OTEL_TESTS set")
	}

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceID, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	spanID, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: false,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	carrier := make(RecordHeaderCarrier)
	Inject(ctx, carrier)
	if len(carrier) == 0 {
		t.Fatal("Inject produced no headers")
	}

	extracted := Extract(context.Background(), carrier)
	got := trace.SpanContextFromContext(extracted)
	if !got.IsValid() {
		t.Fatal("expected valid SpanContext after Extract")
	}
	if got.TraceID() != traceID {
		t.Errorf("TraceID mismatch: got %s, want %s", got.TraceID(), traceID)
	}
}
