package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// fakeKafka records every PublishRaw call so tests can assert on
// routing, key, and body shape.
type fakeKafka struct {
	mu       sync.Mutex
	calls    []fakeKafkaCall
	errByKey map[string]error
}

type fakeKafkaCall struct {
	topic   string
	key     string
	body    []byte
	headers map[string]string
}

func (f *fakeKafka) PublishRaw(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errByKey[key]; ok && err != nil {
		return err
	}
	f.calls = append(f.calls, fakeKafkaCall{topic: topic, key: key, body: append([]byte(nil), body...), headers: headers})
	return nil
}

func TestKafkaPublisher_PublishesEachRecord(t *testing.T) {
	fk := &fakeKafka{}
	kp := NewKafkaPublisher(fk)
	recs := []outbox.Record{
		{EventID: "e1", EventType: "OrderCreated", AggregateID: "o1", AggregateType: "Order", SchemaVersion: "1.0", Topic: "order-events", Payload: []byte(`{"x":1}`)},
		{EventID: "e2", EventType: "PaymentCompleted", AggregateID: "p1", AggregateType: "Payment", SchemaVersion: "1.0", Topic: "payment-events", Payload: []byte(`{"y":2}`)},
	}
	if err := kp.Publish(context.Background(), recs); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(fk.calls) != 2 {
		t.Fatalf("calls: got %d want 2", len(fk.calls))
	}
	if fk.calls[0].topic != "order-events" || fk.calls[0].key != "o1" {
		t.Errorf("call[0]: got topic=%q key=%q", fk.calls[0].topic, fk.calls[0].key)
	}
	if fk.calls[1].topic != "payment-events" || fk.calls[1].key != "p1" {
		t.Errorf("call[1]: got topic=%q key=%q", fk.calls[1].topic, fk.calls[1].key)
	}
	// Verify body is JSON with envelope fields.
	var env map[string]any
	if err := json.Unmarshal(fk.calls[0].body, &env); err != nil {
		t.Fatalf("body[0] not JSON: %v", err)
	}
	if env["event_id"] != "e1" || env["event_type"] != "OrderCreated" {
		t.Errorf("envelope[0]: %v", env)
	}
}

func TestKafkaPublisher_PropagatesError(t *testing.T) {
	fk := &fakeKafka{errByKey: map[string]error{"p1": errors.New("kafka down")}}
	kp := NewKafkaPublisher(fk)
	err := kp.Publish(context.Background(), []outbox.Record{
		{EventID: "e1", EventType: "PaymentCompleted", AggregateID: "p1", AggregateType: "Payment", Topic: "payment-events"},
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

func TestKafkaDLQ_SendsToDLQTopic(t *testing.T) {
	fk := &fakeKafka{}
	dlq := NewKafkaDLQ(fk)
	rec := outbox.Record{
		EventID: "e1", EventType: "OrderCreated", AggregateID: "o1",
		AggregateType: "Order", SchemaVersion: "1.0",
		Topic: "order-events", Payload: []byte(`{"x":1}`),
	}
	if err := dlq.Send(context.Background(), rec, "kafka 5xx for 5 polls"); err != nil {
		t.Fatalf("dlq send: %v", err)
	}
	if len(fk.calls) != 1 {
		t.Fatalf("calls: got %d want 1", len(fk.calls))
	}
	if fk.calls[0].topic != "order-events.DLQ" {
		t.Errorf("topic: got %q want order-events.DLQ", fk.calls[0].topic)
	}
	var env map[string]any
	if err := json.Unmarshal(fk.calls[0].body, &env); err != nil {
		t.Fatalf("dlq body not JSON: %v", err)
	}
	if env["event_type"] != "OrderCreated.DLQ" {
		t.Errorf("event_type: got %v want OrderCreated.DLQ", env["event_type"])
	}
}

// withTestTracer installs an in-memory SDK TracerProvider + the
// W3C TraceContext propagator for the duration of a test, and
// restores whatever the caller had set up on cleanup. Returns the
// recorder so callers can inspect emitted spans.
func withTestTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

// TestRecordToEnvelope_PropagatesActiveSpan asserts that an active
// OTel span on ctx flows into the emitted Envelope's TraceID/SpanID
// (sub-stage 3.10.b).
func TestRecordToEnvelope_PropagatesActiveSpan(t *testing.T) {
	withTestTracer(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	env, err := recordToEnvelope(ctx, outbox.Record{})
	if err != nil {
		t.Fatalf("recordToEnvelope: %v", err)
	}
	sc := span.SpanContext()
	if env.TraceID != sc.TraceID().String() {
		t.Errorf("trace_id mismatch: got %q want %q", env.TraceID, sc.TraceID().String())
	}
	if env.SpanID != sc.SpanID().String() {
		t.Errorf("span_id mismatch: got %q want %q", env.SpanID, sc.SpanID().String())
	}
	// Sanity: the JSON wire format must emit 32-hex trace_id and
	// 16-hex span_id (32+16=48 hex chars total in the body).
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundTripped events.Envelope
	if err := json.Unmarshal(body, &roundTripped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(roundTripped.TraceID) != 32 {
		t.Errorf("trace_id length: got %d want 32", len(roundTripped.TraceID))
	}
	if len(roundTripped.SpanID) != 16 {
		t.Errorf("span_id length: got %d want 16", len(roundTripped.SpanID))
	}
}

// TestRecordToEnvelope_NoSpan_LeavesIDsEmpty ensures the function
// is a no-op on the trace fields when no span is active (e.g. a
// tests that doesn't wire OpenTelemetry).
func TestRecordToEnvelope_NoSpan_LeavesIDsEmpty(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	env, err := recordToEnvelope(context.Background(), outbox.Record{})
	if err != nil {
		t.Fatalf("recordToEnvelope: %v", err)
	}
	if env.TraceID != "" || env.SpanID != "" {
		t.Errorf("expected empty trace_id/span_id, got %q/%q", env.TraceID, env.SpanID)
	}
}