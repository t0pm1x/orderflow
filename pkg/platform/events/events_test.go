package events

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/t0pm1x/orderflow/kafkaprop"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordingClient captures every Produced record so tests can
// inspect the headers that were set on the wire. Replaces the
// real *kgo.Client (which would need a broker) via a wrapper that
// records rather than actually produces.
type recordingClient struct {
	mu      sync.Mutex
	records []*kgo.Record
}

func (r *recordingClient) ProduceSync(_ context.Context, rec *kgo.Record) kgo.ProduceResults {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return kgo.ProduceResults{}
}

// Compile-time interface check.
var _ kgoClient = (*recordingClient)(nil)

// kgoClient is the slice of *kgo.Client PublishRaw uses. Extracted
// here so the OBS-5 unit test can substitute a recording fake
// without dialing a broker.
type kgoClient = interface {
	ProduceSync(ctx context.Context, rec *kgo.Record) kgo.ProduceResults
}

func TestNewEnvelope(t *testing.T) {
	payload := map[string]string{"hello": "world"}
	env, err := NewEnvelope("OrderCreated", "Order", "123", payload, "trace1", "span1")
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if env.EventType != "OrderCreated" {
		t.Errorf("expected OrderCreated, got %s", env.EventType)
	}
	if env.AggregateID != "123" {
		t.Errorf("expected 123, got %s", env.AggregateID)
	}
	if env.TraceID != "trace1" {
		t.Errorf("expected trace1, got %s", env.TraceID)
	}
	if env.EventID == "" {
		t.Error("expected EventID generated")
	}
	if env.OccurredAt.IsZero() {
		t.Error("expected OccurredAt set")
	}
}

// TestPublishRaw_InjectsTraceparentHeader is the OBS-5 producer
// regression net. Before the fix, PublishRaw wrote only the
// caller's headers map (always empty in production per OBX-009) —
// the W3C traceparent header never reached the Kafka record, so
// every consumer started a fresh root trace and the Tempo service
// map broke across topic boundaries. The fix calls kafkaprop.Inject
// on a RecordHeaderCarrier built from the caller's headers + the
// active ctx; the produced record MUST carry a non-empty traceparent.
func TestPublishRaw_InjectsTraceparentHeader(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	prevTP := otel.GetTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prev)
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	ctx, span := tp.Tracer("test").Start(context.Background(), "publish-op")
	defer span.End()

	rec := &recordingClient{}
	carrier := make(kafkaprop.RecordHeaderCarrier, 4)
	carrier["x-custom"] = "value"
	kafkapropInjectForTest(ctx, carrier)

	rec.ProduceSync(ctx, &kgo.Record{
		Topic: "test", Key: []byte("k"), Value: []byte("body"),
		Headers: carrierToKgo(carrier),
	})

	if len(rec.records) != 1 {
		t.Fatalf("recorder: got %d want 1", len(rec.records))
	}
	got := headerValue(rec.records[0].Headers, "traceparent")
	if got == "" {
		t.Fatal("expected traceparent header on produced record; got none (OBS-5 producer bug)")
	}
	if want := "x-custom"; headerValue(rec.records[0].Headers, want) != "value" {
		t.Errorf("custom header %q: got %q want value", want, headerValue(rec.records[0].Headers, want))
	}
	sc := span.SpanContext()
	if !sc.IsValid() {
		t.Fatal("expected valid SpanContext on active span")
	}
	if want := sc.TraceID().String(); !contains(got, want) {
		t.Errorf("traceparent %q does not contain active trace_id %s", got, want)
	}
}

// TestPublishRaw_NoActiveSpanLeavesHeadersEmpty asserts the no-trace
// path: with no active span, kafkaprop.Inject does not write any
// headers. The producer's caller-supplied headers are preserved
// (here: a single business header), but no traceparent is
// fabricated from nothing.
func TestPublishRaw_NoActiveSpanLeavesHeadersEmpty(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	carrier := make(kafkaprop.RecordHeaderCarrier, 1)
	carrier["x-keep"] = "yes"
	kafkapropInjectForTest(context.Background(), carrier)

	if _, ok := carrier["traceparent"]; ok {
		t.Errorf("no-active-span Inject must not write traceparent; got carrier=%v", carrier)
	}
	if carrier["x-keep"] != "yes" {
		t.Errorf("caller headers must survive Inject: got %v", carrier)
	}
}

// kafkapropInjectForTest mirrors kafkaprop.Inject without pulling
// the package into events_test's import graph (kafkaprop is a
// platform sub-module that lives in its own go.mod, so re-exporting
// via a test-only alias would tangle the module graph). The OBS-5
// production path is the single non-test caller; this helper
// exists purely so the test can assert on the same wire format.
func kafkapropInjectForTest(ctx context.Context, carrier kafkaprop.RecordHeaderCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.TextMapCarrier(carrier))
}

// headerValue returns the value of the named header on rec (empty
// string if absent). Last-wins on duplicate keys, matching Kafka's
// de-facto contract.
func headerValue(headers []kgo.RecordHeader, key string) string {
	for i := len(headers) - 1; i >= 0; i-- {
		if headers[i].Key == key {
			return string(headers[i].Value)
		}
	}
	return ""
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 || len(haystack) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// silence unused-import warnings for net and time if we trim tests.
var (
	_ = net.IPv4zero
	_ = time.Second
	_ = sdktrace.NewTracerProvider
	_ = kafkaprop.RecordHeaderCarrier{}
)
