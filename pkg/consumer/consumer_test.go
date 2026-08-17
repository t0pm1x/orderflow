package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/t0pm1x/orderflow/platform/events"
)

// TestInMemoryDeduper_SeenAndMark: dedup behavior contract.
func TestInMemoryDeduper_SeenAndMark(t *testing.T) {
	d := NewInMemoryDeduper()
	ctx := context.Background()
	if seen, _ := d.Seen(ctx, "e1"); seen {
		t.Fatal("empty deduper must report unseen")
	}
	if err := d.Mark(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	if seen, _ := d.Seen(ctx, "e1"); !seen {
		t.Fatal("after Mark, Seen must report true")
	}
}

func TestNoopDeduper_AlwaysFalse(t *testing.T) {
	d := NoopDeduper{}
	ctx := context.Background()
	if seen, _ := d.Seen(ctx, "e1"); seen {
		t.Fatal("noop must always report unseen")
	}
	if err := d.Mark(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
}

func orderCreatedRecord() *kgo.Record {
	return &kgo.Record{
		Key:   []byte("o1"),
		Value: []byte(`{"event_id":"e1","event_type":"OrderCreated","aggregate_id":"o1","aggregate_type":"Order","schema_version":"1.0","payload":{}}`),
	}
}

// TestDispatch_DedupSkipsHandler: 3.8.b core contract — replay
// must not invoke the handler.
func TestDispatch_DedupSkipsHandler(t *testing.T) {
	c := &Consumer{
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				t.Error("handler must not run for duplicate event_id")
				return nil
			},
		},
		deduper: NewInMemoryDeduper(),
	}
	_ = c.deduper.Mark(context.Background(), "e1")
	c.dispatch(context.Background(), orderCreatedRecord())
}

func TestDispatch_UnknownEventTypeSkipped(t *testing.T) {
	rec := &kgo.Record{
		Key:   []byte("o1"),
		Value: []byte(`{"event_id":"e1","event_type":"NeverHeardOfIt","aggregate_id":"o1","aggregate_type":"Order","schema_version":"1.0","payload":{}}`),
	}
	c := &Consumer{registry: HandlerRegistry{}}
	c.dispatch(context.Background(), rec)
}

func TestDispatch_HandlerErrorRetriesThenDLQs(t *testing.T) {
	var calls atomic.Int32
	dlq := &fakeDLQ{}
	c := &Consumer{
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				calls.Add(1)
				return errors.New("handler boom")
			},
		},
		dlq:          dlq,
		maxAttempts:  3,
		retryBackoff: time.Millisecond,
	}
	c.dispatch(context.Background(), orderCreatedRecord())
	if got := calls.Load(); got != 3 {
		t.Errorf("handler invocations: got %d want 3", got)
	}
	if len(dlq.sent) != 1 {
		t.Errorf("dlq sends: got %d want 1", len(dlq.sent))
	}
}

func TestDispatch_HandlerSucceedsMarksDeduper(t *testing.T) {
	calls := 0
	d := NewInMemoryDeduper()
	c := &Consumer{
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				calls++
				return nil
			},
		},
		deduper: d,
	}
	c.dispatch(context.Background(), orderCreatedRecord())
	if calls != 1 {
		t.Errorf("handler invocations: got %d want 1", calls)
	}
	if seen, _ := d.Seen(context.Background(), "e1"); !seen {
		t.Error("deduper must mark event_id after success")
	}
}

func TestDispatch_RetryThenSuccessDoesNotDLQ(t *testing.T) {
	var calls atomic.Int32
	c := &Consumer{
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				if calls.Add(1) < 2 {
					return errors.New("transient")
				}
				return nil
			},
		},
		maxAttempts:  3,
		retryBackoff: time.Millisecond,
	}
	c.dispatch(context.Background(), orderCreatedRecord())
	if got := calls.Load(); got != 2 {
		t.Errorf("handler invocations: got %d want 2", got)
	}
}

// TestDispatch_DecodeErrorDLQs: a malformed record goes to DLQ
// even if no handler is registered (so an operator can see it).
func TestDispatch_DecodeErrorDLQs(t *testing.T) {
	dlq := &fakeDLQ{}
	c := &Consumer{
		registry: HandlerRegistry{},
		dlq:      dlq,
	}
	rec := &kgo.Record{Key: []byte("o1"), Value: []byte(`not-json`)}
	c.dispatch(context.Background(), rec)
	if len(dlq.sent) != 1 {
		t.Errorf("decode errors must DLQ; got %d", len(dlq.sent))
	}
}

// fakeDLQ is a minimal DLQ impl for tests.
type fakeDLQ struct {
	mu   sync.Mutex
	sent []string
}

func (d *fakeDLQ) Send(_ context.Context, _ *events.Envelope, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sent = append(d.sent, reason)
	return nil
}

// mustEncode JSON-encodes an envelope for tests; panics on error.
func mustEncode(env events.Envelope) []byte {
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return b
}

// withTestTracer installs an SDK TracerProvider for the duration
// of a test (3.10.c mirrors the 3.10.b pattern from outbox).
func withTestTracer(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})
}

// TestDispatch_RestoresSpanFromEnvelope asserts the consumer
// restores the W3C trace context from the envelope and links the
// handler's span to the producer's trace (sub-stage 3.10.c).
func TestDispatch_RestoresSpanFromEnvelope(t *testing.T) {
	withTestTracer(t)

	ctx, origSpan := otel.Tracer("test").Start(context.Background(), "producer.op")
	defer origSpan.End()

	env := events.Envelope{
		EventID: "id-1", EventType: "OrderCreated",
		AggregateID: "agg-1", AggregateType: "Order",
		TraceID: origSpan.SpanContext().TraceID().String(),
		SpanID:  origSpan.SpanContext().SpanID().String(),
		SchemaVersion: "1.0", Payload: json.RawMessage(`{}`),
	}
	var seenParent trace.SpanContext
	reg := HandlerRegistry{"OrderCreated": func(ctx context.Context, _ *events.Envelope) error {
		seenParent = trace.SpanFromContext(ctx).SpanContext()
		return nil
	}}
	c := &Consumer{registry: reg, maxAttempts: 1}
	rec := &kgo.Record{Value: mustEncode(env), Key: []byte("agg-1")}
	c.dispatch(ctx, rec)
	if seenParent.TraceID() != origSpan.SpanContext().TraceID() {
		t.Fatalf("trace_id lost across consumer boundary")
	}
}

// suppress unused import warnings for json if we trim tests.
var _ = json.RawMessage{}
