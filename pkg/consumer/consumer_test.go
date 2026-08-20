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
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/t0pm1x/orderflow/platform/events"
)

// fakeClient satisfies the consumerClient interface so unit tests
// can exercise dispatch without a real broker.
type fakeClient struct {
	marked    []*kgo.Record
	committed atomic.Int32
	mu        sync.Mutex
}

func (f *fakeClient) MarkCommitRecords(recs ...*kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, recs...)
}

func (f *fakeClient) CommitMarkedOffsets(_ context.Context) error {
	f.committed.Add(1)
	return nil
}

func (f *fakeClient) Close() {}

func (f *fakeClient) PollFetches(_ context.Context) kgo.Fetches { return kgo.Fetches{} }

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

func orderCreatedRecord() *kgo.Record {
	return &kgo.Record{
		Key:   []byte("o1"),
		Value: []byte(`{"event_id":"e1","event_type":"OrderCreated","aggregate_id":"o1","aggregate_type":"Order","schema_version":"1.0","payload":{}}`),
	}
}

// TestDispatch_MarksRecordForCommit_OnSuccess: the consumer must
// MarkCommitRecords for each record whose handler returns nil. Without
// this, franz-go's CommitMarkedOffsets() is a no-op and offsets never
// advance — every restart would replay the whole topic.
func TestDispatch_MarksRecordForCommit_OnSuccess(t *testing.T) {
	fc := &fakeClient{}
	c := &Consumer{
		client: fc,
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error { return nil },
		},
		maxAttempts: 1,
	}
	c.dispatch(context.Background(), orderCreatedRecord())
	if len(fc.marked) != 1 {
		t.Fatalf("dispatch must mark record on success; got %d marks", len(fc.marked))
	}
}

// TestDispatch_MarksRecordForCommit_AfterDLQ: a record that exhausts
// retries and lands in DLQ must still be marked, otherwise the poison
// pill reappears after the next restart and the consumer is stuck.
func TestDispatch_MarksRecordForCommit_AfterDLQ(t *testing.T) {
	fc := &fakeClient{}
	dlq := &fakeDLQ{}
	c := &Consumer{
		client: fc,
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				return errors.New("boom")
			},
		},
		dlq:          dlq,
		maxAttempts:  2,
		retryBackoff: time.Millisecond,
	}
	c.dispatch(context.Background(), orderCreatedRecord())
	if len(fc.marked) != 1 {
		t.Fatalf("dispatch must mark record after DLQ; got %d marks", len(fc.marked))
	}
	if len(dlq.sent) != 1 {
		t.Fatalf("DLQ must receive the poison pill; got %d", len(dlq.sent))
	}
}

// TestDispatch_MarksRecordForCommit_AfterRetryThenSuccess: a record
// that fails once then succeeds must still be marked exactly once.
func TestDispatch_MarksRecordForCommit_AfterRetryThenSuccess(t *testing.T) {
	fc := &fakeClient{}
	var calls atomic.Int32
	c := &Consumer{
		client: fc,
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
	if len(fc.marked) != 1 {
		t.Errorf("must mark exactly once; got %d", len(fc.marked))
	}
}

// TestDispatch_RecoversHandlerPanic: a handler that panics must
// not kill the consumer goroutine — the dispatch recovers, logs,
// and marks the record for commit. Without the recover, the
// panic would propagate out of dispatch into the Run loop and
// (depending on Go runtime behavior) kill the goroutine.
func TestDispatch_RecoversHandlerPanic(t *testing.T) {
	fc := &fakeClient{}
	c := &Consumer{
		client: fc,
		registry: HandlerRegistry{
			"OrderCreated": func(_ context.Context, _ *events.Envelope) error {
				panic("handler bug")
			},
		},
		maxAttempts:  1,
		retryBackoff: time.Millisecond,
	}
	// Should NOT panic out of dispatch.
	c.dispatch(context.Background(), orderCreatedRecord())
	if len(fc.marked) != 1 {
		t.Errorf("panic'd record must be marked for commit; got %d marks", len(fc.marked))
	}
}

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

// TestDispatch_UnknownEventTypeStillMarksForCommit pins the
// v1.1.3 fix: when a record carries an event_type no service
// handles yet (forward-compatible producer), the consumer MUST
// mark it for commit. With DisableAutoCommit and no mark,
// franz-go would re-fetch the same unknown record on every poll,
// holding the partition hostage — every other event behind it
// waits indefinitely. This regression net fails if dispatch
// early-returns without calling markRecord(rec).
func TestDispatch_UnknownEventTypeStillMarksForCommit(t *testing.T) {
	fc := &fakeClient{}
	rec := &kgo.Record{
		Key:   []byte("o1"),
		Value: []byte(`{"event_id":"e1","event_type":"NeverHeardOfIt","aggregate_id":"o1","aggregate_type":"Order","schema_version":"1.0","payload":{}}`),
	}
	c := &Consumer{client: fc, registry: HandlerRegistry{}}
	c.dispatch(context.Background(), rec)
	if len(fc.marked) != 1 {
		t.Fatalf("unknown event_type must still mark record for commit (DisableAutoCommit); got %d marks", len(fc.marked))
	}
}

// TestDispatch_DecodeErrorMarksRecord decodes that fail unmarshal
// must also mark the record for commit AND send to DLQ. Without
// the mark, the malformed bytes would re-poll forever (the fake
// DLQ alone is not enough to advance the offset).
func TestDispatch_DecodeErrorMarksRecord(t *testing.T) {
	fc := &fakeClient{}
	dlq := &fakeDLQ{}
	c := &Consumer{
		client:   fc,
		registry: HandlerRegistry{},
		dlq:      dlq,
	}
	rec := &kgo.Record{Key: []byte("o1"), Value: []byte(`not-json`)}
	c.dispatch(context.Background(), rec)
	if len(dlq.sent) != 1 {
		t.Fatalf("decode errors must DLQ; got %d", len(dlq.sent))
	}
	if len(fc.marked) != 1 {
		t.Fatalf("decode errors must mark record for commit; got %d marks", len(fc.marked))
	}
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
		TraceID:       origSpan.SpanContext().TraceID().String(),
		SpanID:        origSpan.SpanContext().SpanID().String(),
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

// TestDispatch_ExtractsTraceparentFromHeaders is the OBS-5 consumer
// regression net: a Kafka record with a W3C traceparent header must
// restore the parent span context from the header (ADR-0004), not
// from the envelope body's TraceID/SpanID fields. Pre-fix the
// consumer only read the envelope IDs, so a producer that built the
// trace header via events.Client.PublishRaw would lose the trace
// across topic boundaries. The test asserts the handler sees the
// traceparent's trace_id (not the envelope's) when both are set.
func TestDispatch_ExtractsTraceparentFromHeaders(t *testing.T) {
	prevProp := otel.GetTextMapPropagator()
	prevTP := otel.GetTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prevProp)
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	// Build a header-carried traceparent that points at a
	// different trace than the envelope body claims. The handler
	// must see the header's trace, not the body's — the OBS-5
	// fix reads headers BEFORE reading the envelope.
	headerTraceID := "0af7651916cd43dd8448eb211c80319c"
	headerSpanID := "b7ad6b7169203331"
	traceparent := "00-" + headerTraceID + "-" + headerSpanID + "-01"

	envelopeTraceID := "11111111111111111111111111111111"
	envelopeSpanID := "2222222222222222"

	env := events.Envelope{
		EventID: "id-1", EventType: "OrderCreated",
		AggregateID: "agg-1", AggregateType: "Order",
		TraceID:       envelopeTraceID,
		SpanID:        envelopeSpanID,
		SchemaVersion: "1.0", Payload: json.RawMessage(`{}`),
	}

	var seenParent trace.SpanContext
	reg := HandlerRegistry{"OrderCreated": func(ctx context.Context, _ *events.Envelope) error {
		seenParent = trace.SpanFromContext(ctx).SpanContext()
		return nil
	}}
	c := &Consumer{registry: reg, maxAttempts: 1}
	rec := &kgo.Record{
		Value:   mustEncode(env),
		Key:     []byte("agg-1"),
		Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte(traceparent)}},
	}
	c.dispatch(context.Background(), rec)

	if !seenParent.IsValid() {
		t.Fatal("expected valid SpanContext after dispatch")
	}
	if got, want := seenParent.TraceID().String(), headerTraceID; got != want {
		t.Errorf("TraceID: got %s want %s (the OBS-5 header-based trace, not the envelope body)", got, want)
	}
	// The handler's seenParent.SpanID is the consumer-span's own ID
	// (a new child span under the header's parent). The header's
	// SpanID is the producer's span and lives on the parent; we
	// verify it indirectly via the TraceID match above. A stricter
	// check (SeenParentIsRemote parent of the handler's span) is
	// possible but requires walking the recording SDK's span tree.
}

// TestDispatch_EnvelopeFallbackWhenHeaderAbsent pins the fallback
// path: a record with no traceparent header still gets a span
// context from the envelope body (the pre-OBS-5 behavior). Without
// this fallback a legacy producer that hasn't been recompiled with
// the new wire format would silently lose all traces.
func TestDispatch_EnvelopeFallbackWhenHeaderAbsent(t *testing.T) {
	withTestTracer(t)

	_, origSpan := otel.Tracer("test").Start(context.Background(), "producer.op")
	defer origSpan.End()

	env := events.Envelope{
		EventID: "id-1", EventType: "OrderCreated",
		AggregateID: "agg-1", AggregateType: "Order",
		TraceID:       origSpan.SpanContext().TraceID().String(),
		SpanID:        origSpan.SpanContext().SpanID().String(),
		SchemaVersion: "1.0", Payload: json.RawMessage(`{}`),
	}
	var seenParent trace.SpanContext
	reg := HandlerRegistry{"OrderCreated": func(ctx context.Context, _ *events.Envelope) error {
		seenParent = trace.SpanFromContext(ctx).SpanContext()
		return nil
	}}
	c := &Consumer{registry: reg, maxAttempts: 1}
	rec := &kgo.Record{Value: mustEncode(env), Key: []byte("agg-1")}
	c.dispatch(context.Background(), rec)

	if seenParent.TraceID() != origSpan.SpanContext().TraceID() {
		t.Fatalf("envelope-fallback trace_id lost; got %s want %s",
			seenParent.TraceID(), origSpan.SpanContext().TraceID())
	}
}

// suppress unused import warnings for json if we trim tests.
var _ = json.RawMessage{}
