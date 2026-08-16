package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

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
	topic string
	key   string
	body  []byte
}

func (f *fakeKafka) PublishRaw(ctx context.Context, topic, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errByKey[key]; ok && err != nil {
		return err
	}
	f.calls = append(f.calls, fakeKafkaCall{topic: topic, key: key, body: append([]byte(nil), body...)})
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
