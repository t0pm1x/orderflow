package kafkatail_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/kafkatail"
)

// TestTail_PublishesEnvelope is a slow test (needs Kafka). Run only
// if KAFKA_TESTS=1 and KAFKA_BROKER points at a reachable broker.
func TestTail_PublishesEnvelope(t *testing.T) {
	if os.Getenv("KAFKA_TESTS") != "1" {
		t.Skip("set KAFKA_TESTS=1 to run (requires reachable Kafka)")
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		t.Fatal("KAFKA_BROKER required when KAFKA_TESTS=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	bus := events.NewBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe()
	defer unsub()

	stop, err := kafkatail.Start(ctx, slog.Default(), broker, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cli, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	env := pkgEvents.Envelope{
		EventID:       "tail-test-1",
		EventType:     "OrderCreated",
		AggregateID:   "order-1",
		AggregateType: "Order",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`{"x":1}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.ProduceSync(ctx, &kgo.Record{Topic: "order-events", Value: body}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-ch:
		if got.Envelope.EventType != "OrderCreated" {
			t.Errorf("event_type: got %s", got.Envelope.EventType)
		}
		if got.Envelope.EventID != "tail-test-1" {
			t.Errorf("event_id: got %s", got.Envelope.EventID)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no event in 30s")
	}
}