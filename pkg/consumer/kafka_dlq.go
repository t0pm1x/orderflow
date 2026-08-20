package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/t0pm1x/orderflow/platform/events"
)

// KafkaDLQ ships poison-pill events to a per-topic DLQ topic. It
// implements DLQ via the same kgo.Client the consumer uses (the
// franz-go client is safe for concurrent produce/consume).
type KafkaDLQ struct {
	client *kgo.Client
	suffix string
}

// NewKafkaDLQ constructs a DLQ that writes to <topic><suffix>.
// Default suffix is ".DLQ" (mirrors pkg/outbox.TopicDLQSuffix).
func NewKafkaDLQ(client *kgo.Client, suffix string) *KafkaDLQ {
	if suffix == "" {
		suffix = ".DLQ"
	}
	return &KafkaDLQ{client: client, suffix: suffix}
}

// Send publishes the failed envelope to the DLQ topic with a
// dlq_reason header for downstream tooling. Returns the first
// franz-go error from the synchronous produce.
//
// sourceTopic is the Kafka topic the consumer received the record
// from; the DLQ topic is `<sourceTopic><suffix>` (e.g. "order-events"
// → "order-events.DLQ"). Pre-fix the consumer derived source from
// envelope fields and misrouted every DLQ event to `events.DLQ`
// (audit CONSUMER-1).
func (d *KafkaDLQ) Send(ctx context.Context, env *events.Envelope, sourceTopic, reason string) error {
	if env == nil {
		return fmt.Errorf("dlq: nil envelope")
	}
	if sourceTopic == "" {
		return fmt.Errorf("dlq: empty source topic")
	}
	envelope := *env
	envelope.EventType += ".DLQ"
	// Reuse the payload field to carry failure metadata so the DLQ
	// consumer can route without parsing log lines.
	payload := map[string]any{
		"original_event_id":   envelope.EventID,
		"original_event_type": envelope.EventType,
		"original_topic":      sourceTopic,
		"dlq_reason":          reason,
		"dlq_ts":              time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("dlq: marshal payload: %w", err)
	}
	envelope.Payload = body
	wire, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("dlq: marshal envelope: %w", err)
	}
	topic := sourceTopic + d.suffix
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(envelope.AggregateID),
		Value: wire,
	}
	return d.client.ProduceSync(ctx, rec).FirstErr()
}

// Compile-time interface check.
var _ DLQ = (*KafkaDLQ)(nil)
