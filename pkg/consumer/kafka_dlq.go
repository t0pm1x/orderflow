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
func (d *KafkaDLQ) Send(ctx context.Context, env *events.Envelope, reason string) error {
	if env == nil {
		return fmt.Errorf("dlq: nil envelope")
	}
	envelope := *env
	envelope.EventType += ".DLQ"
	// Reuse the payload field to carry failure metadata so the DLQ
	// consumer can route without parsing log lines.
	payload := map[string]any{
		"original_event_id":   envelope.EventID,
		"original_event_type": envelope.EventType,
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
	topic := sourceTopicFromRecord(envelope.AggregateID) + d.suffix
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(envelope.AggregateID),
		Value: wire,
	}
	return d.client.ProduceSync(ctx, rec).FirstErr()
}

// sourceTopicFromRecord is a placeholder for routing the DLQ event
// back to its source topic. The franz-go Record itself carries the
// Topic field but the consumer doesn't keep a reference to the
// record in DLQ.Send — for now we encode the source topic via the
// AggregateID prefix (set by the consumer in the dispatch path).
// Future work (3.8.c follow-up) should plumb the record through.
func sourceTopicFromRecord(aggregateID string) string {
	// Convention: if aggregateID contains a "/", the segment
	// before it is the topic. Otherwise default to "events".
	for i := 0; i < len(aggregateID); i++ {
		if aggregateID[i] == '/' {
			return aggregateID[:i]
		}
	}
	return "events"
}

// Compile-time interface check.
var _ DLQ = (*KafkaDLQ)(nil)
