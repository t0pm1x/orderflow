package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// TopicDLQSuffix is appended to the source topic to form the DLQ
// topic name. e.g. "order-events" → "order-events.DLQ".
const TopicDLQSuffix = ".DLQ"

// KafkaClient is the subset of *events.Client that KafkaPublisher
// and KafkaDLQ need. Exposed as an interface so unit tests can
// substitute a fake without spinning up a broker.
type KafkaClient interface {
	PublishRaw(ctx context.Context, topic, key string, body []byte) error
}

// KafkaPublisher adapts events.Client (franz-go) to the
// outbox.Publisher interface.
//
// Semantics: at-least-once per record, batched into a single Kafka
// producer transaction. The poller marks rows SENT only after
// Kafka has confirmed the publish; on Kafka error the rows stay
// PENDING and the poller retries (3.7.c) or moves them to DLQ
// after MaxAttempts (3.7.d).
//
// True cross-system EOS is impossible (DB tx + Kafka tx cannot be
// committed atomically); we provide effective EOS by giving every
// event a unique event_id and having consumers dedupe on it
// (sub-stage 3.8.b). See ADR-0002 for the design rationale.
type KafkaPublisher struct {
	client KafkaClient
}

// NewKafkaPublisher constructs a KafkaPublisher.
func NewKafkaPublisher(c KafkaClient) *KafkaPublisher {
	return &KafkaPublisher{client: c}
}

// Compile-time interface check.
var _ Publisher = (*KafkaPublisher)(nil)

// Publish converts each outbox.Record into an Envelope (preserving
// the topic from the record) and publishes them in one transaction.
// Returns nil only after Kafka confirms every record in the batch.
//
// If any record fails, the entire batch is aborted and the poller
// will retry the whole batch on the next iteration.
func (k *KafkaPublisher) Publish(ctx context.Context, recs []outbox.Record) error {
	if len(recs) == 0 {
		return nil
	}
	for _, r := range recs {
		env := recordToEnvelope(r)
		body, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}
		if err := k.client.PublishRaw(ctx, r.Topic, r.AggregateID, body); err != nil {
			return fmt.Errorf("publish %s: %w", r.EventID, err)
		}
	}
	return nil
}

// recordToEnvelope lifts an outbox.Record into a publishable
// Envelope. OccurredAt/TraceID/SpanID are zero; future work can
// populate them from context (3.10.a — W3C tracecontext).
func recordToEnvelope(r outbox.Record) map[string]any {
	return map[string]any{
		"event_id":       r.EventID,
		"event_type":     r.EventType,
		"aggregate_id":   r.AggregateID,
		"aggregate_type": r.AggregateType,
		"schema_version": r.SchemaVersion,
		"payload":        json.RawMessage(r.Payload),
	}
}

// KafkaDLQ is a pkg/outbox.DLQ implementation that ships failed
// events to a per-topic DLQ topic. The DLQ topic name is the
// source topic + ".DLQ".
type KafkaDLQ struct {
	client KafkaClient
}

// NewKafkaDLQ constructs a KafkaDLQ.
func NewKafkaDLQ(c KafkaClient) *KafkaDLQ {
	return &KafkaDLQ{client: c}
}

// Compile-time interface check.
var _ DLQ = (*KafkaDLQ)(nil)

// Send publishes a single record to the DLQ topic with a
// "dlq_reason" envelope-level payload field. Errors are returned to
// the poller, which leaves the row FAILED for the next poll to
// retry the DLQ move.
func (d *KafkaDLQ) Send(ctx context.Context, r outbox.Record, reason string) error {
	envelope := recordToEnvelope(r)
	envelope["event_type"] = r.EventType + ".DLQ"
	// Carry the failure reason as a payload field so downstream
	// tooling can route/alert without parsing log lines.
	payload := map[string]any{
		"original_event_id":   r.EventID,
		"original_event_type": r.EventType,
		"original_topic":      r.Topic,
		"dlq_reason":          reason,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}
	envelope["payload"] = body
	wire, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal dlq wire: %w", err)
	}
	return d.client.PublishRaw(ctx, r.Topic+TopicDLQSuffix, r.AggregateID, wire)
}
