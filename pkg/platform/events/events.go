// Package events provides event envelope types and Kafka client helpers.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Envelope is the standard event wrapper used across all orderflow topics.
// W3C trace context is propagated via TraceID/SpanID fields (not headers —
// Kafka headers are implementation-specific, this is in the body for portability).
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateID   string          `json:"aggregate_id"`
	AggregateType string          `json:"aggregate_type"`
	SchemaVersion string          `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	SpanID        string          `json:"span_id"`
	Payload       json.RawMessage `json:"payload"`
}

// NewEnvelope creates an envelope for an event.
func NewEnvelope(eventType, aggregateType, aggregateID string, payload any, traceID, spanID string) (*Envelope, error) {
	pb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		TraceID:       traceID,
		SpanID:        spanID,
		Payload:       pb,
	}, nil
}

// Client wraps a Kafka client with topic-specific helpers.
type Client struct {
	kgo *kgo.Client
}

// NewClient creates a Kafka client connected to the given brokers.
func NewClient(brokers []string, group string, opts ...kgo.Opt) (*Client, error) {
	opts = append(opts,
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
	)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Client{kgo: c}, nil
}

// Publish sends an envelope to the given topic.
func (c *Client) Publish(topic string, env *Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return c.PublishRaw(context.Background(), topic, env.AggregateID, body, nil)
}

// PublishRaw sends a pre-marshalled JSON body to topic with key,
// optionally attaching the given string-string headers as Kafka
// record headers. Used by the outbox poller (3.7.b) which has the
// wire format already from the outbox row.
func (c *Client) PublishRaw(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   body,
		Headers: headersToRecord(headers),
	}
	return c.kgo.ProduceSync(ctx, record).FirstErr()
}

// headersToRecord converts a string-string header map to the
// []kgo.RecordHeader shape franz-go expects on a Produce call.
func headersToRecord(headers map[string]string) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// Close shuts down the client.
func (c *Client) Close() {
	c.kgo.Close()
}
