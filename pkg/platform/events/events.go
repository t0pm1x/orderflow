// Package events provides event envelope types and Kafka client helpers.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"

	"github.com/t0pm1x/orderflow/kafkaprop"
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

// Publish sends an envelope to the given topic. ctx is used by the
// underlying Kafka producer — a cancelled ctx aborts the produce
// so a graceful-shutdown deadline can release the goroutine.
// Previously this ignored ctx and used context.Background(), which
// meant a Kafka stall could keep the producer (and thus the
// service) alive past the SIGTERM grace period.
func (c *Client) Publish(ctx context.Context, topic string, env *Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return c.PublishRaw(ctx, topic, env.AggregateID, body, nil)
}

// PublishRaw sends a pre-marshalled JSON body to topic with key,
// optionally attaching the given string-string headers as Kafka
// record headers. Used by the outbox poller (3.7.b) which has the
// wire format already from the outbox row.
//
// OBS-5: the active OTel span on ctx is injected into the record
// headers as a W3C traceparent (and tracestate, baggage). This is
// the cross-process trace propagation ADR-0004 mandates; without
// it, every consuming service starts a fresh trace root and the
// Tempo service map breaks across Kafka topic boundaries. The
// caller's `headers` map is preserved verbatim — any business
// headers the producer wants to attach (e.g. saga-id) ride along
// with the trace headers.
func (c *Client) PublishRaw(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
	carrier := make(kafkaprop.RecordHeaderCarrier, len(headers)+2)
	for k, v := range headers {
		carrier[k] = v
	}
	// Inject mutates the carrier in-place. The propagator reads
	// ctx for the active span; on a no-active-span ctx the
	// propagator is a no-op so the carrier's existing keys (the
	// business headers) survive untouched.
	kafkaprop.Inject(ctx, propagation.TextMapCarrier(carrier))

	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   body,
		Headers: carrierToKgo(carrier),
	}
	return c.kgo.ProduceSync(ctx, record).FirstErr()
}

// carrierToKgo flattens a kafkaprop.RecordHeaderCarrier (a
// map[string]string populated by kafkaprop.Inject on the publish
// path) back to the []kgo.RecordHeader slice franz-go expects on
// a Produce call. The carrier carries both the producer's
// business headers and the W3C trace headers.
func carrierToKgo(carrier kafkaprop.RecordHeaderCarrier) []kgo.RecordHeader {
	if len(carrier) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(carrier))
	for k, v := range carrier {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// Close shuts down the client.
func (c *Client) Close() {
	c.kgo.Close()
}

// PublishBatch ships one or more records in a single franz-go
// ProduceSync round-trip. Used by the outbox poller's fast path
// (OBX-005): with BatchSize=100 the previous serial implementation
// performed 100 sequential blocking round-trips inside the open
// DB transaction; this collapses them into one.
//
// Each record's headers carry the active span's W3C traceparent
// (mirrors PublishRaw's behavior). On any error, franz-go returns
// a ProduceResults value with per-record errors; we surface the
// first one as the error.
//
// This satisfies pkg/outbox.KafkaBatchClient; the outbox publisher
// type-asserts to that interface and falls back to PublishRaw when
// the client does not expose it (tests using a fake).
func (c *Client) PublishBatch(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	results := c.kgo.ProduceSync(ctx, recs...)
	return results.FirstErr()
}

// Ping verifies at least one Kafka broker is reachable by issuing a
// broker-only Metadata request. It is the OBS-1 readiness probe
// primitive: a /readyz handler invokes Ping with a short ctx so a
// dead broker returns within the readiness timeout rather than
// blocking the kubelet's HTTP request. Errors are surfaced verbatim
// so the handler can include them in the JSON failure response.
func (c *Client) Ping(ctx context.Context) error {
	return c.kgo.Ping(ctx)
}
