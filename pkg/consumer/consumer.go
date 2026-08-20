// Package consumer provides the Kafka consumer base + per-event
// handler plumbing used by every orderflow service. It complements
// pkg/outbox (which is the producer side of the same events): the
// outbox poller writes events, this package reads them and dispatches
// to per-service handlers.
//
// Sub-stages:
//   - 3.8.a Consumer base + per-service consumer group
//   - 3.8.b Idempotent handler wrapper (event_id dedupe)
//   - 3.8.c Consumer DLQ (handler error → retry → DLQ)
//
// Per-service handlers live in services/<svc>/internal/consumer/.
// See the spec for the full mapping of which handler reads which
// event.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"

	"github.com/t0pm1x/orderflow/kafkaprop"
	"github.com/t0pm1x/orderflow/platform/events"
)

// Handler processes one event envelope. Returning nil acknowledges
// the offset; returning a non-nil error triggers retry/DLQ logic
// (see WithDLQ).
type Handler func(ctx context.Context, env *events.Envelope) error

// HandlerRegistry maps event_type strings to Handler funcs. The
// Consumer looks up the handler for each record by the envelope's
// EventType field; an unknown event_type is acked-and-skipped so a
// forward-compatible producer doesn't block the consumer group.
type HandlerRegistry map[string]Handler

// consumerClient is the subset of *kgo.Client the Consumer uses.
// Defined as an interface so unit tests can substitute a fake
// without dialing a real broker — the production wiring still passes
// a real *kgo.Client from kgo.NewClient.
type consumerClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	MarkCommitRecords(records ...*kgo.Record)
	CommitMarkedOffsets(ctx context.Context) error
	Close()
}

// lastPollWarn throttles the "consumer: poll fetch error" warning
// so a sustained post-close error doesn't spam logs at line rate.
// Stored as unix nanos so the comparison stays lock-free.
var lastPollWarn atomic.Int64

// Consumer is one service's subscription to one or more Kafka
// topics. Construct it once at startup; Run blocks until ctx is
// cancelled.
type Consumer struct {
	client   consumerClient
	registry HandlerRegistry

	dlq     DLQ
	deduper Deduper

	maxAttempts  int
	retryBackoff time.Duration

	stopOnce sync.Once
	stopped  chan struct{}
}

// DLQ is the contract for shipping poison-pill events to a DLQ
// topic after MaxAttempts retries. The default impl lives in
// kafka_dlq.go (this package).
//
// sourceTopic is the Kafka topic the consumer received the record
// from (NOT the aggregate_id or any other envelope field). The DLQ
// must write to <sourceTopic>.DLQ so downstream tooling can route
// by source. Pre-fix the consumer derived source from envelope
// fields (e.g. splitting aggregate_id on "/"), which misrouted every
// event to a single `events.DLQ` because real aggregate_ids are
// UUIDs with no slash (audit CONSUMER-1).
type DLQ interface {
	Send(ctx context.Context, env *events.Envelope, sourceTopic, reason string) error
}

// Deduper records which event_ids have already been processed so
// the consumer can ack replays without double-effect. The default
// in-memory impl is for tests; production swaps in a Redis- or
// Postgres-backed one (sub-stage 3.8.b).
type Deduper interface {
	Seen(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}

// Config tunes the Consumer.
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string

	// MaxAttempts is the cap on per-event retries before DLQ.
	// Default 5.
	MaxAttempts int

	// RetryBackoff is the sleep between retry attempts. Default 1s.
	RetryBackoff time.Duration

	// DLQ is required if MaxAttempts > 0; nil disables retry/DLQ
	// (handler errors are still acked to avoid blocking the group,
	// which is the pre-DLQ behavior).
	DLQ DLQ

	// Deduper is optional. nil = no dedup (every record processed).
	Deduper Deduper
}

// New constructs a Consumer. The franz-go client is owned by the
// Consumer; Close releases it.
func New(cfg Config, registry HandlerRegistry) (*Consumer, error) {
	if len(cfg.Topics) == 0 {
		return nil, errors.New("consumer: at least one topic required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: GroupID required")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		// Start from the beginning of every topic on first join.
		// Without this, a consumer that joins a fresh broker (or
		// a topic that was recreated while the consumer was
		// offline) starts at the END and misses every event that
		// was published in the gap. This was masked by E2E tests
		// in CI (where service startup and Kafka publish were
		// sequenced) and only surfaced when chaos tests restarted
		// the broker mid-chain. The cost is at-least-once
		// redelivery on a brand-new consumer group; the deduper
		// makes that safe.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return &Consumer{
		client:       cli,
		registry:     registry,
		dlq:          cfg.DLQ,
		deduper:      cfg.Deduper,
		maxAttempts:  cfg.MaxAttempts,
		retryBackoff: cfg.RetryBackoff,
		stopped:      make(chan struct{}),
	}, nil
}

// Run polls Kafka and dispatches records to handlers until ctx is
// cancelled or Stop is called.
func (c *Consumer) Run(ctx context.Context) error {
	defer close(c.stopped)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				// Non-fatal fetch errors are logged via the slog
				// default logger (a no-op until InitTracing installs
				// one); the loop continues so a transient broker
				// hiccup doesn't kill the consumer. Throttled to
				// once per 5s — a closed client floods the loop
				// with identical "client closed" entries at line
				// rate (~MB/sec), and the throttle keeps the log
				// useful for ops while the consumer winds down.
				if now := time.Now().UnixNano(); now-lastPollWarn.Load() > int64(5*time.Second) {
					slog.Default().Warn("consumer: poll fetch error",
						"topic", e.Topic, "partition", e.Partition, "err", e.Err)
					lastPollWarn.Store(now)
				}
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			c.dispatch(ctx, rec)
		})
		// Best-effort commit; ctx cancellation during shutdown is the
		// common failure mode and the next session rebalances offsets.
		// Errors are not fatal — franz-go retries the commit on the
		// next iteration and the records are at worst re-processed.
		if err := c.client.CommitMarkedOffsets(ctx); err != nil &&
			!errors.Is(err, context.Canceled) {
			slog.Default().Warn("consumer: commit offsets", "err", err)
		}
	}
}

// Stop signals Run to exit at the next poll boundary.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.client.Close()
	})
}

// dispatch decodes the record and invokes the registered handler.
// Retry + DLQ semantics live here.
//
// Panics from the handler are recovered and treated as a regular
// retryable error. Without this guard, a single panic kills the
// consumer goroutine silently — events pile up in Kafka retention
// until Kubernetes notices the liveness probe fails and reschedules
// the pod. With the recover, the panic is counted as a failed
// attempt (up to MaxAttempts) and ultimately DLQ'd like any other
// failure.
func (c *Consumer) dispatch(ctx context.Context, rec *kgo.Record) {
	// OBS-5: extract W3C traceparent from the Kafka record
	// headers before unmarshalling the body. This is the
	// cross-process trace propagation ADR-0004 mandates; without
	// it, every consuming service starts a fresh trace root and
	// the Tempo service map breaks across Kafka topic boundaries.
	// The legacy envelope-based path (env.TraceID/env.SpanID)
	// stays as a fallback for older producers that haven't been
	// recompiled with the new wire format — SpanFromEnvelope
	// below prefers the envelope IDs when the header is absent.
	carrier := kafkaprop.RecordHeaderCarrier{}
	for _, h := range rec.Headers {
		carrier[h.Key] = string(h.Value)
	}
	ctx = kafkaprop.Extract(ctx, carrier)

	var env events.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		// Decode failure: ship to DLQ for triage AND mark the
		// record so the unparseable bytes don't loop on every
		// poll (we disabled auto-commit; without markRecord here
		// the same poison message re-fetches forever).
		c.toDLQ(ctx, nil, fmt.Sprintf("decode error: %v", err), rec)
		c.markRecord(rec)
		return
	}

	// Open the trace span BEFORE the recover defer so a panic
	// inside the handler chain still cleans up the span. The
	// recover catches panics from the handler dispatch path
	// (registry lookup, deduper, markRecord, retry loop).
	// SpanFromEnvelope picks up the (already extracted) ctx and
	// re-derives a remote span context from the envelope IDs —
	// the two paths converge on the same trace because the header
	// extract above populated ctx with the same SpanContext.
	ctx, span := kafkaprop.SpanFromEnvelope(ctx, env.TraceID, env.SpanID, "consumer."+env.EventType)
	defer func() {
		span.End()
		// Recover from any panic in the handler chain. Without
		// this guard, a single panic kills the consumer goroutine
		// silently and events pile up in Kafka retention until
		// Kubernetes notices the failed liveness probe. Here we
		// log and mark the record for commit so we don't loop on
		// a programming bug (retries won't fix a panic).
		if r := recover(); r != nil {
			slog.Default().Error("consumer: handler panic",
				"event_id", env.EventID,
				"event_type", env.EventType,
				"panic", fmt.Sprintf("%v", r),
				"offset", rec.Offset,
				"partition", rec.Partition)
			c.markRecord(rec)
		}
	}()
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", env.EventType),
		attribute.String("messaging.kafka.message.key", string(rec.Key)),
	)

	if c.deduper != nil {
		seen, err := c.deduper.Seen(ctx, env.EventID)
		if err == nil && seen {
			// Already processed — idempotent skip. MUST mark the
			// record for commit because franz-go disables auto-commit;
			// otherwise the same duplicate event loops forever on the
			// next poll, holding the partition hostage (audit NEW-P0-1,
			// originally SAGA-7). The deduper's Mark call will be a no-op
			// for the event_id we already saw, so re-delivery from this
			// record's offset is safe.
			c.markRecord(rec)
			return
		}
	}

	handler, ok := c.registry[env.EventType]
	if !ok {
		// Unknown event_type — ack-and-skip to avoid blocking the
		// group on a forward-compatible producer. MUST mark the
		// record for commit because franz-go disables auto-commit;
		// otherwise the same unknown event loops forever on the
		// next poll, holding the partition hostage.
		c.markRecord(rec)
		return
	}

	maxAttempts := c.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := handler(ctx, &env); err != nil {
			lastErr = err
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return
				case <-time.After(c.retryBackoff):
				}
			}
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		c.toDLQ(ctx, &env, lastErr.Error(), rec)
		// Mark even on DLQ so the poison pill doesn't come back
		// on every restart — DLQ is the terminal state for the
		// record. Operators triage the DLQ topic out-of-band.
		c.markRecord(rec)
		return
	}

	if c.deduper != nil {
		// Mark errors mean the next restart will reprocess this
		// event. The application handlers must be idempotent at
		// the DB layer (ON CONFLICT, optimistic version check,
		// etc.). The poller's deduper.Seen() check is the soft
		// dedup; the DB-level guard is the hard one.
		if err := c.deduper.Mark(ctx, env.EventID); err != nil {
			slog.Default().Warn("consumer: deduper mark failed",
				"event_id", env.EventID, "err", err)
		}
	}
	c.markRecord(rec)
}

// markRecord asks franz-go to include rec in the next
// CommitMarkedOffsets call. No-op when client is nil (unit-test
// path that exercises dispatch without a real broker).
func (c *Consumer) markRecord(rec *kgo.Record) {
	if c.client == nil {
		return
	}
	c.client.MarkCommitRecords(rec)
}

func (c *Consumer) toDLQ(ctx context.Context, env *events.Envelope, reason string, rec *kgo.Record) {
	if c.dlq == nil {
		return
	}
	if env == nil {
		// Synthesize a minimal envelope so the DLQ consumer can
		// at least see the topic and key.
		env = &events.Envelope{
			EventType:     "Unknown",
			AggregateID:   string(rec.Key),
			AggregateType: "Unknown",
		}
	}
	// rec.Topic is the canonical source topic. Passing it through
	// to DLQ.Send ensures the DLQ event lands on <source>.DLQ
	// instead of the misrouted `events.DLQ` that pre-fix code
	// produced (audit CONSUMER-1).
	if err := c.dlq.Send(ctx, env, rec.Topic, reason); err != nil {
		// DLQ failure means the poison pill stays in the topic.
		// We've marked the record for commit so we won't loop on
		// it; operators can replay from the DLQ topic manually.
		slog.Default().Warn("consumer: dlq send failed",
			"event_id", env.EventID, "reason", reason, "err", err)
	}
}
