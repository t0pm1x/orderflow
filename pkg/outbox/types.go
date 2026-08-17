// Package outbox is the shared outbox poller/publisher used by every
// orderflow service. It glues together:
//
//   - a Source that reads PENDING rows from a service's outbox table
//     (e.g. services/order/internal/outbox/postgres.go's mirror),
//   - a Publisher that ships them to Kafka (sub-stage 3.7.b),
//   - a DLQ for events that exceed the retry budget (3.7.c),
//   - a Metrics hook (3.7.d),
//
// and runs a single Poll loop per service. The poller is at-least-once:
// a successful Publish is followed by MarkSent in the same iteration;
// any error leaves the row PENDING for the next poll. See
// docs/adr/0002-outbox-pattern.md.
package outbox

import (
	"context"
	"time"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Source reads PENDING outbox rows from a single table. Implementations
// live in each service's internal/outbox package (they need to know
// the table name and column shape).
//
// Rows are returned ordered by created_at ASC so the poller emits
// events in roughly the order they were committed.
type Source interface {
	FetchPending(ctx context.Context, limit int) ([]outbox.Record, error)
	MarkSent(ctx context.Context, eventIDs []string) error
	MarkFailed(ctx context.Context, eventIDs []string) error
}

// Publisher ships a batch of outbox records to Kafka. Returns nil
// only after Kafka has confirmed the publish (per franz-go's
// ProduceSync). On error, the poller leaves the rows PENDING.
type Publisher interface {
	Publish(ctx context.Context, recs []outbox.Record) error
}

// DLQ is the destination for events that exceeded MaxAttempts.
// Returning nil means the row was successfully moved to the DLQ
// topic; on error the row stays FAILED for the next poll to retry.
type DLQ interface {
	Send(ctx context.Context, rec outbox.Record, reason string) error
}

// Metrics is an optional observability hook. The no-op implementation
// is useful in tests.
type Metrics interface {
	ObservePoll(ctx context.Context, rows int, dur time.Duration, err error)
	ObservePublish(ctx context.Context, count int, err error)
	ObserveDLQ(ctx context.Context, rec outbox.Record, reason string)
}

// NoopMetrics is the default Metrics impl when none is configured.
type NoopMetrics struct{}

// ObservePoll is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObservePoll(context.Context, int, time.Duration, error) {}

// ObservePublish is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObservePublish(context.Context, int, error) {}

// ObserveDLQ is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObserveDLQ(context.Context, outbox.Record, string) {}
