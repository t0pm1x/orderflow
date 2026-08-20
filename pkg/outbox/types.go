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

	"github.com/jackc/pgx/v5"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Source reads PENDING outbox rows from a single table. Implementations
// live in each service's internal/outbox package (they need to know
// the table name and column shape).
//
// RunInTx acquires a row-level lock on the returned rows for the
// duration of fn (FOR UPDATE SKIP LOCKED). Multiple poller replicas
// can run safely: each gets a disjoint batch because locked rows are
// skipped by concurrent transactions. fn may call MarkSentTx /
// MarkFailedTx to advance rows; on a nil return the tx commits
// (releasing the locks); on a non-nil return it rolls back so the
// rows stay PENDING and will be re-fetched on the next poll.
//
// AttemptsOfTx is the v1.1.4 fix for v1.1.2 P1-#3: the in-memory
// retry counter survives only within a single pod lifetime, so a
// pod restart wiping the counter would silently re-DLQ a row that
// had already been moved past the retry budget. Reading attempts
// from the DB row inside the locked tx makes the budget durable
// across restarts and consistent across replicas.
//
// BumpAttempts (OBX-001) is the v1.1.5 fix for v1.1.4's inert
// "DB attempts survives restarts" claim. MarkFailedTx was the only
// writer of the `attempts` column, and MarkFailedTx was only called
// at the terminal FAILED transition (which excluded the row from
// future fetches), so the column was always 0 for any PENDING row.
// BumpAttempts is the autonomous (non-tx) UPDATE that increments
// `attempts` for PENDING rows on every publish failure, making the
// per-row budget durable across restarts even before the row
// crosses MaxAttempts.
type Source interface {
	RunInTx(ctx context.Context, limit int, fn func(tx pgx.Tx, recs []outbox.Record) error) error
	MarkSentTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error
	MarkFailedTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error
	AttemptsOfTx(ctx context.Context, tx pgx.Tx, eventIDs []string) (map[string]int, error)
	BumpAttempts(ctx context.Context, eventIDs []string, reason string) error
	// Lag returns the current PENDING and FAILED row counts as a
	// single SQL read, used to refresh the OBS-9 outbox_pending_events
	// and outbox_failed_events gauges once per poll cycle.
	// Implementations must use a single COUNT(*) … FILTER query (or
	// equivalent) so the per-cycle cost is constant regardless of
	// outbox size.
	Lag(ctx context.Context) (pending, failed int64, err error)
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
	// ObserveLag is the OBS-9 hook. pending is the current count of
	// PENDING rows; failed is the current count of FAILED rows. The
	// gauges are point-in-time snapshots — callers are expected to
	// invoke ObserveLag once per poll cycle.
	ObserveLag(ctx context.Context, pending, failed int64)
}

// NoopMetrics is the default Metrics impl when none is configured.
type NoopMetrics struct{}

// ObservePoll is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObservePoll(context.Context, int, time.Duration, error) {}

// ObservePublish is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObservePublish(context.Context, int, error) {}

// ObserveDLQ is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObserveDLQ(context.Context, outbox.Record, string) {}

// ObserveLag is a no-op; satisfies the Metrics interface without emitting anything.
func (NoopMetrics) ObserveLag(context.Context, int64, int64) {}
