// Package outbox implements pkg/outbox.Source against the
// order_outbox table. The poller (3.7.a) calls RunInTx to fetch a
// batch of PENDING rows under a row-level lock, hands the batch to
// KafkaPublisher (3.7.b), and then MarkSentTx / MarkFailedTx the
// returned event_ids. Two replicas running the poller concurrently
// see disjoint batches because FOR UPDATE SKIP LOCKED makes locked
// rows invisible to other transactions.
package outbox

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// SQL fragments for the poller side of the outbox. Embedded so
// tests can match against them and reviewers can grep for the
// exact bytes the service reads/writes.
//
//go:embed fetchPending.sql
var fetchPendingSQL string

//go:embed markSent.sql
var markSentSQL string

//go:embed markFailed.sql
var markFailedSQL string

// PGSource reads/marks rows in the order_outbox table.
type PGSource struct {
	pool *pgxpool.Pool
}

// NewPGSource constructs a PGSource backed by pool.
func NewPGSource(pool *pgxpool.Pool) *PGSource {
	return &PGSource{pool: pool}
}

// Compile-time interface check.
var _ pkgoutbox.Source = (*PGSource)(nil)

// RunInTx opens a transaction, fetches up to limit PENDING rows
// with FOR UPDATE SKIP LOCKED, calls fn(tx, recs), and commits on a
// nil return / rolls back otherwise. The row lock is held for the
// entire fn execution so concurrent pollers running in parallel
// skip these rows.
func (s *PGSource) RunInTx(ctx context.Context, limit int, fn func(tx pgx.Tx, recs []outbox.Record) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, fetchPendingSQL, limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		out := make([]outbox.Record, 0, limit)
		for rows.Next() {
			var r outbox.Record
			if err := rows.Scan(
				&r.EventID, &r.EventType, &r.AggregateID, &r.AggregateType,
				&r.SchemaVersion, &r.Topic, &r.Payload,
			); err != nil {
				return err
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return fn(tx, out)
	})
}

// MarkSentTx transitions rows to SENT for the given event_ids inside
// the supplied tx. Pairs with RunInTx so the row's status and the
// row's lock release are committed atomically.
func (s *PGSource) MarkSentTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, markSentSQL, eventIDs)
	return err
}

// MarkFailedTx transitions rows to FAILED for the given event_ids
// inside the supplied tx. Used by the DLQ path: the row lock prevents
// another poller from picking up the same row past MaxAttempts.
func (s *PGSource) MarkFailedTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, markFailedSQL, eventIDs)
	return err
}
