// Package outbox implements pkg/outbox.Source against the
// payment_outbox table. See services/order/internal/outbox for the
// canonical pattern; this is its mirror.
package outbox

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

//go:embed fetchPending.sql
var fetchPendingSQL string

//go:embed markSent.sql
var markSentSQL string

//go:embed markFailed.sql
var markFailedSQL string

// attemptsOfSQL mirrors the saga source pattern: the poller's
// per-Pod in-memory retry counter is volatile across restarts, so
// we read attempts from the DB row itself inside the locked tx.
// See services/saga/internal/outbox/source.go for the original.
const attemptsOfSQL = `SELECT event_id, attempts FROM payment_outbox WHERE event_id = ANY($1)`

// PGSource reads/marks rows in the payment_outbox table.
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
// the supplied tx.
func (s *PGSource) MarkSentTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, markSentSQL, eventIDs)
	return err
}

// MarkFailedTx transitions rows to FAILED for the given event_ids
// inside the supplied tx.
func (s *PGSource) MarkFailedTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, markFailedSQL, "publish failed", eventIDs)
	return err
}

// AttemptsOfTx returns the current attempts counter for each given
// event_id inside the supplied tx. See saga/internal/outbox/source.go
// for the full rationale.
func (s *PGSource) AttemptsOfTx(ctx context.Context, tx pgx.Tx, eventIDs []string) (map[string]int, error) {
	if len(eventIDs) == 0 {
		return map[string]int{}, nil
	}
	rows, err := tx.Query(ctx, attemptsOfSQL, eventIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int, len(eventIDs))
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}
