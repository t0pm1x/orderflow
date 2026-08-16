// Package outbox: PGSource implements pkg/outbox.Source against
// the inventory_outbox table. Aggregate_id is TEXT here because
// SKU is the aggregate key (not a UUID).
package outbox

import (
	"context"
	_ "embed"

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

// PGSource reads/marks rows in the inventory_outbox table.
type PGSource struct {
	pool *pgxpool.Pool
}

// NewPGSource constructs a PGSource backed by pool.
func NewPGSource(pool *pgxpool.Pool) *PGSource {
	return &PGSource{pool: pool}
}

// Compile-time interface check.
var _ pkgoutbox.Source = (*PGSource)(nil)

// FetchPending returns up to limit PENDING rows ordered by
// created_at ASC.
func (s *PGSource) FetchPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	rows, err := s.pool.Query(ctx, fetchPendingSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]outbox.Record, 0, limit)
	for rows.Next() {
		var r outbox.Record
		if err := rows.Scan(
			&r.EventID, &r.EventType, &r.AggregateID, &r.AggregateType,
			&r.SchemaVersion, &r.Topic, &r.Payload,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkSent transitions rows to SENT for the given event_ids.
func (s *PGSource) MarkSent(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, markSentSQL, eventIDs)
	return err
}

// MarkFailed transitions rows to FAILED for the given event_ids.
func (s *PGSource) MarkFailed(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, markFailedSQL, eventIDs)
	return err
}
