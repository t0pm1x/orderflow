// Package outbox: PGSource implements pkg/outbox.Source against
// the saga_outbox table. Mirrors the shape of
// services/{order,payment,inventory}/internal/outbox/source.go.
//
// The saga runtime always publishes to "order-events", so this
// Source stamps Topic = "order-events" on every Record it returns;
// the saga_outbox schema does not carry a topic column, which is
// fine for a single-topic service. (The other services carry the
// topic per-row because they publish to more than one topic.)
package outbox

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Topic is the Kafka topic the saga service publishes all emitted
// events to. Source.FetchPending stamps it on every returned Record.
const Topic = "order-events"

const fetchPendingSQL = `SELECT id, event_id, aggregate_id, aggregate_type,
                              event_type, payload, headers
                         FROM saga_outbox
                        WHERE status = 'PENDING'
                        ORDER BY created_at ASC
                        LIMIT $1`

const markSentSQL = `UPDATE saga_outbox
                         SET status = 'SENT', sent_at = NOW()
                       WHERE event_id = ANY($1)
                         AND status = 'PENDING'`

const markFailedSQL = `UPDATE saga_outbox
                          SET attempts = attempts + 1,
                              last_error = COALESCE($1, last_error)
                        WHERE event_id = ANY($2)
                          AND status = 'PENDING'`

// PGSource reads/marks rows in the saga_outbox table.
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
// created_at ASC. Topic is stamped to "order-events" on every
// record so the KafkaPublisher (pkg/outbox) routes correctly
// without reading it from the row.
func (s *PGSource) FetchPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	rows, err := s.pool.Query(ctx, fetchPendingSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]outbox.Record, 0, limit)
	for rows.Next() {
		var (
			r       outbox.Record
			rowID   int64
			payload []byte
			headers []byte
		)
		if err := rows.Scan(
			&rowID, &r.EventID, &r.AggregateID, &r.AggregateType,
			&r.EventType, &payload, &headers,
		); err != nil {
			return nil, err
		}
		r.Payload = payload
		r.Topic = Topic
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkSent transitions rows to SENT for the given event_ids. No-op
// when eventIDs is empty (avoids a useless roundtrip).
func (s *PGSource) MarkSent(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, markSentSQL, eventIDs)
	return err
}

// MarkFailed bumps the attempt counter for the given event_ids and
// records the failure reason. The row stays PENDING; the poller's
// MaxAttempts logic decides when to DLQ it.
func (s *PGSource) MarkFailed(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, markFailedSQL, "publish failed", eventIDs)
	return err
}