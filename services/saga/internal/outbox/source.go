// Package outbox implements pkg/outbox.Source against the
// saga_outbox table. Mirrors the shape of
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
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Topic is the Kafka topic the saga service publishes all emitted
// events to. Source stamps it on every Record returned by RunInTx.
const Topic = "order-events"

const fetchPendingSQL = `SELECT id, event_id, aggregate_id, aggregate_type,
                              event_type, payload, headers
                         FROM saga_outbox
                        WHERE status = 'PENDING'
                        ORDER BY created_at ASC
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED`

const markSentSQL = `UPDATE saga_outbox
                         SET status = 'SENT', sent_at = NOW()
                       WHERE event_id = ANY($1)
                         AND status = 'PENDING'`

const markFailedSQL = `UPDATE saga_outbox
                          SET status = 'FAILED',
                              last_error = COALESCE($1, last_error)
                        WHERE event_id = ANY($2)
                          AND status = 'PENDING'`

// bumpAttemptsSQL is the autonomous (non-tx) UPDATE the poller
// runs to increment `attempts` on every publish failure. Pairs
// with Source.BumpAttempts (OBX-001).
const bumpAttemptsSQL = `UPDATE saga_outbox
                            SET attempts   = attempts + 1,
                                last_error = COALESCE($1, last_error)
                          WHERE event_id = ANY($2)
                            AND status = 'PENDING'`

// lagSQL returns the current PENDING and FAILED row counts in a
// single read. OBS-9 wires the poller to refresh the
// outbox_pending_events / outbox_failed_events gauges once per
// cycle. Single COUNT(*) FILTER read; see
// services/order/internal/outbox for the rationale.
const lagSQL = `SELECT
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
FROM saga_outbox`

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

// RunInTx opens a transaction, fetches up to limit PENDING rows
// with FOR UPDATE SKIP LOCKED, calls fn(tx, recs), and commits on a
// nil return / rolls back otherwise. Topic is stamped to
// "order-events" on every record so the KafkaPublisher (pkg/outbox)
// routes correctly without reading it from the row.
func (s *PGSource) RunInTx(ctx context.Context, limit int, fn func(tx pgx.Tx, recs []outbox.Record) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, fetchPendingSQL, limit)
		if err != nil {
			return err
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
				return err
			}
			r.Payload = payload
			r.Topic = Topic
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

// MarkFailedTx bumps the attempt counter for the given event_ids
// inside the supplied tx and records the failure reason. The row
// stays PENDING until the poller's MaxAttempts logic routes it to
// the DLQ (via MarkFailedTx + dlq.Send).
func (s *PGSource) MarkFailedTx(ctx context.Context, tx pgx.Tx, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, markFailedSQL, "publish failed", eventIDs)
	return err
}

// BumpAttempts is the OBX-001 fix from pkg/outbox/types.go:Source:
// the v1.1.4 claim that DB `attempts` survives restarts was inert
// because MarkFailedTx was the only writer and only fired at the
// terminal FAILED transition. The new autonomous (non-tx) UPDATE
// increments `attempts` for PENDING rows on every publish failure so
// the per-row retry budget is durable across pod restarts.
//
// Required by the pkg/outbox.Source interface; the saga PGSource
// implements the same shape as the order/payment/inventory mirrors.
func (s *PGSource) BumpAttempts(ctx context.Context, eventIDs []string, reason string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, bumpAttemptsSQL, reason, eventIDs)
	return err
}

// AttemptsOfTx returns the current attempts counter for each given
// event_id, read inside the supplied tx. Used by the poller after
// MarkFailedTx to decide whether to DLQ a row. Reading from the DB
// (instead of a per-Pod sync.Map) makes the retry budget survive
// pod restarts and stay consistent across replicas.
func (s *PGSource) AttemptsOfTx(ctx context.Context, tx pgx.Tx, eventIDs []string) (map[string]int, error) {
	if len(eventIDs) == 0 {
		return map[string]int{}, nil
	}
	rows, err := tx.Query(ctx,
		`SELECT event_id, attempts FROM saga_outbox WHERE event_id = ANY($1)`,
		eventIDs)
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

// Lag returns the current count of PENDING and FAILED rows for
// the OBS-9 outbox_pending_events / outbox_failed_events gauges.
// See services/order/internal/outbox/source.go for the rationale.
func (s *PGSource) Lag(ctx context.Context) (pending, failed int64, err error) {
	row := s.pool.QueryRow(ctx, lagSQL)
	if err := row.Scan(&pending, &failed); err != nil {
		return 0, 0, fmt.Errorf("lag query: %w", err)
	}
	return pending, failed, nil
}
