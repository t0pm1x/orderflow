// Package outbox contains the Saga Service transactional outbox
// writer. Append is called within the same pgx.Tx that writes the
// order_sagas row, so the outbox row commits/rolls back with the
// business state change (atomicity).
//
// The Saga Service emits order-events (StockReserveRequested,
// PaymentRequested, OrderConfirmed, StockReleaseRequested,
// OrderCancelled). Topic is constant per service; the writer takes
// the supplied Record.Topic but the saga runtime always sets it to
// "order-events" — see consumer/handlers.go.
package outbox

import (
	"context"
	"encoding/json"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Table is the Saga Service outbox table name. The schema lives in
// services/saga/migrations/0002_saga_outbox.sql.
const Table = "saga_outbox"

// insertSQL is the canonical INSERT used by Append. Kept as a
// constant so reviewers can grep for the exact bytes the service
// writes. schema_version is fixed at 1 in v0.5.0 (the saga payload
// schema is locked for this release); status starts at PENDING.
const insertSQL = `INSERT INTO saga_outbox
    (event_id, aggregate_id, aggregate_type, event_type, payload, headers, schema_version, status)
VALUES
    ($1, $2, $3, $4, $5, $6, 1, 'PENDING')`

// PGWriter is the Saga Service implementation of outbox.Writer.
// Stateless — the tx comes from the caller.
type PGWriter struct{}

// NewPGWriter constructs a PGWriter.
func NewPGWriter() *PGWriter { return &PGWriter{} }

// Append inserts r into the saga_outbox table using tx. The row's
// status starts at PENDING; the poller (sub-stage 3.7 / pkg/outbox)
// will transition it to SENT after Kafka confirms the publish.
//
// The payload column is JSONB; pgx marshals []byte as text so the
// cast to JSONB is implicit. Headers is JSONB too — empty headers
// are JSON-marshalled to "{}" so the column satisfies NOT NULL.
func (w *PGWriter) Append(ctx context.Context, tx outbox.DBTX, r outbox.Record) error {
	headers, err := json.Marshal(r.Headers)
	if err != nil {
		return err
	}
	if r.Headers == nil {
		headers = []byte(`{}`)
	}
	_, err = tx.Exec(ctx, insertSQL,
		r.EventID,
		r.EventType,
		r.AggregateID,
		r.AggregateType,
		r.Payload,
		headers,
	)
	return err
}

// Compile-time interface check.
var _ outbox.Writer = (*PGWriter)(nil)