// Package outbox contains the Order Service transactional outbox
// writer. Append is called within the same pgx.Tx that writes the
// orders row, so the outbox row commits/rolls back with the business
// state change (atomicity).
package outbox

import (
	"context"
	_ "embed"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Table is the Order Service outbox table name. The schema is
// defined in services/order/migrations/0001_init.sql (sub-stage 3.4.e)
// and matches what Append writes here.
const Table = "order_outbox"

// insertSQL is the canonical INSERT used by Append. It is kept as a
// constant so tests can match against it.
//
//go:embed insert.sql
var insertSQL string

// PGWriter is the Order Service implementation of outbox.Writer. It
// has no state — the tx comes from the caller.
type PGWriter struct{}

// NewPGWriter constructs a PGWriter.
func NewPGWriter() *PGWriter { return &PGWriter{} }

// Append inserts r into the order_outbox table using tx. The row's
// status starts at PENDING; the poller (sub-stage 3.7) will transition
// it to SENT after Kafka confirms the publish.
func (w *PGWriter) Append(ctx context.Context, tx outbox.DBTX, r outbox.Record) error {
	_, err := tx.Exec(ctx, insertSQL,
		r.EventID,
		r.EventType,
		r.AggregateID,
		r.AggregateType,
		r.SchemaVersion,
		r.Topic,
		r.Payload,
		outbox.StatusPending,
	)
	return err
}

// Compile-time interface check.
var _ outbox.Writer = (*PGWriter)(nil)
