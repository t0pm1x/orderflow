// Package outbox contains the Payment Service transactional outbox
// writer. Append is called within the same pgx.Tx that writes the
// payments row, so the outbox row commits/rolls back with the
// business state change (atomicity).
//
// Payment Service emits PaymentCompleted and PaymentFailed events
// from the webhook handler (sub-stage 3.5.d per spec, wired in 3.8.d
// per-service consumer/handler work).
package outbox

import (
	"context"
	_ "embed"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Table is the Payment Service outbox table name. The schema lives
// in services/payment/migrations/0001_init.sql (sub-stage 3.5.e) and
// matches what Append writes here.
const Table = "payment_outbox"

// insertSQL is the canonical INSERT used by Append. Kept as a
// constant (//go:embed) so tests can match against it and reviewers
// can grep for the exact bytes the service writes.
//
//go:embed insert.sql
var insertSQL string

// PGWriter is the Payment Service implementation of outbox.Writer.
// Stateless — the tx comes from the caller.
type PGWriter struct{}

// NewPGWriter constructs a PGWriter.
func NewPGWriter() *PGWriter { return &PGWriter{} }

// Append inserts r into the payment_outbox table using tx. The row's
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
