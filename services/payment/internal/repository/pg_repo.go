// Package repository contains the Payment Service's data-access
// implementations. PGRepo is the production implementation of
// webhook.Repository, backed by a pgxpool against the schema defined
// in services/payment/migrations/0001_init.sql.
//
// UpdateStatus is atomic: the payments row and every outbox row are
// written in the same transaction, so the poller (3.7) can never
// publish a PaymentCompleted for a payment whose status update rolled
// back. The outbox half delegates to
// services/payment/internal/outbox.PGWriter so the canonical outbox
// INSERT lives in exactly one place.
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/outbox"

	svcoutbox "github.com/t0pm1x/orderflow/services/payment/internal/outbox"
	"github.com/t0pm1x/orderflow/services/payment/internal/webhook"
)

// Table is the Payment Service's business table name. The same string
// is referenced in services/payment/migrations/0001_init.sql.
const Table = "payments"

// PGRepo is the PostgreSQL-backed implementation of webhook.Repository.
type PGRepo struct {
	pool   *pgxpool.Pool
	writer *svcoutbox.PGWriter
}

// NewPGRepo constructs a PGRepo backed by pool. The outbox writer is
// stateless, so it is created once and reused.
func NewPGRepo(pool *pgxpool.Pool) *PGRepo {
	return &PGRepo{pool: pool, writer: svcoutbox.NewPGWriter()}
}

// Compile-time assertion that PGRepo satisfies webhook.Repository.
var _ webhook.Repository = (*PGRepo)(nil)

// UpsertFromWebhook creates the payments row from the webhook payload
// if it doesn't exist (INSERT ... ON CONFLICT (id) DO NOTHING) and then
// transitions the status with the terminal-state guard. All in one
// transaction so a crash between INSERT and UPDATE never produces a
// half-row. Returns (true, nil) on a real transition with outbox rows
// emitted, (false, nil) when the row's current status is terminal
// (no-op, no outbox emission).
//
// The auto-create is what makes the playground "Force succeed/fail"
// buttons work before the saga's PaymentRequested consumer has run —
// the mock provider accepts any valid payment_id (deterministic on
// order_id) and the SPA's button fires the webhook directly without
// waiting for the saga.
//
// amount_cents and last_four default to 0 / "" if absent (the SPA's
// webhooks don't always send them; the saga path fills them via the
// PaymentRequested consumer).
func (r *PGRepo) UpsertFromWebhook(
	ctx context.Context,
	paymentID, orderID string,
	amountCents int64,
	lastFour string,
	to webhook.PaymentStatus,
	events ...outbox.Record,
) (bool, error) {
	var advanced bool
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		// Auto-create: INSERT with ON CONFLICT DO NOTHING. The
		// unique-constraint on payments.id turns this into a
		// no-op when the saga's consumer already created the row.
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (id, order_id, amount_cents, status, last_four)
			 VALUES ($1, $2, $3, '', $4)
			 ON CONFLICT (id) DO NOTHING`,
			paymentID, orderID, amountCents, lastFour,
		); err != nil {
			return fmt.Errorf("upsert payment: %w", err)
		}
		// Terminal-state guarded transition. The `status = ''`
		// guard on the row created above ensures the first webhook
		// always fires; subsequent webhooks obey the captured/failed
		// terminal-state guard the same as the pre-fix flow.
		tag, err := tx.Exec(ctx,
			`UPDATE payments
			    SET status = $2, updated_at = NOW()
			  WHERE id = $1
			    AND status NOT IN ('captured', 'failed')`,
			paymentID, string(to))
		if err != nil {
			return fmt.Errorf("update payment status: %w", err)
		}
		switch tag.RowsAffected() {
		case 1:
			advanced = true
			for _, ev := range events {
				if err := r.writer.Append(ctx, tx, ev); err != nil {
					return fmt.Errorf("insert outbox: %w", err)
				}
			}
			return nil
		case 0:
			return nil
		default:
			return fmt.Errorf("UpsertFromWebhook: unexpected RowsAffected=%d for payment_id=%s", tag.RowsAffected(), paymentID)
		}
	})
	return advanced, err
}
