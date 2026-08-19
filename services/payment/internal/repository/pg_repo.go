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
	"errors"
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

// Get loads a single payment by id. Returns an error wrapping
// webhook.ErrPaymentNotFound when the row does not exist; the handler
// translates that into a 404. The ctx is honored by pgx so an HTTP
// client disconnect aborts the in-flight query.
func (r *PGRepo) Get(ctx context.Context, id string) (*webhook.Payment, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, order_id, amount_cents, status
		   FROM payments WHERE id = $1`, id)
	var (
		p      webhook.Payment
		status string
	)
	if err := row.Scan(&p.ID, &p.OrderID, &p.AmountCents, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("payment %s: %w", id, webhook.ErrPaymentNotFound)
		}
		return nil, err
	}
	p.Status = webhook.PaymentStatus(status)
	return &p, nil
}

// UpdateStatus moves the payment to status and appends every supplied
// outbox Record in one transaction. A zero-row UPDATE means the id
// vanished between Get and here, which is reported as
// webhook.ErrPaymentNotFound rather than a silent no-op.
func (r *PGRepo) UpdateStatus(ctx context.Context, id string, status webhook.PaymentStatus, events ...outbox.Record) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE payments SET status = $2, updated_at = NOW() WHERE id = $1`,
			id, string(status),
		)
		if err != nil {
			return fmt.Errorf("update payment status: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("payment %s: %w", id, webhook.ErrPaymentNotFound)
		}
		for _, ev := range events {
			if err := r.writer.Append(ctx, tx, ev); err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}
		}
		return nil
	})
}

// UpdateStatusFromNonTerminal transitions the payment to `to` only
// if its current status is non-terminal (NOT in {captured, failed}).
// Returns (true, nil) on transition with outbox rows emitted, (false,
// nil) when the row's status is already terminal (no-op, no outbox
// emission), and (false, ErrPaymentNotFound) when the row doesn't
// exist. Outbox rows only commit on a real transition so a replay
// against a terminal-state payment doesn't produce duplicate
// PaymentCompleted / PaymentFailed events.
func (r *PGRepo) UpdateStatusFromNonTerminal(ctx context.Context, id string, to webhook.PaymentStatus, events ...outbox.Record) (bool, error) {
	var advanced bool
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE payments
			    SET status = $2, updated_at = NOW()
			  WHERE id = $1
			    AND status NOT IN ('captured', 'failed')`,
			id, string(to))
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
			return fmt.Errorf("UpdateStatusFromNonTerminal: unexpected RowsAffected=%d for payment_id=%s", tag.RowsAffected(), id)
		}
	})
	return advanced, err
}

// runInTx wraps a tx body and converts pgx.ErrNoRows to ErrPaymentNotFound
// when the row vanished. Used by UpdateStatusIfCurrent so the
// handler can return 404 cleanly.
func (r *PGRepo) runInTx(ctx context.Context, body func(pgx.Tx) (bool, error)) (bool, error) {
	return runInPGx(ctx, r.pool, body)
}

// runInPGx is a package-local helper so the body closure can use
// the pool without leaking pgx pool semantics.
func runInPGx(ctx context.Context, pool *pgxpool.Pool, body func(pgx.Tx) (bool, error)) (bool, error) {
	var advanced bool
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var ierr error
		advanced, ierr = body(tx)
		return ierr
	})
	if err != nil {
		// Distinguish "row vanished" from real errors. body returns
		// nil on no-op (RowsAffected=0) and a value on transition
		// or error. ErrPaymentNotFound surfaces only when body
		// explicitly returned it.
		return advanced, err
	}
	return advanced, nil
}
