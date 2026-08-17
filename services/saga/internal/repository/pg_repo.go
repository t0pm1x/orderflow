// Package repository contains the Saga Service's data-access layer
// over the order_sagas table. PGRepo is the production
// implementation; the tests live in pg_repo_test.go and skip when
// DATABASE_URL is unset so the package stays buildable without a
// running Postgres.
package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/services/saga"
)

// Saga is the in-memory shape of an order_sagas row. Items is
// pre-marshalled JSON (the JSONB column in Postgres). State is the
// saga state machine string (initiated, stock_reserved, etc.).
type Saga struct {
	OrderID       string
	State         saga.State
	Items         []byte
	TotalCents    int64
	ReservationID string
}

// Repository is the abstract contract PGRepo satisfies. The
// consumer handlers depend on this so they can be unit-tested with
// a fake (the handler tests use a small inline fake, not a mock
// library — keeps stdlib-only).
type Repository interface {
	Insert(ctx context.Context, s *Saga) error
	Get(ctx context.Context, orderID string) (*Saga, error)
	UpdateState(ctx context.Context, orderID string, state saga.State) error
	SetReservationID(ctx context.Context, orderID, reservationID string) error
}

// ErrNotFound is returned by Get/UpdateState when no row exists for
// the supplied order_id. The handlers treat it as a "skip this
// event" signal (the saga hasn't started yet, or it already
// completed and was cleaned up).
var ErrNotFound = errors.New("saga: not found")

// PGRepo is the PostgreSQL-backed Repository. Stateless — all
// connections come from the supplied pool.
type PGRepo struct {
	pool *pgxpool.Pool
}

// NewPGRepo constructs a PGRepo backed by pool.
func NewPGRepo(pool *pgxpool.Pool) *PGRepo {
	return &PGRepo{pool: pool}
}

// Compile-time interface check.
var _ Repository = (*PGRepo)(nil)

// Insert writes a new order_sagas row in StateInitiated with
// expires_at set to NOW()+5min. The expires_at column is the source
// of truth for the watchdog sweep across restarts (sub-stage 3.9.c
// follow-up). A duplicate order_id surfaces as a unique-violation
// error from pgx; callers treat that as "already started".
func (r *PGRepo) Insert(ctx context.Context, s *Saga) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO order_sagas (order_id, state, items, total_cents, reservation_id, expires_at)
		 VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '5 minutes')`,
		s.OrderID, string(s.State), s.Items, s.TotalCents, s.ReservationID,
	)
	return err
}

// Get reads a saga row by order_id. Returns ErrNotFound when the
// row doesn't exist (so handlers can skip events that race ahead of
// the saga row).
func (r *PGRepo) Get(ctx context.Context, orderID string) (*Saga, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT order_id, state, items, total_cents, reservation_id
		   FROM order_sagas WHERE order_id = $1`, orderID)
	var (
		s     Saga
		state string
	)
	if err := row.Scan(&s.OrderID, &state, &s.Items, &s.TotalCents, &s.ReservationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	s.State = saga.State(state)
	return &s, nil
}

// UpdateState transitions the saga row's state column. Returns
// ErrNotFound when the row doesn't exist (vs. a real SQL error).
func (r *PGRepo) UpdateState(ctx context.Context, orderID string, state saga.State) error {
	ct, err := r.pool.Exec(ctx,
		`UPDATE order_sagas SET state = $1, updated_at = NOW() WHERE order_id = $2`,
		string(state), orderID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetReservationID updates the reservation_id column. Used when
// the saga is retried (a new reservation_id is generated and the
// old one overwritten).
func (r *PGRepo) SetReservationID(ctx context.Context, orderID, reservationID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE order_sagas SET reservation_id = $1, updated_at = NOW() WHERE order_id = $2`,
		reservationID, orderID)
	return err
}

// ListExpired returns sagas whose expires_at is in the past and
// whose state is non-terminal. Used by the cross-restart TTL sweep
// (services/saga/internal/watchdog) to find sagas that crashed
// before the in-process watchdog could fire their compensation.
//
// Terminal states ("completed", "compensated") are excluded so
// already-clean sagas are never re-compensated. Rows are returned
// ordered by expires_at ASC so the oldest abandoned sagas are
// reaped first; the caller enforces a hard cap with limit to
// bound tx size.
func (r *PGRepo) ListExpired(ctx context.Context, limit int) ([]*Saga, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT order_id, state, items, total_cents, reservation_id
		   FROM order_sagas
		  WHERE expires_at < NOW()
		    AND state NOT IN ('completed', 'compensated')
		  ORDER BY expires_at ASC
		  LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*Saga, 0, limit)
	for rows.Next() {
		var (
			s     Saga
			state string
		)
		if err := rows.Scan(&s.OrderID, &state, &s.Items, &s.TotalCents, &s.ReservationID); err != nil {
			return nil, err
		}
		s.State = saga.State(state)
		out = append(out, &s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}