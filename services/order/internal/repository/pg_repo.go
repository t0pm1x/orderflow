// Package repository contains the Order Service's data-access
// implementations. PGRepo is the production implementation of
// api.Repository, backed by a pgxpool against the schema defined in
// services/order/migrations/0001_init.sql.
//
// Insert is atomic: the orders row and every outbox row are written
// in the same transaction, so the poller (3.7) and the business
// state can never disagree. The outbox half delegates to
// services/order/internal/outbox.PGWriter so the canonical outbox
// INSERT lives in exactly one place.
//
// Cancel is also atomic: state transition to 'cancelled' and the
// OrderCancelled outbox row commit together. A missing or already-
// terminal order surfaces as ErrNotFound so the handler can map it
// to a 404.
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/outbox"
	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/api"
	"github.com/t0pm1x/orderflow/services/order/internal/domain"
	svcoutbox "github.com/t0pm1x/orderflow/services/order/internal/outbox"
)

// Table is the Order Service's orders table name. The same string
// is referenced in services/order/migrations/0001_init.sql.
const Table = "orders"

// ErrNotFound is returned by Get/Cancel when the order does not
// exist or cannot be cancelled (already terminal). api.Repository
// callers translate this into a 404.
var ErrNotFound = errors.New("order not found")

// PGRepo is the PostgreSQL-backed implementation of api.Repository.
type PGRepo struct {
	pool   *pgxpool.Pool
	writer *svcoutbox.PGWriter
}

// NewPGRepo constructs a PGRepo backed by pool. The outbox writer
// is stateless, so it is created once and reused.
func NewPGRepo(pool *pgxpool.Pool) *PGRepo {
	return &PGRepo{pool: pool, writer: svcoutbox.NewPGWriter()}
}

// Compile-time assertion that PGRepo satisfies api.Repository.
var _ api.Repository = (*PGRepo)(nil)

// Insert writes the order row and every supplied outbox Record in
// a single transaction. Either all rows commit or none do. The
// caller's context (typically the HTTP request) is honored by pgx
// so a cancelled request aborts the write before commit.
func (r *PGRepo) Insert(ctx context.Context, o *domain.Order, events ...outbox.Record) error {
	itemsJSON, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}
	// last_four is NULLABLE on the orders column (see
	// migrations/0007_orders_last_four.sql); an empty Payment block
	// in the submit body is a valid v1.x-compatible call so we
	// must turn "" into a SQL NULL rather than an empty string.
	// Using sql.NullString here keeps the logic local to the repo
	// rather than leaking into the domain types.
	var lastFour sql.NullString
	if o.LastFour != "" {
		lastFour = sql.NullString{String: o.LastFour, Valid: true}
	}
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO orders (id, customer_id, items, state, total_cents, last_four, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())`,
			o.ID, o.CustomerID, itemsJSON, string(o.State), o.TotalCents.Cents(), lastFour,
		); err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		for _, ev := range events {
			if err := r.writer.Append(ctx, tx, ev); err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}
		}
		return nil
	})
}

// Get loads a single order by id. Returns an error wrapping
// pgx.ErrNoRows when the order does not exist; callers (the
// handler) translate that into a 404.
func (r *PGRepo) Get(ctx context.Context, id types.OrderID) (*domain.Order, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, customer_id, items, state, total_cents,
		        created_at, updated_at, completed_at, last_four
		   FROM orders WHERE id = $1`, id)
	var (
		o         domain.Order
		itemsJSON []byte
		state     string
		lastFour  sql.NullString
	)
	if err := row.Scan(
		&o.ID, &o.CustomerID, &itemsJSON, &state, &o.TotalCents,
		&o.CreatedAt, &o.UpdatedAt, &o.CompletedAt, &lastFour,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("order not found: %w", err)
		}
		return nil, err
	}
	o.State = domain.OrderState(state)
	if lastFour.Valid {
		o.LastFour = lastFour.String
	}
	if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
		return nil, fmt.Errorf("unmarshal items: %w", err)
	}
	return &o, nil
}

// Cancel transitions the order to StateCancelled and writes an
// OrderCancelled outbox row in the same transaction. The state guard
// `state NOT IN ('confirmed','cancelled','failed')` matches the
// P1-#2 consumer-side SQL (services/order/internal/consumer/handlers.go:126)
// so a Cancel request against an already-terminal or unknown id is
// a no-op and returns ErrNotFound (no outbox row emitted).
//
// Atomicity is critical: if the UPDATE rolled back but the outbox
// row committed, inventory would release stock against an order
// that never actually transitioned.
func (r *PGRepo) Cancel(ctx context.Context, id types.OrderID) error {
	payload, err := json.Marshal(struct {
		OrderID string `json:"order_id"`
		Reason  string `json:"reason"`
		Source  string `json:"source"`
	}{
		OrderID: id.String(),
		Reason:  "user_request",
		Source:  "user",
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	rec := outbox.Record{
		EventID:       uuid.NewString(),
		EventType:     "OrderCancelled",
		AggregateID:   id.String(),
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       payload,
	}
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE orders
			    SET state = 'cancelled',
			        updated_at = NOW(),
			        completed_at = NOW()
			  WHERE id = $1
			    AND state NOT IN ('confirmed', 'cancelled', 'failed')`, id)
		if err != nil {
			return fmt.Errorf("update order: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		if err := r.writer.Append(ctx, tx, rec); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
		return nil
	})
}

// List returns up to limit orders whose state matches the filter,
// ordered by created_at DESC. A non-positive or excessive limit
// is clamped to 50 to keep the call cheap. An empty state is
// treated as "no filter" so callers (notably the web inventory
// page) that want all states get all states; pre-fix this
// matched `WHERE state = ”` which returned zero rows.
func (r *PGRepo) List(ctx context.Context, state domain.OrderState, limit int) ([]*domain.Order, error) {
	// Clamp limit to match the handler's allowed range (1..500).
	// Non-positive or excessive values fall back to the default 50.
	// Keep both bounds in sync to avoid handler-side has_more
	// drift on the response contract.
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var (
		rows pgx.Rows
		err  error
	)
	if state == "" {
		rows, err = r.pool.Query(ctx,
			`SELECT id, customer_id, items, state, total_cents, created_at, updated_at
			   FROM orders
			  ORDER BY created_at DESC
			  LIMIT $1`, limit)
	} else {
		rows, err = r.pool.Query(ctx,
			`SELECT id, customer_id, items, state, total_cents, created_at, updated_at
			   FROM orders
			  WHERE state = $1
			  ORDER BY created_at DESC
			  LIMIT $2`, string(state), limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*domain.Order
	for rows.Next() {
		var (
			o         domain.Order
			itemsJSON []byte
			st        string
		)
		if err := rows.Scan(&o.ID, &o.CustomerID, &itemsJSON, &st, &o.TotalCents, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.State = domain.OrderState(st)
		if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
			return nil, fmt.Errorf("unmarshal items: %w", err)
		}
		out = append(out, &o)
	}
	return out, rows.Err()
}
