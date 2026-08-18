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
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO orders (id, customer_id, items, state, total_cents, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
			o.ID, o.CustomerID, itemsJSON, string(o.State), o.TotalCents.Cents(),
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
		`SELECT id, customer_id, items, state, total_cents
		   FROM orders WHERE id = $1`, id)
	var (
		o         domain.Order
		itemsJSON []byte
		state     string
	)
	if err := row.Scan(&o.ID, &o.CustomerID, &itemsJSON, &state, &o.TotalCents); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("order not found: %w", err)
		}
		return nil, err
	}
	o.State = domain.OrderState(state)
	if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
		return nil, fmt.Errorf("unmarshal items: %w", err)
	}
	return &o, nil
}

// List returns up to limit orders whose state matches the filter,
// ordered by created_at DESC. A non-positive or excessive limit
// is clamped to 50 to keep the call cheap.
func (r *PGRepo) List(ctx context.Context, state domain.OrderState, limit int) ([]*domain.Order, error) {
	// Clamp limit to match the handler's allowed range (1..500).
	// Non-positive or excessive values fall back to the default 50.
	// Keep both bounds in sync to avoid handler-side has_more
	// drift on the response contract.
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, customer_id, items, state, total_cents
		   FROM orders
		  WHERE state = $1
		  ORDER BY created_at DESC
		  LIMIT $2`, string(state), limit)
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
		if err := rows.Scan(&o.ID, &o.CustomerID, &itemsJSON, &st, &o.TotalCents); err != nil {
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
