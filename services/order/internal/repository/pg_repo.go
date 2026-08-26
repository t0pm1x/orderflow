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

// ErrNotFound is an alias for domain.ErrOrderNotFound so callers
// who import only repository (and not domain) keep working. Lives
// here for backwards compatibility — new code should use
// domain.ErrOrderNotFound.
var ErrNotFound = domain.ErrOrderNotFound

// ErrAlreadyTerminal is an alias for domain.ErrOrderAlreadyTerminal.
// The api handler maps it to 409 Conflict (audit WEB-3-CANCEL-409).
var ErrAlreadyTerminal = domain.ErrOrderAlreadyTerminal

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
// a no-op.
//
// WEB-3-CANCEL-409 fix: RowsAffected==0 is split into ErrNotFound
// (row doesn't exist) and ErrAlreadyTerminal (row exists but is
// already terminal). The pre-fix code collapsed both into
// ErrNotFound which the handler mapped to 404 — so a previously
// cancelled order looked the same to the operator as a typo'd id.
// Now the handler maps each to a distinct status code (404 vs
// 409) and the BFF surfaces a distinct banner message.
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
			return fmt.Errorf("update order: %v", err)
		}
		if tag.RowsAffected() == 0 {
			// Distinguish "row doesn't exist" (404) from "row is
			// already terminal" (409). The lookup uses a SELECT
			// rather than an EXISTS-with-state so we report
			// terminal-state transitions to the operator instead
			// of folding them into a silent "no-op".
			var state string
			err := tx.QueryRow(ctx, `SELECT state FROM orders WHERE id = $1`, id).Scan(&state)
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrOrderNotFound
			}
			if err != nil {
				return fmt.Errorf("lookup order state: %w", err)
			}
			// state is one of confirmed/cancelled/failed by
			// construction (the UPDATE's WHERE clause filtered
			// these out, and we only get RowsAffected=0 if all
			// other states also matched zero rows). The error
			// string carries the state for the operator.
			return fmt.Errorf("%w: state=%s", domain.ErrOrderAlreadyTerminal, state)
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
//
// The SELECT includes every column Get reads (last_four,
// completed_at) so the BFF / payments-sim view sees the same row
// shape from /v1/orders as from /v1/orders/{id}. Pre-fix List
// omitted last_four and completed_at, so the payments-sim's hidden
// last_four input on the force-webhook form was always empty
// (the upstream errorCode() fallback then always picked
// "network_error" instead of the card-derived reason) and the
// order-detail's "completed" timestamp never rendered for terminal
// orders surfaced via the list endpoint.
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
			`SELECT id, customer_id, items, state, total_cents,
			        created_at, updated_at, completed_at, last_four
			   FROM orders
			  ORDER BY created_at DESC
			  LIMIT $1`, limit)
	} else {
		rows, err = r.pool.Query(ctx,
			`SELECT id, customer_id, items, state, total_cents,
			        created_at, updated_at, completed_at, last_four
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
			lastFour  sql.NullString
		)
		if err := rows.Scan(&o.ID, &o.CustomerID, &itemsJSON, &st, &o.TotalCents,
			&o.CreatedAt, &o.UpdatedAt, &o.CompletedAt, &lastFour); err != nil {
			return nil, err
		}
		o.State = domain.OrderState(st)
		if lastFour.Valid {
			o.LastFour = lastFour.String
		}
		if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
			return nil, fmt.Errorf("unmarshal items: %w", err)
		}
		out = append(out, &o)
	}
	return out, rows.Err()
}
