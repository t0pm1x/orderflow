// Package repository contains the Inventory Service's data-access
// implementations. PGRepo is the production implementation of the
// Repository interface, backed by a pgxpool against the schema
// defined in services/inventory/migrations/0001_init.sql.
//
// ReserveStock and ReleaseStock are atomic: the stock_items row
// mutation and the inventory_outbox row commit (or roll back) in
// the same transaction. The outbox INSERT delegates to
// services/inventory/internal/outbox.PGWriter so the canonical
// outbox INSERT lives in exactly one place (matches the Order
// Service pattern).
package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t0pm1x/orderflow/platform/outbox"

	"github.com/t0pm1x/orderflow/services/inventory/internal/model"
	svcoutbox "github.com/t0pm1x/orderflow/services/inventory/internal/outbox"
)

// Repository is the inventory data-access seam. GetStock is a
// read-only lookup; ReserveStock and ReleaseStock atomically
// mutate stock_items and append an outbox event in the same tx.
type Repository interface {
	GetStock(ctx context.Context, sku string) (*model.Stock, error)
	// ReserveStock atomically decrements Available, increments
	// Reserved, and appends ev to inventory_outbox. Returns
	// ErrInsufficientStock when Available < qty,
	// ErrStaleVersion when another tx bumped Version between
	// the SELECT FOR UPDATE and the UPDATE.
	ReserveStock(ctx context.Context, sku string, qty int, ev outbox.Record) error
	// ReleaseStock atomically increments Available, decrements
	// Reserved, and appends ev to inventory_outbox.
	ReleaseStock(ctx context.Context, sku string, qty int, ev outbox.Record) error
}

// Compile-time check that PGRepo satisfies Repository.
var _ Repository = (*PGRepo)(nil)

// Errors surfaced by PGRepo. They wrap nothing so callers can
// match with errors.Is.
var (
	ErrInsufficientStock = errors.New("inventory: insufficient stock")
	ErrStaleVersion      = errors.New("inventory: stale version")
	ErrNotFound          = errors.New("inventory: stock item not found")
)

// PGRepo is the Postgres-backed implementation of Repository.
type PGRepo struct {
	pool   *pgxpool.Pool
	writer *svcoutbox.PGWriter
}

// NewPGRepo constructs a PGRepo backed by pool. The outbox writer
// is stateless, so it is created once and reused (matches the
// Order Service pattern).
func NewPGRepo(pool *pgxpool.Pool) *PGRepo {
	return &PGRepo{pool: pool, writer: svcoutbox.NewPGWriter()}
}

// GetStock reads a single stock_items row by sku. Returns an
// error wrapping ErrNotFound when no row exists; the chi handler
// translates that into a 404.
func (r *PGRepo) GetStock(ctx context.Context, sku string) (*model.Stock, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT sku, available, reserved, version, updated_at
		   FROM stock_items WHERE sku = $1`, sku)
	var s model.Stock
	if err := row.Scan(&s.SKU, &s.Available, &s.Reserved, &s.Version, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, sku)
		}
		return nil, err
	}
	return &s, nil
}

// ReserveStock atomically decrements available, increments
// reserved, and appends an outbox event. SELECT FOR UPDATE pins
// the row for the duration of the tx so concurrent reserves
// serialize; the WHERE version=$expected guard catches a stale
// read between SELECT and UPDATE.
func (r *PGRepo) ReserveStock(ctx context.Context, sku string, qty int, ev outbox.Record) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		var available, version int
		err := tx.QueryRow(ctx,
			`SELECT available, version FROM stock_items WHERE sku = $1 FOR UPDATE`, sku).
			Scan(&available, &version)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", ErrNotFound, sku)
			}
			return err
		}
		if available < qty {
			return ErrInsufficientStock
		}
		ct, err := tx.Exec(ctx,
			`UPDATE stock_items
			    SET available = available - $1,
			        reserved  = reserved + $2,
			        version   = version + 1
			  WHERE sku = $3 AND version = $4`,
			qty, qty, sku, version)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return ErrStaleVersion
		}
		return r.writer.Append(ctx, tx, ev)
	})
}

// ReleaseStock atomically increments available, decrements
// reserved, and appends an outbox event. Missing sku surfaces as
// ErrNotFound; concurrent modification is not possible here
// because we do not pin a specific version on the UPDATE (callers
// that need stricter guarantees should re-read and retry).
func (r *PGRepo) ReleaseStock(ctx context.Context, sku string, qty int, ev outbox.Record) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx,
			`UPDATE stock_items
			    SET available = available + $1,
			        reserved  = reserved - $2,
			        version   = version + 1
			  WHERE sku = $3`,
			qty, qty, sku)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s", ErrNotFound, sku)
		}
		return r.writer.Append(ctx, tx, ev)
	})
}
