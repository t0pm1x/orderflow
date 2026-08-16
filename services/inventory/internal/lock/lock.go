// Package lock provides version-checked UPDATE primitives for the
// inventory stock_items table. Combined with the model.Stock
// aggregate's optimistic-lock token, this gives us:
//
//   - atomic decrement/increment on available vs. reserved
//   - rejection of updates whose version was bumped by a concurrent
//     transaction (model.ErrStaleVersion)
//   - distinction between insufficient stock and concurrent
//     modification in a single round trip
//
// All operations take an outbox.DBTX so the caller wraps the call in
// the same tx that writes the inventory_outbox row (3.6.d).
package lock

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t0pm1x/orderflow/platform/outbox"

	"github.com/t0pm1x/orderflow/services/inventory/internal/model"
)

// Table is the inventory stock table name.
const Table = "stock_items"

// SQL fragments for the version-checked UPDATE.
//
//go:embed reserve.sql
var reserveSQL string

//go:embed release.sql
var releaseSQL string

//go:embed checkStock.sql
var checkStockSQL string

// Locker is the contract for version-checked stock mutations.
type Locker interface {
	// Reserve atomically decrements Available by qty, increments
	// Reserved, and bumps Version, but only if:
	//   1. a row exists for sku,
	//   2. its version matches expectedVersion,
	//   3. available >= qty.
	// Returns the updated Stock on success. Returns
	// model.ErrStaleVersion if (1) holds but (2) does not.
	// Returns model.ErrInsufficientStock if (1) and (2) hold but
	// (3) does not.
	Reserve(ctx context.Context, tx outbox.DBTX, sku string, qty int, expectedVersion int64) (*model.Stock, error)

	// Release atomically increments Available by qty, decrements
	// Reserved, and bumps Version, but only if:
	//   1. a row exists for sku,
	//   2. its version matches expectedVersion,
	//   3. reserved >= qty.
	Release(ctx context.Context, tx outbox.DBTX, sku string, qty int, expectedVersion int64) (*model.Stock, error)
}

// PGLocker is the Postgres implementation of Locker.
type PGLocker struct{}

// NewPGLocker constructs a PGLocker.
func NewPGLocker() *PGLocker { return &PGLocker{} }

// Compile-time interface check.
var _ Locker = (*PGLocker)(nil)

// Reserve implements Locker.
func (l *PGLocker) Reserve(ctx context.Context, tx outbox.DBTX, sku string, qty int, expectedVersion int64) (*model.Stock, error) {
	stock, err := scanStock(tx.QueryRow(ctx, reserveSQL, sku, qty, expectedVersion))
	if err == nil {
		return stock, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return differentiate(ctx, tx, sku, expectedVersion, true, qty)
}

// Release implements Locker.
func (l *PGLocker) Release(ctx context.Context, tx outbox.DBTX, sku string, qty int, expectedVersion int64) (*model.Stock, error) {
	stock, err := scanStock(tx.QueryRow(ctx, releaseSQL, sku, qty, expectedVersion))
	if err == nil {
		return stock, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return differentiate(ctx, tx, sku, expectedVersion, false, qty)
}

// scanStock reads a stock row from r. Returns pgx.ErrNoRows for an
// empty result set; all other errors are propagated.
func scanStock(r pgx.Row) (*model.Stock, error) {
	var s model.Stock
	var updatedAt time.Time
	if err := r.Scan(&s.SKU, &s.Available, &s.Reserved, &s.Version, &updatedAt); err != nil {
		return nil, err
	}
	s.UpdatedAt = updatedAt
	return &s, nil
}

// differentiate resolves the cause of a zero-row UPDATE. We re-read
// the row and compare version+stock to expectedVersion+qty to pick
// the right error.
func differentiate(ctx context.Context, tx outbox.DBTX, sku string, expectedVersion int64, checkAvailable bool, qty int) (*model.Stock, error) {
	var currentVersion int64
	var currentStock int
	err := tx.QueryRow(ctx, checkStockSQL, sku).Scan(&currentVersion, &currentStock)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Row missing entirely: treat as stale so the caller
			// surfaces a 409/404 instead of a generic 500.
			return nil, model.ErrStaleVersion
		}
		return nil, err
	}
	if currentVersion != expectedVersion {
		return nil, model.ErrStaleVersion
	}
	if checkAvailable && currentStock < qty {
		return nil, model.ErrInsufficientStock
	}
	if !checkAvailable && currentStock < qty {
		// reserved < qty: inconsistent state; surface as stale.
		return nil, model.ErrStaleVersion
	}
	return nil, model.ErrStaleVersion
}
