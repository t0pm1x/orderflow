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
	// Reserved, and appends ev to inventory_outbox. SAGA-3: keyed
	// on reservationID so the release only matches the saga's own
	// reservation (closes cross-order stock theft). Returns
	// ErrNotFound when no stock_reservations row exists for
	// reservationID.
	ReleaseStock(ctx context.Context, reservationID, sku string, qty int, ev outbox.Record) error
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
//
// SAGA-3: also INSERTs a stock_reservations row in the same tx so
// a later ReleaseStock can match by reservation_id (rather than
// blindly decrementing any reserved counter that happens to be >=
// qty for the SKU). This closes the cross-order stock-theft hole:
// pre-fix, ReleaseStock keyed on sku+qty only and could decrement
// another order's reservation.
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
		// SAGA-3: record the reservation so a later release can
		// match by reservation_id (the per-reservation proof of
		// ownership). Pull reservation_id off the outbox Record's
		// AggregateID — the StockReserveRequested handler sets
		// AggregateID = reservation_id by convention.
		if _, err := tx.Exec(ctx,
			`INSERT INTO stock_reservations (reservation_id, sku, quantity, order_id)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (reservation_id) DO NOTHING`,
			ev.AggregateID, sku, qty, orderIDFromEvent(ev),
		); err != nil {
			return fmt.Errorf("insert stock_reservations: %w", err)
		}
		return r.writer.Append(ctx, tx, ev)
	})
}

// orderIDFromEvent extracts the order_id from an outbox.Record's
// payload by cheap string matching. Returns "" if the payload
// shape is unexpected. Used only by ReserveStock to persist the
// order_id on the new stock_reservations row; the join key for
// release is still reservation_id, so an empty order_id here just
// means the diagnostic join on order_id returns nothing.
func orderIDFromEvent(ev outbox.Record) string {
	if len(ev.Payload) == 0 {
		return ""
	}
	// Cheap scan for `"order_id":"<uuid>"` — avoids pulling in
	// encoding/json for what is a hot-path observation.
	const k = `"order_id":"`
	i := indexOf(ev.Payload, k)
	if i < 0 {
		return ""
	}
	start := i + len(k)
	end := indexOf(ev.Payload[start:], `"`)
	if end < 0 {
		return ""
	}
	return string(ev.Payload[start : start+end])
}

// indexOf is a hand-rolled, allocation-free byte search. Equivalent
// to strings.Index but avoids the strings import for what is a
// tight inner-loop helper.
func indexOf(haystack []byte, needle string) int {
	if len(needle) == 0 {
		return 0
	}
	n := len(needle)
	first := needle[0]
	for i := 0; i+n <= len(haystack); i++ {
		if haystack[i] != first {
			continue
		}
		if string(haystack[i:i+n]) == needle {
			return i
		}
	}
	return -1
}

// ReleaseStock atomically increments available, decrements
// reserved, and appends an outbox event. The release is keyed on
// the supplied reservation_id: a missing stock_reservations row is
// ErrNotFound, and the UPDATE is only performed if the DELETE
// returns a row. This is the SAGA-3 fix for cross-order stock
// theft — pre-fix, ReleaseStock keyed on sku+qty only, so a
// release for items[1..n] (never reserved by this saga) could
// match some other concurrent order's reserved counter and
// oversell. Post-fix, a release that doesn't find its
// reservation row is a no-op.
//
// SAGA-3 also retains the reserved >= qty guard so a buggy
// producer emitting a larger release than the original reservation
// cannot drive reserved negative (which would silently inflate
// available on every subsequent release — a permanent stock-counter
// corruption).
//
// Missing reservation OR over-release surfaces as ErrNotFound
// (callers (e.g. the inventory consumer) ack-and-skip on
// ErrNotFound so the poison event doesn't loop on the consumer).
func (r *PGRepo) ReleaseStock(ctx context.Context, reservationID, sku string, qty int, ev outbox.Record) error {
	if qty <= 0 {
		return fmt.Errorf("inventory: release qty must be positive (got %d)", qty)
	}
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		// SAGA-3: claim the reservation by deleting it. If 0 rows
		// are affected, this reservation was never created (or
		// already released) — refuse the release so a release for
		// items[1..n] can't decrement another order's reserved
		// counter.
		var reservedQty int
		err := tx.QueryRow(ctx,
			`DELETE FROM stock_reservations
			   WHERE reservation_id = $1
			   RETURNING quantity`,
			reservationID,
		).Scan(&reservedQty)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: reservation_id=%s", ErrNotFound, reservationID)
			}
			return fmt.Errorf("delete stock_reservations: %w", err)
		}
		if reservedQty != qty {
			// Quantity mismatch between the event and the
			// recorded reservation. Roll back the DELETE by
			// aborting the tx; the caller will retry or DLQ.
			return fmt.Errorf("inventory: release qty %d != reservation qty %d for reservation_id=%s",
				qty, reservedQty, reservationID)
		}
		ct, err := tx.Exec(ctx,
			`UPDATE stock_items
			    SET available = available + $1,
			        reserved  = reserved - $2,
			        version   = version + 1
			  WHERE sku = $3
			    AND reserved >= $2`,
			qty, qty, sku)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s (reserved < qty)", ErrNotFound, sku)
		}
		return r.writer.Append(ctx, tx, ev)
	})
}
