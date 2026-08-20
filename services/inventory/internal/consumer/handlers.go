// Package consumer wires the Inventory Service's Kafka handler
// registry. The handlers translate saga-driven commands
// (StockReserveRequested, StockReleaseRequested) into Postgres
// state mutations and inventory_outbox events (StockReserved,
// StockReleased, StockReservationFailed).
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/platform/outbox"
	inventoryoutbox "github.com/t0pm1x/orderflow/services/inventory/internal/outbox"
	"github.com/t0pm1x/orderflow/services/inventory/internal/repository"
)

// Topic is the Kafka topic the Inventory Service publishes its
// StockReserved / StockReleased / StockReservationFailed events to.
// The Order Service consumes from this topic (matches the
// orderflow-events spec).
const Topic = "inventory-events"

// handlerDeps holds the data dependencies the consumer handlers
// need. Set once at startup by main.go via SetPool before any event
// is consumed. When nil (e.g. unit tests that don't wire the pool),
// the handlers log and ack-and-skip — preserving the stub behavior
// the pre-handler tests assert.
type handlerDeps struct {
	pool   *pgxpool.Pool
	repo   *repository.PGRepo
	writer *inventoryoutbox.PGWriter
}

// globalDeps holds the per-handler data dependencies. Accesses go
// through atomic.Pointer so SetPool in main and Registry in the
// consumer goroutine never race on the pointer write. (atomic.Pointer
// over a mutex because the consumer hot path needs to read this on
// every Kafka record — RWMutex would be fine but atomic.Pointer
// is lock-free.)
var globalDeps atomic.Pointer[handlerDeps]

// SetPool configures the consumer handlers' data dependencies. main.go
// must call this once after pgxpool is ready and before consumer.Start.
// Passing nil disables the handlers (they log and ack-and-skip,
// preserving the stub behavior used by tests).
func SetPool(pool *pgxpool.Pool) {
	if pool == nil {
		globalDeps.Store(nil)
		return
	}
	globalDeps.Store(&handlerDeps{
		pool:   pool,
		repo:   repository.NewPGRepo(pool),
		writer: inventoryoutbox.NewPGWriter(),
	})
}

// loadDeps is a small helper that returns the current globalDeps
// pointer or nil. Callers that need to access deps must do so through
// this helper so the atomic load is uniform.
func loadDeps() *handlerDeps { return globalDeps.Load() }

// suppress unused warning when this file's other helpers
// already import sync/atomic; the import is intentional and used
// for future sync requirements.
var _ = sync.Mutex{}

// Registry returns the Inventory Service's handler registry.
func Registry(logger *slog.Logger) pkgconsumer.HandlerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	return pkgconsumer.HandlerRegistry{
		"StockReserveRequested": stockReserveRequested(logger),
		"StockReleaseRequested": stockReleaseRequested(logger),
	}
}

// stockReserveRequested handles StockReserveRequested events by
// atomically reserving stock and emitting a StockReserved event
// (or StockReservationFailed when stock is insufficient).
func stockReserveRequested(logger *slog.Logger) pkgconsumer.Handler {
	return func(ctx context.Context, env *events.Envelope) error {
		deps := loadDeps()
		if deps == nil {
			logger.Info("orderflow-inventory received event (no DB pool)",
				"event_type", "StockReserveRequested",
				"event_id", env.EventID,
				"aggregate_id", env.AggregateID)
			return nil
		}
		var p struct {
			OrderID       string `json:"order_id"`
			SKU           string `json:"sku"`
			Quantity      int    `json:"quantity"`
			ReservationID string `json:"reservation_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			logger.Error("inventory StockReserveRequested: decode payload", "err", err)
			return err
		}

		payload, err := json.Marshal(map[string]any{
			"reservation_id": p.ReservationID,
			"order_id":       p.OrderID,
			"sku":            p.SKU,
			"quantity":       p.Quantity,
			"expires_at":     time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339),
		})
		if err != nil {
			return err
		}
		outRec := outbox.Record{
			EventID:       uuid.NewString(),
			EventType:     "StockReserved",
			AggregateID:   p.ReservationID,
			AggregateType: "Reservation",
			SchemaVersion: "1.0",
			Topic:         Topic,
			Payload:       payload,
			Headers:       map[string]string{},
		}
		// SAGA-3: ReserveStock uses the AggregateID (the
		// reservation_id) to record a per-reservation row in
		// stock_reservations, so a later ReleaseStock matches
		// only this saga's reservation.
		err = deps.repo.ReserveStock(ctx, p.SKU, p.Quantity, outRec)
		if errors.Is(err, repository.ErrInsufficientStock) {
			return emitStockReservationFailed(ctx, p.OrderID, p.SKU, "insufficient_stock", logger)
		}
		if errors.Is(err, repository.ErrNotFound) {
			return emitStockReservationFailed(ctx, p.OrderID, p.SKU, "sku_not_found", logger)
		}
		return err
	}
}

// stockReleaseRequested handles StockReleaseRequested events. The
// saga now publishes SKU and quantity on the release payload
// (sub-stage v1.1 fix; previously the stock_items.reserved counter
// leaked on every cancelled order). The handler releases the stock
// and emits a StockReleased event, both in one transaction so the
// counter and the downstream event commit (or roll back) together.
//
// SAGA-2: the StockReleased payload now includes order_id. Pre-fix
// inventory emitted StockReleased with AggregateID=reservation_id
// and no order_id field; the saga's StockReleasedHandler decoded
// OrderID="" and ran UPDATE WHERE order_id=” against the UUID
// column, raising SQLSTATE 22P02 — non-nil, retry-5x, DLQ. Every
// cancelled order blocked the saga consumer for 5 seconds. The
// saga handler also gains a defensive ack-skip on empty order_id
// (see services/saga/internal/consumer/handlers.go) so any pre-fix
// straggler events don't loop.
func stockReleaseRequested(logger *slog.Logger) pkgconsumer.Handler {
	return func(ctx context.Context, env *events.Envelope) error {
		deps := loadDeps()
		if deps == nil {
			logger.Info("orderflow-inventory received event (no DB pool)",
				"event_type", "StockReleaseRequested",
				"event_id", env.EventID,
				"aggregate_id", env.AggregateID)
			return nil
		}
		var p struct {
			OrderID       string `json:"order_id"`
			ReservationID string `json:"reservation_id"`
			SKU           string `json:"sku"`
			Quantity      int    `json:"quantity"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			logger.Error("inventory StockReleaseRequested: decode payload", "err", err)
			return err
		}
		if p.SKU == "" || p.Quantity <= 0 {
			// Old producers (pre-v1.1) didn't carry SKU/qty. Log
			// and ack-skip — operators see the stock_items.reserved
			// counter drift up via /metrics and can manually
			// reconcile after upgrading the producer.
			logger.Warn("inventory StockReleaseRequested: missing SKU or quantity (legacy producer?)",
				"event_id", env.EventID,
				"order_id", p.OrderID,
				"reservation_id", p.ReservationID)
			return nil
		}
		// SAGA-2: include order_id on the StockReleased payload so
		// the saga's StockReleasedHandler can UPDATE the saga row.
		payload, err := json.Marshal(map[string]any{
			"reservation_id": p.ReservationID,
			"order_id":       p.OrderID,
			"sku":            p.SKU,
			"quantity":       p.Quantity,
			"reason":         "order_cancelled",
		})
		if err != nil {
			return err
		}
		outRec := outbox.Record{
			EventID:       uuid.NewString(),
			EventType:     "StockReleased",
			AggregateID:   p.ReservationID,
			AggregateType: "Reservation",
			SchemaVersion: "1.0",
			Topic:         Topic,
			Payload:       payload,
			Headers:       map[string]string{},
		}
		if err := deps.repo.ReleaseStock(ctx, p.ReservationID, p.SKU, p.Quantity, outRec); err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				// SAGA-3: a reservation_id mismatch (the release
				// tries to release stock this saga never reserved)
				// surfaces as ErrNotFound. Ack-and-skip so the
				// poison event doesn't loop. Pre-fix this path
				// would silently oversell stock from another
				// order's reservation.
				logger.Warn("inventory StockReleaseRequested: reservation not found",
					"reservation_id", p.ReservationID,
					"sku", p.SKU, "order_id", p.OrderID)
				return nil
			}
			return err
		}
		return nil
	}
}

// emitStockReservationFailed writes a StockReservationFailed event
// in its own transaction (the original ReserveStock tx rolled back
// when stock was insufficient).
func emitStockReservationFailed(ctx context.Context, orderID, sku, reason string, logger *slog.Logger) error {
	payload, err := json.Marshal(map[string]any{
		"order_id": orderID,
		"sku":      sku,
		"reason":   reason,
	})
	if err != nil {
		return err
	}
	outRec := outbox.Record{
		EventID:       uuid.NewString(),
		EventType:     "StockReservationFailed",
		AggregateID:   orderID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         Topic,
		Payload:       payload,
		Headers:       map[string]string{},
	}
	deps := loadDeps()
	if deps == nil {
		// No deps wired — nothing we can do; let the consumer
		// mark the record for retry so it lands somewhere once
		// the pool is up.
		return errors.New("inventory: deps not wired")
	}
	if err := pgx.BeginFunc(ctx, deps.pool, func(tx pgx.Tx) error {
		return deps.writer.Append(ctx, tx, outRec)
	}); err != nil {
		logger.Error("inventory emit StockReservationFailed", "err", err)
		return err
	}
	return nil
}
