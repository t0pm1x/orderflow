// Package consumer wires the Order Service's Kafka handler
// registry for the events it consumes. Handlers translate inventory
// / saga events into updates on the orders.state column; the full
// event flow is documented in docs/superpowers/specs/orderflow-events.md.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/order/internal/domain"
	"github.com/t0pm1x/orderflow/services/order/internal/repository"
)

// Handler implements the Order Service's consumer-side event handling.
// Each method updates the orders.state column in response to a saga
// or inventory event.
type Handler struct {
	pool   *pgxpool.Pool
	repo   *repository.PGRepo
	logger *slog.Logger
}

// NewHandler constructs a Handler that owns its own PGRepo over pool.
func NewHandler(pool *pgxpool.Pool, logger *slog.Logger) *Handler {
	return &Handler{
		pool:   pool,
		repo:   repository.NewPGRepo(pool),
		logger: logger,
	}
}

// Registry returns the Order Service's handler registry. Every
// entry updates the orders.state column in response to a saga or
// inventory event.
func (h *Handler) Registry() pkgconsumer.HandlerRegistry {
	return pkgconsumer.HandlerRegistry{
		"StockReserved":          h.StockReserved,
		"StockReservationFailed": h.StockReservationFailed,
		"OrderConfirmed":         h.OrderConfirmed,
		"OrderCancelled":         h.OrderCancelled,
		"PaymentCompleted":       h.PaymentCompleted,
		"PaymentFailed":          h.PaymentFailed,
	}
}

// StockReserved handles StockReserved events by transitioning the
// referenced order to the reserved state.
func (h *Handler) StockReserved(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("reserved"))
}

// StockReservationFailed handles StockReservationFailed events by
// transitioning the referenced order to the cancelled state.
func (h *Handler) StockReservationFailed(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("cancelled"))
}

// OrderConfirmed handles OrderConfirmed events by transitioning the
// referenced order to the confirmed state.
func (h *Handler) OrderConfirmed(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("confirmed"))
}

// OrderCancelled handles OrderCancelled events by transitioning the
// referenced order to the cancelled state.
func (h *Handler) OrderCancelled(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("cancelled"))
}

// PaymentCompleted handles PaymentCompleted events by transitioning
// the referenced order to the confirmed state. Independent path from
// the saga's OrderConfirmed emit — covers the cross-topic race where
// PaymentCompleted arrives before the saga's OrderCreatedHandler has
// committed the order_sagas row, which causes the saga's
// PaymentCompletedHandler to silently skip with ErrNotFound. Idempotent
// via updateState's terminal-state WHERE clause; the saga's eventual
// OrderConfirmed becomes a no-op UPDATE.
func (h *Handler) PaymentCompleted(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("confirmed"))
}

// PaymentFailed handles PaymentFailed events by transitioning the
// referenced order to the cancelled state.
func (h *Handler) PaymentFailed(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("cancelled"))
}

func (h *Handler) updateState(ctx context.Context, orderID string, state domain.OrderState) error {
	if h.pool == nil {
		return errors.New("order consumer handler: pool not initialized")
	}
	// Guard all terminal states (confirmed, cancelled, AND
	// failed). The P1-#2 audit finding was that the pre-v1.1.1
	// SQL excluded only 'confirmed' and 'cancelled', letting a
	// late 'OrderConfirmed' for a 'failed' order resurrect the
	// order. StateFailed is reachable via the saga TTL sweep,
	// so it must be protected too.
	//
	// COMPLETED-AT-ON-CONSUMER fix: when the new state is terminal
	// (confirmed/cancelled/failed) we also stamp completed_at so
	// the order-detail "completed {{time}}" line in the BFF renders.
	// Pre-fix the consumer only updated state + updated_at, so a
	// saga-driven OrderConfirmed / OrderCancelled / saga-TTL
	// StateFailed all left completed_at NULL — visible to the
	// operator as "the order is confirmed but no completion
	// timestamp". The PGRepo.Cancel path already sets completed_at;
	// the consumer path is now consistent.
	var stmt string
	switch state {
	case domain.StateConfirmed, domain.StateCancelled, domain.StateFailed:
		stmt = `UPDATE orders
		           SET state = $1, updated_at = NOW(), completed_at = NOW()
		         WHERE id = $2
		           AND state NOT IN ('confirmed', 'cancelled', 'failed')`
	default:
		stmt = `UPDATE orders
		           SET state = $1, updated_at = NOW()
		         WHERE id = $2
		           AND state NOT IN ('confirmed', 'cancelled', 'failed')`
	}
	if _, err := h.pool.Exec(ctx, stmt, string(state), orderID); err != nil {
		h.logger.Error("update order state failed", "order_id", orderID, "state", state, "err", err)
		return err
	}
	return nil
}
