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

type Handler struct {
	pool   *pgxpool.Pool
	repo   *repository.PGRepo
	logger *slog.Logger
}

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
		"PaymentFailed":          h.PaymentFailed,
	}
}

func (h *Handler) StockReserved(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("reserved"))
}

func (h *Handler) StockReservationFailed(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("cancelled"))
}

func (h *Handler) OrderConfirmed(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("confirmed"))
}

func (h *Handler) OrderCancelled(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return h.updateState(ctx, p.OrderID, domain.OrderState("cancelled"))
}

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
	if _, err := h.pool.Exec(ctx,
		`UPDATE orders SET state = $1, updated_at = NOW() WHERE id = $2`,
		string(state), orderID,
	); err != nil {
		h.logger.Error("update order state failed", "order_id", orderID, "state", state, "err", err)
		return err
	}
	return nil
}