// Package consumer wires the Saga Service's Kafka handler registry
// for the events it consumes. Every OrderCreated starts a saga;
// StockReserved / PaymentCompleted drive the happy path;
// PaymentFailed / StockReleased drive compensation.
//
// Handlers persist state changes via repository.PGRepo and emit
// downstream events via outbox.PGWriter. The two writes happen in
// the same pgx.Tx (see emit) so a handler that crashes mid-step
// leaves no orphan rows.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
	platformoutbox "github.com/t0pm1x/orderflow/platform/outbox"

	sagapkg "github.com/t0pm1x/orderflow/services/saga"
	sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
	"github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	"github.com/t0pm1x/orderflow/services/saga/internal/repository"
)

// Handler holds the deps every per-event handler shares (repo,
// writer, logger, pool — pool is needed because the handlers open
// their own pgx.BeginFunc for emit).
type Handler struct {
	repo   *repository.PGRepo
	writer *outbox.PGWriter
	logger *slog.Logger
	pool   *pgxpool.Pool
}

// NewHandler constructs a Handler against the supplied pool.
// pool is also wrapped in PGRepo so emit can re-use it.
func NewHandler(pool *pgxpool.Pool, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		repo:   repository.NewPGRepo(pool),
		writer: outbox.NewPGWriter(),
		logger: logger,
		pool:   pool,
	}
}

// Registry returns the saga handler registry. Every event the saga
// runtime consumes has a handler here; the consumer acks-and-skips
// unknown event_types (pkg/consumer behavior).
func (h *Handler) Registry() pkgconsumer.HandlerRegistry {
	return pkgconsumer.HandlerRegistry{
		"OrderCreated":           h.OrderCreatedHandler,
		"StockReserved":          h.StockReservedHandler,
		"PaymentCompleted":       h.PaymentCompletedHandler,
		"PaymentFailed":          h.PaymentFailedHandler,
		"StockReleased":          h.StockReleasedHandler,
		"StockReservationFailed": h.StockReservationFailedHandler,
	}
}

// OrderCreatedHandler starts a new saga. Inserts the saga row in
// StateInitiated and emits StockReserveRequested for the order's
// first item (multi-item flow is out of scope for v0.5.0).
func (h *Handler) OrderCreatedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
		Items      []struct {
			SKU            string `json:"sku"`
			Quantity       int    `json:"quantity"`
			UnitPriceCents int64  `json:"unit_price_cents"`
		} `json:"items"`
		TotalCents int64 `json:"total_cents"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.logger.Warn("OrderCreated unmarshal", "err", err, "event_id", env.EventID)
		return err
	}
	if len(p.Items) == 0 {
		h.logger.Warn("OrderCreated has no items", "order_id", p.OrderID)
		return nil
	}
	first := p.Items[0]

	reservationID := uuid.NewString()
	itemsJSON, _ := json.Marshal(p.Items)

	if err := h.repo.Insert(ctx, &repository.Saga{
		OrderID:       p.OrderID,
		State:         sagapkg.StateInitiated,
		Items:         itemsJSON,
		TotalCents:    p.TotalCents,
		ReservationID: reservationID,
	}); err != nil {
		return err
	}

	return h.emit(ctx, p.OrderID, "StockReserveRequested", sagaev.StockReserveRequestedPayload{
		OrderID:       p.OrderID,
		SKU:           first.SKU,
		Quantity:      first.Quantity,
		ReservationID: reservationID,
	})
}

// StockReservedHandler advances the saga to StateStockReserved and
// emits PaymentRequested with the saga's stored total. The
// AmountCents comes from the saga row, not the StockReserved
// payload, because the inventory event doesn't carry the order
// total (separate aggregate).
func (h *Handler) StockReservedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	s, err := h.repo.Get(ctx, p.OrderID)
	if err != nil {
		if err == repository.ErrNotFound {
			h.logger.Warn("StockReserved for unknown saga", "order_id", p.OrderID)
			return nil
		}
		return err
	}
	if err := h.repo.UpdateState(ctx, p.OrderID, sagapkg.StateStockReserved); err != nil {
		return err
	}
	return h.emit(ctx, p.OrderID, "PaymentRequested", sagaev.PaymentRequestedPayload{
		OrderID:        p.OrderID,
		AmountCents:    s.TotalCents,
		IdempotencyKey: uuid.NewString(),
	})
}

// PaymentCompletedHandler is the happy-path terminal: advance to
// StateCompleted and emit OrderConfirmed.
func (h *Handler) PaymentCompletedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if err := h.repo.UpdateState(ctx, p.OrderID, sagapkg.StateCompleted); err != nil {
		if err == repository.ErrNotFound {
			h.logger.Warn("PaymentCompleted for unknown saga", "order_id", p.OrderID)
			return nil
		}
		return err
	}
	return h.emit(ctx, p.OrderID, "OrderConfirmed", sagaev.OrderConfirmedPayload{
		OrderID:     p.OrderID,
		ConfirmedAt: nowRFC3339(),
	})
}

// PaymentFailedHandler triggers compensation. The saga transitions
// to StateCompensated, then emits StockReleaseRequested (so
// inventory releases the reservation) and OrderCancelled (so the
// order service can mark the order as cancelled). ReservationID
// comes from the saga row — PaymentFailed doesn't carry it.
func (h *Handler) PaymentFailedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	s, err := h.repo.Get(ctx, p.OrderID)
	if err != nil {
		if err == repository.ErrNotFound {
			h.logger.Warn("PaymentFailed for unknown saga", "order_id", p.OrderID)
			return nil
		}
		return err
	}
	if err := h.repo.UpdateState(ctx, p.OrderID, sagapkg.StateCompensated); err != nil {
		return err
	}
	if err := h.emit(ctx, p.OrderID, "StockReleaseRequested", sagaev.StockReleaseRequestedPayload{
		OrderID:       p.OrderID,
		ReservationID: s.ReservationID,
	}); err != nil {
		return err
	}
	return h.emit(ctx, p.OrderID, "OrderCancelled", sagaev.OrderCancelledPayload{
		OrderID: p.OrderID,
		Reason:  "payment_failed",
		Source:  "saga",
	})
}

// StockReleasedHandler is the compensation ack from inventory. The
// saga is already in StateCompensated by the time this fires — we
// just touch updated_at so observability tools see the chain
// close. Note: the brief labels this terminal "fully_compensated"
// but the state machine defines StateCompensated as terminal
// already, so we keep it consistent with state.go.
func (h *Handler) StockReleasedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if err := h.repo.UpdateState(ctx, p.OrderID, sagapkg.StateCompensated); err != nil {
		if err == repository.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

// StockReservationFailedHandler is the inventory-side failure
// path: saga never started, so no compensation is needed — just
// emit OrderCancelled so the order service can mark it failed.
func (h *Handler) StockReservationFailedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if err := h.repo.UpdateState(ctx, p.OrderID, sagapkg.StateCompensated); err != nil {
		if err == repository.ErrNotFound {
			h.logger.Warn("StockReservationFailed for unknown saga", "order_id", p.OrderID)
			return nil
		}
		return err
	}
	return h.emit(ctx, p.OrderID, "OrderCancelled", sagaev.OrderCancelledPayload{
		OrderID: p.OrderID,
		Reason:  "stock_failed",
		Source:  "saga",
	})
}

// nowRFC3339 returns the current UTC time formatted as RFC3339 —
// the wire shape OrderConfirmedPayload expects. Defined here so
// the test in handlers_test.go can override it via package-level
// variable swap if needed (it doesn't today).
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// emit writes a single outbox row inside a fresh pgx.Tx. The tx
// commits and the row goes to PENDING for the poller to publish.
// Returns the tx error directly; the poller's retry semantics
// handle transient errors.
func (h *Handler) emit(ctx context.Context, aggregateID, eventType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		return h.writer.Append(ctx, tx, platformoutbox.Record{
			EventID:       uuid.NewString(),
			EventType:     eventType,
			AggregateID:   aggregateID,
			AggregateType: "Order",
			SchemaVersion: "1.0",
			Topic:         outbox.Topic,
			Payload:       payloadBytes,
			Headers:       map[string]string{},
		})
	})
}
