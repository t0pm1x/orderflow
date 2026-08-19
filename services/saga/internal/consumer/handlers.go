// Package consumer wires the Saga Service's Kafka handler registry
// for the events it consumes. Every OrderCreated starts a saga;
// StockReserved / PaymentCompleted drive the happy path;
// PaymentFailed / StockReleased drive compensation.
//
// Handlers persist state changes via repository.PGRepo and emit
// downstream events via outbox.PGWriter. State-update and outbox
// Append happen in the same pgx.Tx (see withTx) so a handler that
// crashes mid-step leaves no orphan rows — the saga row only advances
// if the matching event is queued for publish.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
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

// WithRepoAndWriter returns a copy of h that uses the supplied repo
// and writer. Used by handler-level integration tests to inject
// dependencies without exposing internal struct fields. Production
// code uses NewHandler.
func (h *Handler) WithRepoAndWriter(repo *repository.PGRepo, writer *outbox.PGWriter) *Handler {
	cp := *h
	cp.repo = repo
	cp.writer = writer
	return &cp
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
// first item (multi-item flow is out of scope for v0.5.0). Both
// writes share one pgx.BeginFunc so the saga row only commits if
// the outbox event is queued.
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
	itemsJSON, err := json.Marshal(p.Items)
	if err != nil {
		return err
	}

	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if err := h.repo.InsertTx(ctx, tx, &repository.Saga{
			OrderID:       p.OrderID,
			State:         sagapkg.StateInitiated,
			Items:         itemsJSON,
			TotalCents:    p.TotalCents,
			ReservationID: reservationID,
		}); err != nil {
			return err
		}
		payload, perr := json.Marshal(sagaev.StockReserveRequestedPayload{
			OrderID:       p.OrderID,
			SKU:           first.SKU,
			Quantity:      first.Quantity,
			ReservationID: reservationID,
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "StockReserveRequested", p.OrderID, payload)
	})
}

// StockReservedHandler advances the saga to StateStockReserved and
// emits PaymentRequested with the saga's stored total. The
// AmountCents comes from the saga row, not the StockReserved
// payload, because the inventory event doesn't carry the order
// total (separate aggregate).
//
// Idempotency: TransitionStateTx matches state='initiated' so a
// redelivered StockReserved (Kafka rebalance, consumer restart
// after the tx committed but before the offset was marked) is a
// no-op — no second PaymentRequested is emitted, no double
// charge downstream.
func (h *Handler) StockReservedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		s, err := h.repo.GetTx(ctx, tx, p.OrderID)
		if err != nil {
			if err == repository.ErrNotFound {
				h.logger.Warn("StockReserved for unknown saga", "order_id", p.OrderID)
				return nil
			}
			return err
		}
		advanced, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateInitiated, sagapkg.StateStockReserved)
		if err != nil {
			return err
		}
		if !advanced {
			h.logger.Info("StockReserved: saga already past initiated, skipping emit",
				"order_id", p.OrderID, "current_state", s.State)
			return nil
		}
		payload, perr := json.Marshal(sagaev.PaymentRequestedPayload{
			OrderID:        p.OrderID,
			AmountCents:    s.TotalCents,
			IdempotencyKey: uuid.NewString(),
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "PaymentRequested", p.OrderID, payload)
	})
}

// PaymentCompletedHandler is the happy-path terminal: advance to
// StateCompleted and emit OrderConfirmed — atomically.
//
// Idempotency: TransitionStateTx matches any non-terminal state
// (initiated or stock_reserved). A redelivered PaymentCompleted
// (replay or duplicate consumer) is a no-op for both state and
// outbox emission.
func (h *Handler) PaymentCompletedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Try the two legal pre-terminal states in order. Either
		// match advances the saga; both are non-terminal so
		// either can transition to StateCompleted.
		var advanced bool
		var currentState string
		for _, from := range []sagapkg.State{sagapkg.StateStockReserved, sagapkg.StateInitiated} {
			ok, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, from, sagapkg.StateCompleted)
			if err != nil {
				if err == repository.ErrNotFound {
					h.logger.Warn("PaymentCompleted for unknown saga", "order_id", p.OrderID)
					return nil
				}
				return err
			}
			if ok {
				advanced = true
				break
			}
			currentState = string(from)
		}
		if !advanced {
			h.logger.Info("PaymentCompleted: saga already terminal, skipping emit",
				"order_id", p.OrderID, "tried_from", currentState)
			return nil
		}
		payload, perr := json.Marshal(sagaev.OrderConfirmedPayload{
			OrderID:     p.OrderID,
			ConfirmedAt: nowRFC3339(),
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "OrderConfirmed", p.OrderID, payload)
	})
}

// PaymentFailedHandler triggers compensation. The saga transitions
// to StateCompensated and emits one StockReleaseRequested per item
// in the saga row + OrderCancelled — all in one transaction. SKU
// and Quantity come from the saga row because PaymentFailed doesn't
// carry them.
//
// Idempotency: TransitionStateTx matches any non-terminal state
// (initiated or stock_reserved). A redelivered PaymentFailed is
// a silent no-op.
func (h *Handler) PaymentFailedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		s, err := h.repo.GetTx(ctx, tx, p.OrderID)
		if err != nil {
			if err == repository.ErrNotFound {
				h.logger.Warn("PaymentFailed for unknown saga", "order_id", p.OrderID)
				return nil
			}
			return err
		}
		advanced, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateStockReserved, sagapkg.StateCompensated)
		if err != nil {
			return err
		}
		if !advanced {
			// Try the other non-terminal state (initiated → failed before stock reserved).
			ok2, err2 := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateInitiated, sagapkg.StateCompensated)
			if err2 != nil {
				return err2
			}
			if !ok2 {
				h.logger.Info("PaymentFailed: saga already terminal, skipping emit",
					"order_id", p.OrderID)
				return nil
			}
		}
		if err := emitReleaseForItems(ctx, tx, h, p.OrderID, s.ReservationID, s.Items); err != nil {
			return err
		}
		cancelPayload, perr := json.Marshal(sagaev.OrderCancelledPayload{
			OrderID: p.OrderID,
			Reason:  "payment_failed",
			Source:  "saga",
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "OrderCancelled", p.OrderID, cancelPayload)
	})
}

// emitReleaseForItems writes one StockReleaseRequested per item the
// saga reserved. Decodes the JSONB items blob (set when the saga was
// created on OrderCreated); emits nothing when the row has no items
// (e.g. empty items in payload — the OrderCreatedHandler already
// ack-skips that case so this should not happen in practice).
func emitReleaseForItems(ctx context.Context, tx pgx.Tx, h *Handler, orderID, reservationID string, itemsJSON []byte) error {
	var items []struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	if err := json.Unmarshal(itemsJSON, &items); err != nil {
		return fmt.Errorf("decode saga items for release: %w", err)
	}
	for _, it := range items {
		if it.Quantity <= 0 || it.SKU == "" {
			continue
		}
		releasePayload, perr := json.Marshal(sagaev.StockReleaseRequestedPayload{
			OrderID:       orderID,
			ReservationID: reservationID,
			SKU:           it.SKU,
			Quantity:      it.Quantity,
		})
		if perr != nil {
			return perr
		}
		if err := h.appendOutbox(ctx, tx, "StockReleaseRequested", orderID, releasePayload); err != nil {
			return err
		}
	}
	return nil
}

// StockReleasedHandler is the compensation ack from inventory. The
// saga is already in StateCompensated by the time this fires — we
// touch updated_at so observability tools see the chain close.
// Note: the brief labels this terminal "fully_compensated" but the
// state machine defines StateCompensated as terminal already, so
// we keep it consistent with state.go.
//
// We use a guarded TransitionStateTx (from='compensated' to='compensated') so
// an out-of-order replay cannot downgrade a saga that has somehow
// reached StateCompleted (defensive only — the normal event flow
// makes this unreachable; PaymentFailedHandler is the only emitter
// of StockReleaseRequested, which only fans out into StockReleased
// via inventory's terminal handler).
func (h *Handler) StockReleasedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		_, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateCompensated, sagapkg.StateCompensated)
		return err
	})
	if err != nil && err != repository.ErrNotFound {
		return err
	}
	return nil
}

// StockReservationFailedHandler is the inventory-side failure
// path: saga is in StateInitiated (never got stock reserved), so
// we transition directly to StateCompensated and emit
// OrderCancelled so the order service can mark it failed.
//
// Idempotency: TransitionStateTx matches state='initiated'. A
// redelivered StockReservationFailed is a silent no-op.
func (h *Handler) StockReservationFailedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		advanced, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateInitiated, sagapkg.StateCompensated)
		if err != nil {
			if err == repository.ErrNotFound {
				h.logger.Warn("StockReservationFailed for unknown saga", "order_id", p.OrderID)
				return nil
			}
			return err
		}
		if !advanced {
			h.logger.Info("StockReservationFailed: saga already past initiated, skipping emit",
				"order_id", p.OrderID)
			return nil
		}
		payload, perr := json.Marshal(sagaev.OrderCancelledPayload{
			OrderID: p.OrderID,
			Reason:  "stock_failed",
			Source:  "saga",
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "OrderCancelled", p.OrderID, payload)
	})
}

// nowRFC3339 returns the current UTC time formatted as RFC3339 —
// the wire shape OrderConfirmedPayload expects. Defined here so
// the test in handlers_test.go can override it via package-level
// variable swap if needed (it doesn't today).
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// appendOutbox writes a single outbox row inside the supplied tx.
// Used by every handler that needs to atomically advance saga state
// AND queue the matching downstream event.
func (h *Handler) appendOutbox(ctx context.Context, tx pgx.Tx, eventType, aggregateID string, payload []byte) error {
	return h.writer.Append(ctx, tx, platformoutbox.Record{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         outbox.Topic,
		Payload:       payload,
		Headers:       map[string]string{},
	})
}
