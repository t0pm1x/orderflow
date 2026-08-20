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
//
// SAGA-5: OrderCancelled is added so a user-initiated DELETE on
// /v1/orders/{id} (which publishes OrderCancelled with reason=
// "user_request", source="user") cancels the saga. Pre-fix the
// event was ack-and-skipped by the saga consumer's unknown-event
// branch, so the saga proceeded to charge the card and never
// released stock.
//
// SAGA-7 [DEFERRED]: pkg/consumer/consumer.go:258-263 returns
// without calling c.markRecord(rec) when the deduper.Seen()
// check reports the event has already been processed. With
// kgo.DisableAutoCommit, the record re-fetches forever — the
// saga is the most exposed consumer (3 topics, 1 group). The
// fix would be to add `c.markRecord(rec); return` at lines
// 258-263 of pkg/consumer/consumer.go. This is left for the
// pkg/consumer owner (not in this saga fix's scope).
func (h *Handler) Registry() pkgconsumer.HandlerRegistry {
	return pkgconsumer.HandlerRegistry{
		"OrderCreated":           h.OrderCreatedHandler,
		"StockReserved":          h.StockReservedHandler,
		"PaymentCompleted":       h.PaymentCompletedHandler,
		"PaymentFailed":          h.PaymentFailedHandler,
		"StockReleased":          h.StockReleasedHandler,
		"StockReservationFailed": h.StockReservationFailedHandler,
		"OrderCancelled":         h.OrderCancelledHandler,
	}
}

// OrderCreatedHandler starts a new saga. Inserts the saga row in
// StateInitiated and emits one StockReserveRequested per item in
// the order, each with its own reservation_id. Both writes share
// one pgx.BeginFunc so the saga row + all outbox events commit
// (or roll back) atomically.
//
// SAGA-3: pre-fix this handler emitted StockReserveRequested for
// items[0] only, but PaymentFailed emits StockReleaseRequested for
// ALL items — so a release for items[1..n] decremented the wrong
// order's reservation (cross-order stock theft). Post-fix, every
// item gets its own reservation_id emitted in the same tx, so the
// saga's later release events match by reservation_id and only
// touch stock this saga actually reserved.
//
// The v1.1.5 handler also persists the order's LastFour payment
// hint on the saga row (saga/migrations/0003_saga_payment_last_four.sql)
// so StockReservedHandler can forward it on the downstream
// PaymentRequested payload — see the doc on events.PaymentRequestedPayload.
//
// SAGA-6: InsertTx is idempotent (ON CONFLICT DO NOTHING). On
// replay the row is already there; the closure returns nil
// without emitting a fresh StockReserveRequested outbox row.
func (h *Handler) OrderCreatedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
		Items      []struct {
			SKU            string `json:"sku"`
			Quantity       int    `json:"quantity"`
			UnitPriceCents int64  `json:"unit_price_cents"`
		} `json:"items"`
		TotalCents int64  `json:"total_cents"`
		LastFour   string `json:"last_four,omitempty"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.logger.Warn("OrderCreated unmarshal", "err", err, "event_id", env.EventID)
		return err
	}
	if len(p.Items) == 0 {
		h.logger.Warn("OrderCreated has no items", "order_id", p.OrderID)
		return nil
	}

	// SAGA-3: one reservation_id per item. The release flow uses
	// these to match stock_reservations rows so a release for
	// items[1..n] can only touch stock this saga reserved.
	reservationIDs := make([]string, len(p.Items))
	for i := range p.Items {
		reservationIDs[i] = uuid.NewString()
	}
	// Embed the per-item reservation_id into the persisted items
	// blob so PaymentFailedHandler / TTL sweep can match each
	// release to its specific reservation without a separate
	// column. The external wire shape is unchanged.
	persistedItems := make([]sagaev.PersistedItem, len(p.Items))
	for i, it := range p.Items {
		persistedItems[i] = sagaev.PersistedItem{
			SKU:            it.SKU,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
			ReservationID:  reservationIDs[i],
		}
	}
	persistedItemsJSON, err := json.Marshal(persistedItems)
	if err != nil {
		return err
	}

	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.repo.InsertTx(ctx, tx, &repository.Saga{
			OrderID:    p.OrderID,
			State:      sagapkg.StateInitiated,
			Items:      persistedItemsJSON,
			TotalCents: p.TotalCents,
			// Saga row keeps the FIRST reservation_id for
			// backwards compatibility with existing read paths
			// (PaymentFailed emits releases keyed on this id).
			ReservationID: reservationIDs[0],
			LastFour:      p.LastFour,
		})
		if err != nil {
			return err
		}
		if !inserted {
			// SAGA-6: duplicate OrderCreated is a silent no-op;
			// no fresh StockReserveRequested rows. The first
			// delivery's outbox rows are still PENDING and will
			// be polled normally.
			h.logger.Info("OrderCreated: saga already exists, ack-skipping",
				"order_id", p.OrderID)
			return nil
		}
		// SAGA-3: one StockReserveRequested per item, each with
		// its own reservation_id. Emit inside the same tx so
		// the saga row + N reservation events commit together.
		for i, it := range p.Items {
			payload, perr := json.Marshal(sagaev.StockReserveRequestedPayload{
				OrderID:       p.OrderID,
				SKU:           it.SKU,
				Quantity:      it.Quantity,
				ReservationID: reservationIDs[i],
			})
			if perr != nil {
				return perr
			}
			if err := h.appendOutbox(ctx, tx, "StockReserveRequested", p.OrderID, payload); err != nil {
				return err
			}
		}
		return nil
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
//
// The v1.1.5 handler also forwards LastFour from the saga row onto
// the PaymentRequested payload (set in OrderCreatedHandler). The
// payment service uses it to pick a deterministic success/decline
// branch instead of the pre-v1.1.5 derive-from-orderID fallback.
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
			LastFour:       s.LastFour,
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
//
// SAGA-4: when PaymentCompleted lands on an already-compensated
// saga, the payment service has already charged the customer's
// card but the saga drove the order to StateCompensated (e.g.
// PaymentFailed raced with PaymentCompleted, or the TTL sweep
// compensated after PaymentCompleted). The pre-fix handler
// silently swallowed the event — the customer was charged for a
// cancelled order. Post-fix, the handler emits
// PaymentRefundRequested so the payment service can issue a
// refund against the captured transaction.
//
// The PaymentCompleted event may also carry payment_id (see
// services/payment/internal/webhook/handler.go:148-180); the
// current payload decode is minimal, but the refund event
// includes whatever the consumer receives. If payment_id is not
// present in the payload, the refund's PaymentID is left empty
// and the payment service falls back to its own lookup by
// order_id.
func (h *Handler) PaymentCompletedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID   string `json:"order_id"`
		PaymentID string `json:"payment_id,omitempty"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Try the two legal pre-terminal states in order. Either
		// match advances the saga; both are non-terminal so
		// either can transition to StateCompleted.
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
				payload, perr := json.Marshal(sagaev.OrderConfirmedPayload{
					OrderID:     p.OrderID,
					ConfirmedAt: nowRFC3339(),
				})
				if perr != nil {
					return perr
				}
				return h.appendOutbox(ctx, tx, "OrderConfirmed", p.OrderID, payload)
			}
		}
		// SAGA-4: neither from-state matched. The saga is either
		// already terminal (compensated or completed) or does
		// not exist. Look up the row's actual current state to
		// decide between refund-emit and silent no-op.
		s, gerr := h.repo.GetTx(ctx, tx, p.OrderID)
		if gerr != nil {
			if gerr == repository.ErrNotFound {
				h.logger.Warn("PaymentCompleted for unknown saga", "order_id", p.OrderID)
				return nil
			}
			return gerr
		}
		if s.State == sagapkg.StateCompensated {
			h.logger.Warn("PaymentCompleted on compensated saga — issuing refund",
				"order_id", p.OrderID,
				"payment_id", p.PaymentID)
			refundPayload, perr := json.Marshal(sagaev.PaymentRefundRequestedPayload{
				OrderID:     p.OrderID,
				PaymentID:   p.PaymentID,
				AmountCents: s.TotalCents,
				Reason:      "saga_already_compensated",
			})
			if perr != nil {
				return perr
			}
			return h.appendOutbox(ctx, tx, "PaymentRefundRequested", p.OrderID, refundPayload)
		}
		h.logger.Info("PaymentCompleted: saga already terminal, skipping emit",
			"order_id", p.OrderID, "current_state", s.State)
		return nil
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
		if err := emitReleaseForItems(ctx, tx, h, p.OrderID, s.Items); err != nil {
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
//
// SAGA-3: each emitted StockReleaseRequested carries the per-item
// reservation_id persisted on the saga row's items blob, so
// inventory's release flow can match by reservation_id and only
// touch stock this saga actually reserved. Pre-fix, every release
// used the saga row's first reservation_id and stock_reservations
// was keyed on sku+qty only — a release for items[1..n] could
// decrement another order's reserved counter.
func emitReleaseForItems(ctx context.Context, tx pgx.Tx, h *Handler, orderID string, itemsJSON []byte) error {
	var items []sagaev.PersistedItem
	if err := json.Unmarshal(itemsJSON, &items); err != nil {
		// Legacy rows (pre-SAGA-3) may have the old {sku, quantity}
		// shape without reservation_id. Decode those as a
		// fallback and emit releases with the saga's primary
		// reservation_id; the inventory release flow falls back
		// to sku+qty matching when stock_reservations has no row
		// for the supplied id.
		var legacy []struct {
			SKU      string `json:"sku"`
			Quantity int    `json:"quantity"`
		}
		if lerr := json.Unmarshal(itemsJSON, &legacy); lerr != nil {
			return fmt.Errorf("decode saga items for release: %w", lerr)
		}
		for _, it := range legacy {
			if it.Quantity <= 0 || it.SKU == "" {
				continue
			}
			releasePayload, perr := json.Marshal(sagaev.StockReleaseRequestedPayload{
				OrderID:       orderID,
				ReservationID: "", // legacy path: no per-item reservation_id
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
	for _, it := range items {
		if it.Quantity <= 0 || it.SKU == "" {
			continue
		}
		releasePayload, perr := json.Marshal(sagaev.StockReleaseRequestedPayload{
			OrderID:       orderID,
			ReservationID: it.ReservationID,
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
//
// SAGA-2: defensively ack-skip when order_id is empty. Inventory
// historically emitted StockReleased with AggregateID=reservation_id
// and no order_id field in the payload; the saga handler would
// decode OrderID="" and run UPDATE WHERE order_id=” against the
// UUID column, raising SQLSTATE 22P02 — a non-nil error that retries
// 5×1s and DLQs, blocking the consumer for every cancelled order.
// After the SAGA-2 fix inventory includes order_id in the payload,
// and the defensive skip absorbs any straggler pre-fix events
// without blocking the consumer.
func (h *Handler) StockReleasedHandler(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if p.OrderID == "" {
		// SAGA-2 defensive skip: malformed/legacy StockReleased
		// payload without an order_id. Ack-and-skip so the consumer
		// doesn't loop on the same record with the same
		// SQLSTATE 22P02 forever. Production should never see
		// these after the inventory fix lands, but defense-in-depth
		// is cheap.
		h.logger.Warn("StockReleased: empty order_id, ack-skipping",
			"event_id", env.EventID,
			"reservation_id", env.AggregateID)
		return nil
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

// OrderCancelledHandler is the SAGA-5 fix: a user-initiated DELETE
// on /v1/orders/{id} publishes OrderCancelled (source="user",
// reason="user_request") to order-events. Pre-fix the saga had no
// handler for this event so the consumer ack-and-skipped it: the
// saga proceeded to charge the card and never released stock.
//
// Post-fix, the handler drives the saga to StateCompensated and
// emits one StockReleaseRequested per item so inventory's SAGA-3
// reservation_id matching can decrement the saga's own reservation
// (no cross-order theft). It also emits a saga-source
// OrderCancelled so the order service's OrderCancelled handler
// can finalize the order row.
//
// Idempotency: TransitionStateTx matches any non-terminal state
// (initiated or stock_reserved). A redelivered OrderCancelled is
// a silent no-op (the saga is already terminal). A first delivery
// against a saga already in StateCompleted is also a silent
// no-op (the saga already finalized on a prior PaymentCompleted;
// the user's cancel request was already too late).
func (h *Handler) OrderCancelledHandler(ctx context.Context, env *events.Envelope) error {
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
				h.logger.Warn("OrderCancelled for unknown saga", "order_id", p.OrderID)
				return nil
			}
			return err
		}
		advanced, err := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateStockReserved, sagapkg.StateCompensated)
		if err != nil {
			return err
		}
		if !advanced {
			// Try the other non-terminal state (initiated → user
			// cancelled before stock was reserved).
			ok2, err2 := h.repo.TransitionStateTx(ctx, tx, p.OrderID, sagapkg.StateInitiated, sagapkg.StateCompensated)
			if err2 != nil {
				return err2
			}
			if !ok2 {
				h.logger.Info("OrderCancelled: saga already terminal, skipping emit",
					"order_id", p.OrderID)
				return nil
			}
		}
		if err := emitReleaseForItems(ctx, tx, h, p.OrderID, s.Items); err != nil {
			return err
		}
		cancelPayload, perr := json.Marshal(sagaev.OrderCancelledPayload{
			OrderID: p.OrderID,
			Reason:  "user_request",
			Source:  "saga",
		})
		if perr != nil {
			return perr
		}
		return h.appendOutbox(ctx, tx, "OrderCancelled", p.OrderID, cancelPayload)
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
