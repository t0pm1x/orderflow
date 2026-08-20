// Package consumer wires the Payment Service's Kafka handler
// registry. Payment consumes PaymentRequested (from the saga
// orchestrator) and emits PaymentCompleted/PaymentFailed through
// the transactional outbox.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
	pkgox "github.com/t0pm1x/orderflow/platform/outbox"
	svcoutbox "github.com/t0pm1x/orderflow/services/payment/internal/outbox"
	"github.com/t0pm1x/orderflow/services/payment/internal/provider"
)

// topic is the Kafka topic the Payment Service emits
// PaymentCompleted / PaymentFailed to. The saga service subscribes
// to this topic (alongside order-events + inventory-events) so it
// sees the payment result for the saga it started.
const topic = "payment-events"

// Handler is the real Payment Service handler for PaymentRequested.
// It calls the mock provider, persists the payment row, and emits
// PaymentCompleted/PaymentFailed via the outbox — all in one tx.
type Handler struct {
	pool   *pgxpool.Pool
	writer *svcoutbox.PGWriter
	logger *slog.Logger
}

// NewHandler constructs a Handler. pool must be non-nil — the
// caller (main.go) wires the pool only when DATABASE_URL is set,
// and only then calls SetHandler.
func NewHandler(pool *pgxpool.Pool, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		pool:   pool,
		writer: svcoutbox.NewPGWriter(),
		logger: logger,
	}
}

// globalHandler is wired by SetHandler at startup. When nil, Registry
// falls back to the v0.4.x stub behavior so unit tests that don't
// have a Postgres fixture still work. Accesses are synchronized via
// atomic.Pointer so SetHandler in main and Registry in the consumer
// goroutine never race on the pointer write.
var globalHandler atomic.Pointer[Handler]

// SetHandler wires h as the active handler. Call once at startup
// after the DB pool is built.
func SetHandler(h *Handler) { globalHandler.Store(h) }

// Registry returns the Payment Service's handler registry. When the
// real handler has been wired via SetHandler it dispatches to it;
// otherwise it returns the stub registry that just logs (the
// pre-v0.5.0 behavior — tests rely on this).
func Registry(logger *slog.Logger) pkgconsumer.HandlerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	if h := globalHandler.Load(); h != nil {
		return pkgconsumer.HandlerRegistry{
			"PaymentRequested":       h.PaymentRequested,
			"PaymentRefundRequested": h.PaymentRefundRequested,
		}
	}
	stub := func(eventType string) pkgconsumer.Handler {
		return func(_ context.Context, env *events.Envelope) error {
			logger.Info("orderflow-payment received event",
				"event_type", eventType,
				"event_id", env.EventID,
				"aggregate_id", env.AggregateID,
			)
			return nil
		}
	}
	return pkgconsumer.HandlerRegistry{
		"PaymentRequested": stub("PaymentRequested"),
	}
}

// PaymentRequested handles a PaymentRequested event by charging the
// mock provider, persisting the payment row, and emitting
// PaymentCompleted or PaymentFailed through the outbox — both in
// the same transaction so the row + event are atomic.
//
// Errors from provider.Charge (e.g. timeout) bubble up to the
// consumer retry/DLQ loop; we do NOT write a payment row or outbox
// event in that case so a retry can drive the saga to a clean
// terminal state.
func (h *Handler) PaymentRequested(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID        string `json:"order_id"`
		AmountCents    int64  `json:"amount_cents"`
		IdempotencyKey string `json:"idempotency_key"`
		LastFour       string `json:"last_four,omitempty"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("decode PaymentRequested: %w", err)
	}
	if p.OrderID == "" {
		return errors.New("PaymentRequested: order_id is required")
	}

	// Use the order_id as the payment row's primary key. The
	// pre-v1.1.5 handler allocated a fresh uuid.NewString() here,
	// which broke the orderflow-web "Force webhook" button: the
	// BFF fires `POST /v1/payments/webhook` with payment_id set to
	// the order_id (services/web/internal/backend/payment.go: the
	// mock's idempotency guard keys on the webhook body, and the
	// author intended payment_id == order_id), but the payment
	// row's actual id was a fresh UUID, so the webhook's
	// `repo.Get(payment_id)` returned ErrPaymentNotFound (HTTP 404)
	// and the BFF surfaced 502. Aligning payment.id with order_id
	// satisfies the existing UNIQUE(order_id) constraint, lets the
	// web's "Force ✓/✗" buttons work out of the box, and removes
	// the only cross-service id mismatch on the platform.
	paymentID := p.OrderID

	// Pick the last_four that drives the mock provider's
	// success/decline branch. v1.1.5: prefer the value the saga
	// forwarded from the originating OrderCreated event (which in
	// turn came from the submit body); this makes the test
	// `last_four=0001 → declined` claim actually deterministic.
	// Pre-v1.1.5 fallback: derive from orderID[len(orderID)-4:]
	// when the saga didn't forward a hint — keep that behavior
	// for clients that haven't yet shipped the v1.1.5 wire shape.
	lastFour := p.LastFour
	if lastFour == "" {
		lastFour = "0000"
		if len(p.OrderID) >= 4 {
			lastFour = p.OrderID[len(p.OrderID)-4:]
		}
	}

	result, err := provider.Charge(ctx, paymentID, p.AmountCents, lastFour)
	if err != nil {
		return fmt.Errorf("provider.Charge: %w", err)
	}

	eventType := "PaymentFailed"
	payload := map[string]any{
		"payment_id": paymentID,
		"order_id":   p.OrderID,
		"error_code": result.ErrorCode,
	}
	if result.Status == "succeeded" {
		eventType = "PaymentCompleted"
		payload = map[string]any{
			"payment_id": paymentID,
			"order_id":   p.OrderID,
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	rec := pkgox.Record{
		EventID:       uuid.NewString(),
		AggregateID:   p.OrderID,
		AggregateType: "Order",
		EventType:     eventType,
		SchemaVersion: "1.0",
		Topic:         topic,
		Payload:       body,
	}

	h.logger.Info("payment service handling PaymentRequested",
		"order_id", p.OrderID,
		"payment_id", paymentID,
		"amount_cents", p.AmountCents,
		"last_four", lastFour,
		"result_status", result.Status,
		"emitting", eventType,
	)

	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Dedupe on order_id (the saga's aggregate). The pre-v1.1
		// code deduped on paymentID (a fresh UUID per delivery),
		// which never collided on redelivery — duplicate payments
		// rows were written. The v1.1 migration
		// (0003_payment_order_unique.sql) adds a UNIQUE constraint
		// on order_id so ON CONFLICT (order_id) DO NOTHING actually
		// dedupes.
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (id, order_id, amount_cents, status, error_code, last_four)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (order_id) DO NOTHING`,
			paymentID, p.OrderID, p.AmountCents, result.Status, result.ErrorCode, lastFour,
		); err != nil {
			return fmt.Errorf("insert payment: %w", err)
		}
		return h.writer.Append(ctx, tx, rec)
	})
}

// PaymentRefundRequested handles a refund request emitted by the saga
// when PaymentCompleted lands on an already-compensated saga (audit
// NEW-P0-2 / SAGA-4). Calls provider.Refund and writes a
// PaymentRefunded outbox event in the same transaction so the saga
// can audit the refund chain.
//
// Terminal-state guard: only refunds payments that are still in a
// succeeded state. If the row is already refunded/failed, the
// handler is a no-op so duplicate deliveries don't double-refund.
func (h *Handler) PaymentRefundRequested(ctx context.Context, env *events.Envelope) error {
	var p struct {
		OrderID     string `json:"order_id"`
		PaymentID   string `json:"payment_id"`
		AmountCents int64  `json:"amount_cents"`
		Reason      string `json:"reason,omitempty"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("decode PaymentRefundRequested: %w", err)
	}
	if p.OrderID == "" {
		return errors.New("PaymentRefundRequested: order_id is required")
	}

	// Payment ID defaults to order_id, matching the
	// UNIQUE(order_id) invariant enforced by PaymentRequested.
	paymentID := p.PaymentID
	if paymentID == "" {
		paymentID = p.OrderID
	}

	// Refund via the provider. The mock's Refund always succeeds.
	if _, err := provider.Refund(ctx, paymentID, p.AmountCents); err != nil {
		return fmt.Errorf("provider.Refund: %w", err)
	}

	// Emit PaymentRefunded for downstream audit (saga + web
	// playground). Terminal-state guard on the UPDATE: only the
	// succeeded → refunded transition is allowed.
	refundedPayload, err := json.Marshal(map[string]any{
		"order_id":     p.OrderID,
		"payment_id":   paymentID,
		"amount_cents": p.AmountCents,
		"reason":       p.Reason,
	})
	if err != nil {
		return fmt.Errorf("marshal refund outbox payload: %w", err)
	}

	rec := pkgox.Record{
		EventID:       uuid.NewString(),
		AggregateID:   p.OrderID,
		AggregateType: "Order",
		EventType:     "PaymentRefunded",
		SchemaVersion: "1.0",
		Topic:         topic,
		Payload:       refundedPayload,
	}

	h.logger.Info("payment service handling PaymentRefundRequested",
		"order_id", p.OrderID,
		"payment_id", paymentID,
		"amount_cents", p.AmountCents,
	)

	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Terminal-state guard: only mark the payment row as
		// refunded if it's currently 'succeeded'. Idempotent on
		// redelivery.
		tag, err := tx.Exec(ctx,
			`UPDATE payments SET status = 'refunded' WHERE id = $1 AND status = 'succeeded'`,
			paymentID,
		)
		if err != nil {
			return fmt.Errorf("update payment status: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Already refunded or never succeeded; ack-drop so
			// the saga audit isn't re-emitted.
			h.logger.Info("PaymentRefundRequested: payment not in succeeded state, ack-drop",
				"payment_id", paymentID,
			)
			return nil
		}
		return h.writer.Append(ctx, tx, rec)
	})
}
