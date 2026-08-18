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
			"PaymentRequested": h.PaymentRequested,
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
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("decode PaymentRequested: %w", err)
	}
	if p.OrderID == "" {
		return errors.New("PaymentRequested: order_id is required")
	}

	paymentID := uuid.NewString()

	// Derive lastFour from the order_id for the mock provider. Real
	// integrations would read this from the order's payment row (we
	// don't have one yet at this stage). For UUIDs this gives the
	// last 4 hex chars of the order_id; for the magic suffixes in
	// the mock ("0001"/"0002"/"0003") the operator has to construct
	// a matching order_id. Fallback "0000" means "anything else →
	// succeeded" in the mock.
	lastFour := "0000"
	if len(p.OrderID) >= 4 {
		lastFour = p.OrderID[len(p.OrderID)-4:]
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
