// Package watchdog contains the Saga Service's cross-restart TTL
// sweep. The in-process Watchdog (services/saga/timeout.go) is
// only correct for sagas that are alive in memory — a crash loses
// its registered deadlines. The sweep here is the durable
// recovery path: every interval, it queries order_sagas for rows
// past expires_at AND still non-terminal, transitions them to
// compensated, and emits StockReleaseRequested + OrderCancelled
// (reason="timeout") through the saga outbox so the rest of the
// platform can finalize the cancellation on the next poll.
//
// This is sub-stage 3.9.c. It does not replace the in-process
// Watchdog; both run in parallel (in-process for live sagas,
// DB sweep for sagas that died before they could fire).
package watchdog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	platformoutbox "github.com/t0pm1x/orderflow/platform/outbox"

	sagaev "github.com/t0pm1x/orderflow/services/saga/internal/events"
	sagaoutbox "github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	"github.com/t0pm1x/orderflow/services/saga/internal/repository"
)

// TTLSweep periodically scans order_sagas for expired non-terminal
// rows and emits the same compensation events PaymentFailedHandler
// emits (StockReleaseRequested + OrderCancelled), so the rest of
// the platform can't distinguish a sweep-driven compensation from
// a payment-failure-driven one.
type TTLSweep struct {
	pool      *pgxpool.Pool
	repo      *repository.PGRepo
	writer    *sagaoutbox.PGWriter
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
}

// NewTTLSweep constructs a TTLSweep. interval is how often the
// sweep runs (production uses 30s); batchSize caps the number of
// sagas compensated per tick so a backlog can't blow up a single
// transaction. logger may be nil (defaults to slog.Default()).
func NewTTLSweep(pool *pgxpool.Pool, repo *repository.PGRepo, writer *sagaoutbox.PGWriter, interval time.Duration, logger *slog.Logger) *TTLSweep {
	if logger == nil {
		logger = slog.Default()
	}
	return &TTLSweep{
		pool:      pool,
		repo:      repo,
		writer:    writer,
		interval:  interval,
		batchSize: 100,
		logger:    logger,
	}
}

// Run blocks until ctx is cancelled. The first sweep runs
// immediately (so a restart doesn't wait interval for its first
// pass), then on every tick afterwards.
func (t *TTLSweep) Run(ctx context.Context) {
	t.RunOnce(ctx)
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.RunOnce(ctx)
		}
	}
}

// RunOnce executes exactly one sweep pass. Exported (not
// sweepOnce-private) so the integration tests can drive it
// deterministically without sleeping for a real ticker tick. The
// production loop (Run) calls RunOnce on every tick.
func (t *TTLSweep) RunOnce(ctx context.Context) {
	expired, err := t.repo.ListExpired(ctx, t.batchSize)
	if err != nil {
		t.logger.Error("ttl sweep: list expired failed", "err", err)
		return
	}
	if len(expired) == 0 {
		return
	}
	t.logger.Info("ttl sweep: compensating expired sagas", "count", len(expired))
	for _, s := range expired {
		if err := t.compensate(ctx, s); err != nil {
			t.logger.Error("ttl sweep: compensate failed", "order_id", s.OrderID, "err", err)
		}
	}
}

// compensate transitions a single expired saga to compensated and
// emits the same outbox rows PaymentFailedHandler emits: one
// StockReleaseRequested per item the saga reserved (so inventory
// decrements stock_items.reserved) and OrderCancelled with
// reason="timeout" (so the order service marks the order cancelled).
// All writes happen in one tx so the saga state and its events
// commit/rollback atomically — preventing the half-state of
// "compensated with no events emitted" that would leave stock
// stranded. Marshal errors are returned (not panicked) so a
// malformed payload aborts the tx cleanly instead of crashing the
// whole saga service mid-tx.
//
// The UPDATE carries BOTH a state guard AND an expires_at < NOW()
// guard so a saga whose expires_at was refreshed by TransitionStateTx
// (SAGA-1 fix) between the sweep's SELECT and UPDATE is a no-op for
// state — and crucially, RowsAffected=0 skips the outbox emission
// so a race with the in-flight consumer handler doesn't double-emit
// compensation events. Pre-fix, the UPDATE only guarded on state,
// which let the sweep charge-and-cancel a saga that had just been
// transitioned by a handler.
func (t *TTLSweep) compensate(ctx context.Context, s *repository.Saga) error {
	cancelPayload, err := json.Marshal(sagaev.OrderCancelledPayload{
		OrderID: s.OrderID,
		Reason:  "timeout",
		Source:  "saga",
	})
	if err != nil {
		return fmt.Errorf("marshal OrderCancelled: %w", err)
	}

	return pgx.BeginFunc(ctx, t.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx,
			`UPDATE order_sagas
			    SET state = 'compensated', updated_at = NOW()
			  WHERE order_id = $1
			    AND state NOT IN ('completed', 'compensated')
			    AND expires_at < NOW()`, s.OrderID)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			// Saga was already terminal OR alive (expires_at
			// refreshed by a concurrent TransitionStateTx) by the
			// time the sweep got here. Either way: skip the outbox
			// emission to avoid duplicate compensation events
			// downstream and to avoid charge-and-cancel on an
			// alive saga (SAGA-1).
			t.logger.Info("ttl sweep: saga already terminal or alive, skipping emit",
				"order_id", s.OrderID)
			return nil
		}
		if err := appendReleaseEvents(ctx, tx, t.writer, s); err != nil {
			return err
		}
		return t.writer.Append(ctx, tx, platformoutbox.Record{
			EventID:       uuid.NewString(),
			AggregateID:   s.OrderID,
			AggregateType: "Order",
			EventType:     "OrderCancelled",
			SchemaVersion: "1.0",
			Topic:         sagaoutbox.Topic,
			Payload:       cancelPayload,
			Headers:       map[string]string{},
		})
	})
}

// appendReleaseEvents emits one StockReleaseRequested per item the
// saga reserved. Decodes the saga row's JSONB items blob; emits
// nothing when the items list is empty (the OrderCreatedHandler
// already ack-skips empty-item payloads so this should be rare).
//
// SAGA-3: each emitted StockReleaseRequested carries the per-item
// reservation_id persisted on the saga row's items blob, so
// inventory's release flow can match by reservation_id and only
// touch stock this saga actually reserved.
func appendReleaseEvents(ctx context.Context, tx pgx.Tx, writer *sagaoutbox.PGWriter, s *repository.Saga) error {
	var items []sagaev.PersistedItem
	if err := json.Unmarshal(s.Items, &items); err != nil {
		// Legacy rows (pre-SAGA-3) may have the old {sku, quantity}
		// shape without reservation_id. Decode those as a fallback
		// and emit releases with the saga's primary reservation_id.
		var legacy []struct {
			SKU      string `json:"sku"`
			Quantity int    `json:"quantity"`
		}
		if lerr := json.Unmarshal(s.Items, &legacy); lerr != nil {
			return fmt.Errorf("decode saga items for release: %w", lerr)
		}
		for _, it := range legacy {
			if it.Quantity <= 0 || it.SKU == "" {
				continue
			}
			payload, perr := json.Marshal(sagaev.StockReleaseRequestedPayload{
				OrderID:       s.OrderID,
				ReservationID: s.ReservationID,
				SKU:           it.SKU,
				Quantity:      it.Quantity,
			})
			if perr != nil {
				return fmt.Errorf("marshal StockReleaseRequested: %w", perr)
			}
			if err := writer.Append(ctx, tx, platformoutbox.Record{
				EventID:       uuid.NewString(),
				AggregateID:   s.OrderID,
				AggregateType: "Order",
				EventType:     "StockReleaseRequested",
				SchemaVersion: "1.0",
				Topic:         sagaoutbox.Topic,
				Payload:       payload,
				Headers:       map[string]string{},
			}); err != nil {
				return err
			}
		}
		return nil
	}
	for _, it := range items {
		if it.Quantity <= 0 || it.SKU == "" {
			continue
		}
		payload, perr := json.Marshal(sagaev.StockReleaseRequestedPayload{
			OrderID:       s.OrderID,
			ReservationID: it.ReservationID,
			SKU:           it.SKU,
			Quantity:      it.Quantity,
		})
		if perr != nil {
			return fmt.Errorf("marshal StockReleaseRequested: %w", perr)
		}
		if err := writer.Append(ctx, tx, platformoutbox.Record{
			EventID:       uuid.NewString(),
			AggregateID:   s.OrderID,
			AggregateType: "Order",
			EventType:     "StockReleaseRequested",
			SchemaVersion: "1.0",
			Topic:         sagaoutbox.Topic,
			Payload:       payload,
			Headers:       map[string]string{},
		}); err != nil {
			return err
		}
	}
	return nil
}
