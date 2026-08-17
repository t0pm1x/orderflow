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
// emits the same two outbox rows PaymentFailedHandler emits:
// StockReleaseRequested (so inventory releases the reservation)
// and OrderCancelled with reason="timeout" (so the order service
// marks the order cancelled). All three writes happen in one tx so
// the saga state and its events commit/rollback atomically —
// preventing the half-state of "compensated with no events
// emitted" that would leave stock stranded.
func (t *TTLSweep) compensate(ctx context.Context, s *repository.Saga) error {
	return pgx.BeginFunc(ctx, t.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE order_sagas
			    SET state = 'compensated', updated_at = NOW()
			  WHERE order_id = $1`, s.OrderID); err != nil {
			return err
		}
		releaseRec := platformoutbox.Record{
			EventID:       uuid.NewString(),
			AggregateID:   s.OrderID,
			AggregateType: "Order",
			EventType:     "StockReleaseRequested",
			SchemaVersion: "1.0",
			Topic:         sagaoutbox.Topic,
			Payload: mustMarshal(sagaev.StockReleaseRequestedPayload{
				OrderID:       s.OrderID,
				ReservationID: s.ReservationID,
			}),
			Headers: map[string]string{},
		}
		if err := t.writer.Append(ctx, tx, releaseRec); err != nil {
			return err
		}
		cancelRec := platformoutbox.Record{
			EventID:       uuid.NewString(),
			AggregateID:   s.OrderID,
			AggregateType: "Order",
			EventType:     "OrderCancelled",
			SchemaVersion: "1.0",
			Topic:         sagaoutbox.Topic,
			Payload: mustMarshal(sagaev.OrderCancelledPayload{
				OrderID: s.OrderID,
				Reason:  "timeout",
				Source:  "saga",
			}),
			Headers: map[string]string{},
		}
		return t.writer.Append(ctx, tx, cancelRec)
	})
}

// mustMarshal marshals v to JSON, panicking on error. The payloads
// in this file are local structs with primitive fields — a marshal
// failure would indicate a programmer error, not a runtime one,
// so panic is appropriate (and the alternative of threading an
// error through here would obscure the tx commit/rollback
// semantics that matter more).
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
