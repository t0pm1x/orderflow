// Package consumer runner: wires pkg/consumer.New for the Saga
// Service. It reads its env config (KAFKA_BROKER, GROUP_ID), creates
// the franz-go client, starts the consumer in a goroutine, and
// returns a shutdown handle.
//
// The saga runtime consumes three topics:
//   - order-events:    OrderCreated (start saga),
//                      OrderCancelled (ack), PaymentRequested (audit)
//   - inventory-events: StockReserved (-> PaymentRequested),
//                      StockReservationFailed (-> OrderCancelled),
//                      StockReleased (audit)
//   - payment-events:  PaymentCompleted (-> OrderConfirmed),
//                      PaymentFailed (-> compensation)
//
// PaymentRequested is published by the saga itself (saga_outbox
// topic = order-events), so the consumer for it is the audit-only
// path — the saga doesn't act on it because it's the author.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// Topic is the topic the Saga Service publishes all its outgoing
// events to. StockReserveRequested / PaymentRequested / OrderConfirmed
// / StockReleaseRequested / OrderCancelled all go to order-events.
const Topic = "order-events"

// Start wires the Saga Service consumer. Returns a shutdown func
// that closes the underlying franz-go client. When kafkaBroker or
// groupID are unset (e.g. local 'go run' without docker-compose),
// returns a no-op shutdown and a nil error -- the same disabled
// pattern services/{order,payment,inventory}/internal/consumer use.
// deduper may be nil — pkgconsumer.New substitutes a NoopDeduper.
// wg tracks the consumer goroutine for graceful shutdown.
func Start(ctx context.Context, logger *slog.Logger, kafkaBroker, groupID string, pool *pgxpool.Pool, deduper pkgconsumer.Deduper, wg *sync.WaitGroup) (func(context.Context) error, error) {
	if kafkaBroker == "" || groupID == "" || pool == nil {
		logger.Info("saga consumer disabled: KAFKA_BROKER, GROUP_ID, or pool not set")
		return func(context.Context) error { return nil }, nil
	}
	if deduper == nil {
		deduper = pkgconsumer.NoopDeduper{}
	}
	if wg == nil {
		wg = &sync.WaitGroup{}
	}

	h := NewHandler(pool, logger)

	c, err := pkgconsumer.New(pkgconsumer.Config{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topics: []string{
			"order-events",
			"inventory-events",
			"payment-events",
		},
		Deduper: deduper,
	}, h.Registry())
	if err != nil {
		return nil, fmt.Errorf("saga consumer: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil {
			logger.Error("saga consumer exited", "err", err)
		}
	}()

	return func(_ context.Context) error {
		c.Stop()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		<-done
		return nil
	}, nil
}
