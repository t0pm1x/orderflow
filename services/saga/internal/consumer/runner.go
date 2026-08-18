// Package consumer runner: wires pkg/consumer.New for the Saga
// Service. Reads its env config (KAFKA_BROKER, GROUP_ID), creates
// the franz-go client, starts the consumer in a goroutine, and
// returns a shutdown handle.
//
// The saga runtime consumes order-events and inventory-events.
// order-events: OrderCreated (start), PaymentCompleted (-> OrderConfirmed),
// PaymentFailed (-> compensation), OrderCancelled (ack).
// inventory-events: StockReserved (-> PaymentRequested),
// StockReservationFailed (-> OrderCancelled), StockReleased (audit).
//
// PaymentRequested lands on order-events (the saga publishes there
// via its outbox poller), so the saga does not subscribe to
// payment-events directly. The full spec alignment is left for
// v1.1; single-topic per service keeps the wire simple.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

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
func Start(ctx context.Context, logger *slog.Logger, kafkaBroker, groupID string, pool *pgxpool.Pool) (func(context.Context) error, error) {
	if kafkaBroker == "" || groupID == "" || pool == nil {
		logger.Info("saga consumer disabled: KAFKA_BROKER, GROUP_ID, or pool not set")
		return func(context.Context) error { return nil }, nil
	}

	h := NewHandler(pool, logger)

	c, err := pkgconsumer.New(pkgconsumer.Config{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topics: []string{
			"order-events",
			"inventory-events",
		},
	}, h.Registry())
	if err != nil {
		return nil, fmt.Errorf("saga consumer: %w", err)
	}

	go func() {
		if err := c.Run(ctx); err != nil {
			logger.Error("saga consumer exited", "err", err)
		}
	}()

	return func(context.Context) error { c.Stop(); return nil }, nil
}
