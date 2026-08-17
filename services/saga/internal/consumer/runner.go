// Package consumer runner: wires pkg/consumer.New for the Saga
// Service. Reads its env config (KAFKA_BROKER, GROUP_ID), creates
// the franz-go client, starts the consumer in a goroutine, and
// returns a shutdown handle.
//
// The saga runtime consumes order-events from all sibling services
// (order, inventory, payment) — every OrderCreated starts a saga;
// every subsequent StockReserved / PaymentCompleted / PaymentFailed
// advances it. The single-topic, single-consumer-group approach
// matches the existing order/inventory/payment runners.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// Topic is the single Kafka topic the Saga Service reads from and
// publishes to. Single-topic simplifies ordering: events for the
// same order_id land on the same partition (Kafka key = order_id).
const Topic = "order-events"

// Start wires the Saga Service consumer. Returns a shutdown func
// that closes the underlying franz-go client. When kafkaBroker or
// groupID are unset (e.g. local 'go run' without docker-compose),
// returns a no-op shutdown and a nil error — the same disabled
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
		Topics:  []string{Topic},
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
