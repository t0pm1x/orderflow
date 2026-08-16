// Package consumer runner: wires pkg/consumer.New for the Order
// Service. Reads its env config (KAFKA_BROKER, GROUP_ID), creates
// the franz-go client, starts the consumer in a goroutine, and
// returns a shutdown handle.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// Start wires the Order Service consumer. Returns a shutdown func
// that closes the underlying franz-go client. When kafkaBroker or
// groupID are unset (e.g. local 'go run' without docker-compose),
// returns a no-op shutdown and a nil error.
func Start(ctx context.Context, logger *slog.Logger, kafkaBroker, groupID string) (func(context.Context) error, error) {
	if kafkaBroker == "" || groupID == "" {
		logger.Info("order consumer disabled: KAFKA_BROKER or GROUP_ID not set")
		return func(context.Context) error { return nil }, nil
	}

	c, err := pkgconsumer.New(pkgconsumer.Config{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topics:  []string{"payment-events", "inventory-events"},
	}, Registry(logger))
	if err != nil {
		return nil, fmt.Errorf("order consumer: %w", err)
	}

	go func() {
		if err := c.Run(ctx); err != nil {
			logger.Error("order consumer exited", "err", err)
		}
	}()

	return func(context.Context) error { c.Stop(); return nil }, nil
}
