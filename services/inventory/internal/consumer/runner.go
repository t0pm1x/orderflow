// Package consumer runner for the Inventory Service.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// Start wires the Inventory Service consumer. Consumes from the
// order-events topic (StockReserveRequested/StockReleaseRequested
// are published by the saga). deduper may be nil — pkgconsumer.New
// substitutes a NoopDeduper.
func Start(ctx context.Context, logger *slog.Logger, kafkaBroker, groupID string, deduper pkgconsumer.Deduper) (func(context.Context) error, error) {
	if kafkaBroker == "" || groupID == "" {
		logger.Info("inventory consumer disabled: KAFKA_BROKER or GROUP_ID not set")
		return func(context.Context) error { return nil }, nil
	}
	if deduper == nil {
		deduper = pkgconsumer.NoopDeduper{}
	}

	c, err := pkgconsumer.New(pkgconsumer.Config{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topics:  []string{"order-events"},
		Deduper: deduper,
	}, Registry(logger))
	if err != nil {
		return nil, fmt.Errorf("inventory consumer: %w", err)
	}

	go func() {
		if err := c.Run(ctx); err != nil {
			logger.Error("inventory consumer exited", "err", err)
		}
	}()

	return func(context.Context) error { c.Stop(); return nil }, nil
}
