// Package consumer wires the Inventory Service's Kafka handler
// registry. The Inventory Service consumes stock-reservation
// requests (from the saga orchestrator) and emits
// StockReserved/StockReleased/StockReservationFailed.
package consumer

import (
	"context"
	"log/slog"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
)

// Registry returns the Inventory Service's handler registry.
func Registry(logger *slog.Logger) pkgconsumer.HandlerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	stub := func(eventType string) pkgconsumer.Handler {
		return func(_ context.Context, env *events.Envelope) error {
			logger.Info("orderflow-inventory received event",
				"event_type", eventType,
				"event_id", env.EventID,
				"aggregate_id", env.AggregateID,
			)
			return nil
		}
	}
	return pkgconsumer.HandlerRegistry{
		"StockReserveRequested": stub("StockReserveRequested"),
		"StockReleaseRequested": stub("StockReleaseRequested"),
	}
}
