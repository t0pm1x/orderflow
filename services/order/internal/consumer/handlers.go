// Package consumer wires the Order Service's Kafka handler
// registry for the events it consumes. Handlers are intentionally
// stub-only at this stage (3.8.d); the real state-machine logic
// arrives with the saga orchestrator in 3.9.
package consumer

import (
	"context"
	"log/slog"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
)

// Registry returns the Order Service's handler registry. Every
// handler is a stub that logs the event so consumers can be wired
// in main.go without depending on the (still-being-built) saga
// orchestrator.
func Registry(logger *slog.Logger) pkgconsumer.HandlerRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	stub := func(eventType string) pkgconsumer.Handler {
		return func(_ context.Context, env *events.Envelope) error {
			logger.Info("orderflow-order received event",
				"event_type", eventType,
				"event_id", env.EventID,
				"aggregate_id", env.AggregateID,
				"schema_version", env.SchemaVersion,
			)
			return nil
		}
	}
	return pkgconsumer.HandlerRegistry{
		"StockReserved":          stub("StockReserved"),
		"StockReleased":          stub("StockReleased"),
		"StockReservationFailed": stub("StockReservationFailed"),
		"PaymentCompleted":       stub("PaymentCompleted"),
		"PaymentFailed":          stub("PaymentFailed"),
	}
}
