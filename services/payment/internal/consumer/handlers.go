// Package consumer wires the Payment Service's Kafka handler
// registry. The Payment Service consumes PaymentRequested (from
// the saga orchestrator) and emits PaymentCompleted/PaymentFailed.
package consumer

import (
	"context"
	"log/slog"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/platform/events"
)

// Registry returns the Payment Service's handler registry.
func Registry(logger *slog.Logger) pkgconsumer.HandlerRegistry {
	if logger == nil {
		logger = slog.Default()
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
