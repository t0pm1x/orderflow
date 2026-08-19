// Package kafkatail wraps pkg/consumer with a tiny adapter that
// publishes every Kafka record on the orderflow event topics to
// the in-process bus.
package kafkatail

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	kafkaconsumer "github.com/t0pm1x/orderflow/consumer"
	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
)

// All three orderflow event topics — the web UI's live-events
// sidebar should surface events from every service, not just the
// ones the saga emits to order-events. v1.1.5 widened the tail
// from just order-events to all three; the pre-v1.1.5 tail missed
// StockReserved / StockReservationFailed (inventory-events) and
// PaymentCompleted / PaymentFailed (payment-events).
var topics = []string{
	"order-events",
	"payment-events",
	"inventory-events",
}

const groupID = "orderflow-web"

// Start subscribes a Kafka consumer to all orderflow event topics
// with group "orderflow-web" and publishes each Envelope into bus.
// Returns a stop function that blocks until the consumer has
// exited.
//
// brokersCSV may be empty: in that case Start returns (nil, nil)
// and no consumer is started (the web service runs without live
// events, ready to be wired later).
func Start(ctx context.Context, logger *slog.Logger, brokersCSV string, bus *events.Bus) (func(), error) {
	if brokersCSV == "" {
		logger.Info("kafka tail disabled: KAFKA_BROKERS not set")
		return nil, nil
	}
	brokers := splitCSV(brokersCSV)
	c, err := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers: brokers,
		GroupID: groupID,
		Topics:  topics,
		// DLQ=nil + Deduper=nil: skip retries/dedup; UI just acks.
	}, kafkaconsumer.HandlerRegistry{
		// order-events — saga emits these
		"OrderCreated":          forwardToBus(bus),
		"OrderConfirmed":        forwardToBus(bus),
		"OrderCancelled":        forwardToBus(bus),
		"StockReserveRequested": forwardToBus(bus),
		"StockReleaseRequested": forwardToBus(bus),
		"PaymentRequested":      forwardToBus(bus),
		// inventory-events — inventory service emits these
		"StockReserved":             forwardToBus(bus),
		"StockReservationFailed":    forwardToBus(bus),
		// payment-events — payment service emits these
		"PaymentCompleted":          forwardToBus(bus),
		"PaymentFailed":             forwardToBus(bus),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka tail: %w", err)
	}
	var wg sync.WaitGroup
	var closed atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil && !closed.Load() {
			logger.Error("kafka tail exited", "err", err)
		}
	}()
	stop := func() {
		if !closed.CompareAndSwap(false, true) {
			return
		}
		c.Stop()
		wg.Wait()
	}
	return stop, nil
}

func forwardToBus(bus *events.Bus) kafkaconsumer.Handler {
	return func(_ context.Context, env *pkgEvents.Envelope) error {
		bus.Publish(events.BusEvent{Envelope: *env})
		return nil
	}
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, ch := range s {
		if ch == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(ch)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
