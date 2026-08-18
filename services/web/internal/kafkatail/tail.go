// Package kafkatail wraps pkg/consumer with a tiny adapter that
// publishes every Kafka record on order-events to the in-process bus.
package kafkatail

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	kafkaconsumer "github.com/t0pm1x/orderflow/consumer"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
)

const (
	topic   = "order-events"
	groupID = "orderflow-web"
)

// Start subscribes a Kafka consumer to "order-events" with group
// "orderflow-web" and publishes each Envelope into bus. Returns a
// stop function that blocks until the consumer has exited.
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
		Topics:  []string{topic},
		// DLQ=nil + Deduper=nil: skip retries/dedup; UI just acks.
	}, kafkaconsumer.HandlerRegistry{
		"OrderCreated":            forwardToBus(bus),
		"OrderConfirmed":          forwardToBus(bus),
		"OrderCancelled":          forwardToBus(bus),
		"StockReserveRequested":   forwardToBus(bus),
		"StockReleaseRequested":   forwardToBus(bus),
		"PaymentRequested":        forwardToBus(bus),
		"OrderUpdated":            forwardToBus(bus),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka tail: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil {
			logger.Error("kafka tail exited", "err", err)
		}
	}()
	stop := func() {
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