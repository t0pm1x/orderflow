// Package kafkatail wraps pkg/consumer with a tiny adapter that
// publishes every Kafka record on the orderflow event topics to
// the in-process bus.
package kafkatail

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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

// Health reports whether the Kafka tail consumer is currently
// connected. The probe handler reads this for the dashboard's
// Kafka chip. Starts true when a consumer is running; flips
// false on Run error and on graceful shutdown.
var Health atomic.Bool

const groupID = "orderflow-web"

// Start subscribes a Kafka consumer to all orderflow event topics
// with group "orderflow-web" and publishes each Envelope into bus.
// Returns a stop function that blocks until the consumer has
// exited.
//
// brokersCSV may be empty: in that case Start returns (nil, nil)
// and no consumer is started (the web service runs without live
// events, ready to be wired later).
//
// KAFKA-NO-RECONNECT fix: on any Run error (broker restart, network
// blip, group rebalance failure, etc.) the goroutine sleeps and
// re-creates the consumer + retries Run. Without this fix, a single
// transient broker hiccup permanently killed the SSE stream: the
// goroutine returned, no events flowed, but the heartbeat kept the
// UI looking "live". With the loop the tail self-heals within
// 1-15s of a broker recovery.
func Start(ctx context.Context, logger *slog.Logger, brokersCSV string, bus *events.Bus) (func(), error) {
	if brokersCSV == "" {
		logger.Info("kafka tail disabled: KAFKA_BROKERS not set")
		Health.Store(false)
		return nil, nil
	}
	brokers := splitCSV(brokersCSV)
	registry := kafkaconsumer.HandlerRegistry{
		// order-events — saga emits these
		"OrderCreated":          forwardToBus(bus),
		"OrderConfirmed":        forwardToBus(bus),
		"OrderCancelled":        forwardToBus(bus),
		"StockReserveRequested": forwardToBus(bus),
		"StockReleaseRequested": forwardToBus(bus),
		"PaymentRequested":      forwardToBus(bus),
		// inventory-events — inventory service emits these
		"StockReserved":          forwardToBus(bus),
		"StockReservationFailed": forwardToBus(bus),
		// payment-events — payment service emits these
		"PaymentCompleted": forwardToBus(bus),
		"PaymentFailed":    forwardToBus(bus),
	}
	var (
		wg      sync.WaitGroup
		closed  atomic.Bool
		backoff = 1 * time.Second
		maxBack = 15 * time.Second
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		attempt := 0
		for {
			if closed.Load() {
				Health.Store(false)
				return
			}
			Health.Store(true)
			c, err := kafkaconsumer.New(kafkaconsumer.Config{
				Brokers: brokers,
				GroupID: groupID,
				Topics:  topics,
				// DLQ=nil + Deduper=nil: skip retries/dedup; UI just acks.
			}, registry)
			if err != nil {
				attempt++
				logger.Warn("kafka tail: consumer init failed; will retry",
					"err", err, "attempt", attempt, "backoff", backoff.String())
				if !sleepCtx(ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff, maxBack)
				continue
			}
			runErr := c.Run(ctx)
			Health.Store(false)
			// Context cancellation: clean exit on shutdown.
			if closed.Load() || ctx.Err() != nil {
				return
			}
			attempt++
			logger.Warn("kafka tail: consumer exited; will retry",
				"err", runErr, "attempt", attempt, "backoff", backoff.String())
			// Drop the consumer reference — the next iteration
			// builds a fresh one (the old one was already closed
			// by c.Run's Stop on context cancel, but we're past
			// that path here so a fresh consumer is the only
			// safe move).
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBack)
		}
	}()
	stop := func() {
		if !closed.CompareAndSwap(false, true) {
			return
		}
		wg.Wait()
	}
	return stop, nil
}

// sleepCtx waits for d or returns false on ctx cancellation.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff returns min(d*2, maxSleep) for the retry loop.
func nextBackoff(d, maxSleep time.Duration) time.Duration {
	next := d * 2
	if next > maxSleep {
		return maxSleep
	}
	return next
}

func forwardToBus(bus *events.Bus) kafkaconsumer.Handler {
	return func(_ context.Context, env *pkgEvents.Envelope) error {
		// Defensive default: producers across the orderflow stack
		// do not currently populate Envelope.OccurredAt when
		// emitting events, so the field arrives at the bus as the
		// Go zero-time. Without this fallback the per-order
		// timeline page renders every entry as 00:00:00 (the
		// v1.1.3 saga-timeline regression). Producers should set
		// the field at emission time; the consumer-side default
		// keeps the UI usable until each producer is fixed.
		if env.OccurredAt.IsZero() {
			env.OccurredAt = time.Now().UTC()
		}
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
