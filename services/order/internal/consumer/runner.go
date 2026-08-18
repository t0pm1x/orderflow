// Package consumer runner: wires pkg/consumer.New for the Order
// Service. Reads its env config (KAFKA_BROKER, GROUP_ID), creates
// the franz-go client, starts the consumer in a goroutine, and
// returns a shutdown handle.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
)

// Start wires the Order Service consumer. Returns a shutdown func
// that closes the underlying franz-go client and waits for the
// consumer goroutine to exit. When kafkaBroker or groupID are
// unset (e.g. local 'go run' without docker-compose), or when
// handler is nil (DATABASE_URL unset → no pool to update the orders
// table with), returns a no-op shutdown and a nil error.
//
// wg tracks the consumer goroutine so the caller can wait for clean
// shutdown — without it, main exits while the consumer is still
// processing (a real bug under Kubernetes rolling deploys).
// deduper may be nil — pkgconsumer.New substitutes a NoopDeduper.
func Start(ctx context.Context, logger *slog.Logger, kafkaBroker, groupID string, handler *Handler, deduper pkgconsumer.Deduper, wg *sync.WaitGroup) (func(context.Context) error, error) {
	if kafkaBroker == "" || groupID == "" || handler == nil {
		logger.Info("order consumer disabled: KAFKA_BROKER, GROUP_ID, or pool not set")
		return func(context.Context) error { return nil }, nil
	}
	if deduper == nil {
		deduper = pkgconsumer.NoopDeduper{}
	}
	if wg == nil {
		wg = &sync.WaitGroup{}
	}

	c, err := pkgconsumer.New(pkgconsumer.Config{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topics:  []string{"order-events", "payment-events", "inventory-events"},
		Deduper: deduper,
	}, handler.Registry())
	if err != nil {
		return nil, fmt.Errorf("order consumer: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil {
			logger.Error("order consumer exited", "err", err)
		}
	}()

	return func(_ context.Context) error {
		c.Stop()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		<-done
		return nil
	}, nil
}
