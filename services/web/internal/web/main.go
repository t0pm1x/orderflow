// Package web hosts the orderflow-web binary's startup/shutdown
// contract. Mirrors services/order/cmd/order/main.go so the
// release story is identical for ops (SIGTERM-aware shutdown,
// structured startup log, environment overrides).
package web

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/handlers"
	"github.com/t0pm1x/orderflow/services/web/internal/kafkatail"
	"github.com/t0pm1x/orderflow/services/web/internal/server"
)

// Version is the binary version (overridden at build via -ldflags
// -X main.Version). 0.0.0-dev is the pre-tag default.
var Version = "0.0.0-dev"

// boundAddr holds the actual listen address the HTTP server is bound
// to when HTTP_ADDR ends in ":0". Tests + the playground smoke
// script poll ListenAddr() to discover the OS-picked port.
var boundAddr atomic.Value

// ListenAddr returns the address the embedded HTTP server is
// currently bound to, or "" if Run has not started yet. Test-only.
func ListenAddr() string {
	v, _ := boundAddr.Load().(string)
	return v
}

// envOrDefault returns the env var named by key or fallback.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// redact returns a redacted view of a secret string for logging.
// Returns "<unset>" when empty, otherwise truncates.
func redact(s string) string {
	if s == "" {
		return "<unset>"
	}
	if len(s) > 12 {
		return s[:6] + "…" + s[len(s)-4:]
	}
	return "***"
}

// Run blocks until ctx is cancelled (SIGTERM/SIGINT). Returns nil on
// clean shutdown. The order of work:
//  1. Init tracing (no-op in this stage; Task 3 wires the actual
//     HTTP server start).
//  2. Block on ctx.
//  3. Return nil.
//
// Future tasks extend this with: HTTP server goroutine (Task 3),
// Kafka tail goroutine (Task 10). Both will use a *sync.WaitGroup
// to wait for shutdown before Run returns, matching the saga
// shutdown pattern.
func Run(ctx context.Context) error {
	logger := slog.Default()

	logger.Info("orderflow-web starting",
		"version", Version,
		"http_addr", envOrDefault("HTTP_ADDR", ":8083"),
		"order_url", redact(envOrDefault("ORDER_URL", "http://localhost:8081")),
		"payment_url", redact(envOrDefault("PAYMENT_URL", "http://localhost:8082")),
		"inventory_url", redact(envOrDefault("INVENTORY_URL", "http://localhost:8083")),
		"kafka_brokers", redact(envOrDefault("KAFKA_BROKERS", "")))

	httpAddr := envOrDefault("HTTP_ADDR", ":8083")
	orderURL := envOrDefault("ORDER_URL", "http://localhost:8081")
	paymentURL := envOrDefault("PAYMENT_URL", "http://localhost:8082")
	inventoryURL := envOrDefault("INVENTORY_URL", "http://localhost:8083")

	bus := events.NewBus()
	defer bus.Close()
	bc := backend.New(nil, orderURL, paymentURL, inventoryURL)
	hSet := handlers.NewSet(bc, bc, bc, bus)

	stopTail, err := kafkatail.Start(ctx, logger, envOrDefault("KAFKA_BROKERS", ""), bus)
	if err != nil {
		return fmt.Errorf("kafka tail: %w", err)
	}
	if stopTail != nil {
		defer stopTail()
	}

	srv := server.New(server.Options{
		Name:         "web",
		Logger:       logger,
		OrderURL:     orderURL,
		PaymentURL:   paymentURL,
		InventoryURL: inventoryURL,
		Handlers:     hSet,
	})
	if err := srv.Start(ctx, httpAddr); err != nil {
		return fmt.Errorf("server start: %w", err)
	}
	boundAddr.Store(srv.Addr())
	return nil
}

// Main is the function called by cmd/web/main.go. It owns the
// exit-code translation; the actual run loop lives in runMain so
// that the defer-cancel scope does NOT contain os.Exit (gocritic
// exitAfterDefer). The non-zero exit code is what entrypoint
// shells look at.
func Main() {
	if err := runMain(); err != nil {
		fmt.Fprintf(os.Stderr, "web service: %v\n", err)
		os.Exit(1)
	}
}

// runMain wires the signal-aware context and runs the service
// until SIGTERM/SIGINT. Returns nil on clean shutdown.
func runMain() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return Run(ctx)
}
