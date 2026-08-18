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
		"order_url", redact(envOrDefault("ORDER_URL", "http://localhost:8080")),
		"payment_url", redact(envOrDefault("PAYMENT_URL", "http://localhost:8081")),
		"inventory_url", redact(envOrDefault("INVENTORY_URL", "http://localhost:8082")),
		"kafka_brokers", redact(envOrDefault("KAFKA_BROKERS", "")))

	<-ctx.Done()
	logger.Info("orderflow-web shutting down")
	return nil
}

// Main is the function called by cmd/web/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "web service: %v\n", err)
		os.Exit(1)
	}
}
