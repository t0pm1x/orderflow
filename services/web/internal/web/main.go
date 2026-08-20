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
	"time"

	"github.com/t0pm1x/orderflow/platform"
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
//  1. Seed OTLP defaults, set JSON slog default, init OTel tracing.
//  2. Build the event bus, BFF backend client, handler set.
//  3. Start the Kafka tail (with reconnect loop; KAFKA-NO-RECONNECT).
//  4. Start the HTTP server.
//  5. Block on ctx, then shut everything down.
//
// NO-TRACING / NO-JSON-LOGGER / NO-OTLP-DEFAULTS fix: web now
// matches the four backend services — JSON slog, OTLP exporter
// defaults, OTel tracer provider with service.name="web" +
// service.version=Version, and a defer that flushes spans on
// shutdown. Pre-fix, web spans had no resource attributes so the
// Tempo service map couldn't identify them, and the default
// text-format slog made Loki log parsing impossible.
func Run(ctx context.Context) error {
	// OTLP defaults: only set when not already present so an
	// operator's explicit OTEL_EXPORTER=stdout still wins.
	if os.Getenv("OTEL_EXPORTER") == "" {
		_ = os.Setenv("OTEL_EXPORTER", "otlp")
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	}
	// JSON default logger so structured-log pipelines (Loki,
	// Datadog, etc.) can ingest web's stdout.
	slog.SetDefault(platform.NewLogger())
	logger := slog.Default()

	logger.Info("orderflow-web starting",
		"version", Version,
		"http_addr", envOrDefault("HTTP_ADDR", ":8085"),
		"order_url", redact(envOrDefault("ORDER_URL", "http://localhost:8081")),
		"payment_url", redact(envOrDefault("PAYMENT_URL", "http://localhost:8082")),
		"inventory_url", redact(envOrDefault("INVENTORY_URL", "http://localhost:8083")),
		"kafka_brokers", redact(envOrDefault("KAFKA_BROKERS", "")))

	// Init tracing — sets service.name="web" + service.version=Version
	// on every span emitted from this process. Returns a shutdown
	// func that flushes pending spans; we defer it so SIGTERM
	// doesn't drop telemetry.
	shutdownTracing, err := platform.InitTracing(ctx, "web", Version)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	httpAddr := envOrDefault("HTTP_ADDR", ":8085")
	orderURL := envOrDefault("ORDER_URL", "http://localhost:8081")
	paymentURL := envOrDefault("PAYMENT_URL", "http://localhost:8082")
	inventoryURL := envOrDefault("INVENTORY_URL", "http://localhost:8083")

	bus := events.NewBus()
	defer bus.Close()
	bc := backend.New(nil, orderURL, paymentURL, inventoryURL)
	hSet := handlers.NewSet(bc, bc, bc, bus, logger)

	// KAFKA-BROKER-NAME-MISMATCH fix: accept either KAFKA_BROKER
	// (singular, the legacy convention used by the tests/chaos
	// harness and the v1.1.x Helm charts) or KAFKA_BROKERS
	// (plural, the new convention). Prefer the singular if set
	// (the plural would be empty in any of the test paths that
	// still set only the singular).
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "")
	if singular := os.Getenv("KAFKA_BROKER"); singular != "" && kafkaBrokers == "" {
		kafkaBrokers = singular
	}
	stopTail, err := kafkatail.Start(ctx, logger, kafkaBrokers, bus)
	if err != nil {
		return fmt.Errorf("kafka tail: %w", err)
	}
	hSet.SetEventsEnabled(stopTail != nil)
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
