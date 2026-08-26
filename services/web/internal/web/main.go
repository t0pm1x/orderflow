// Package web is the orderflow-web binary's startup/shutdown contract.
//
// Run is invoked by cmd/web/main.go. Lifecycle:
//
//  1. Seed OTLP defaults, switch to JSON slog, init OTel tracing.
//  2. Build the event bus + Kafka tail (so the SPA's live-event
//     sidebar has events to forward via /events/stream).
//  3. Start the HTTP server (Go BFF: /api/* proxy, /events/stream
//     SSE, static SPA fallback).
//  4. Block on ctx (SIGTERM-aware), then drain tracing + close bus.
//
// The web binary serves a single-binary artifact: the SvelteKit
// SPA is embedded via //go:embed in internal/server/server.go, so
// no separate static-file container / CDN is required in
// production. Operators only need to build the SvelteKit bundle
// once (npm run build → services/web/frontend/dist/) before
// `go build` so the embed has something to ship.
package web

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/t0pm1x/orderflow/platform"
	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/kafkatail"
	"github.com/t0pm1x/orderflow/services/web/internal/server"
)

// Version is the binary version (overridden at build via -ldflags
// -X github.com/t0pm1x/orderflow/services/web/internal/web.Version=...).
// 0.0.0-dev is the pre-tag default.
var Version = "0.0.0-dev"

// envOrDefault returns the env var named by key or fallback.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// redact returns a redacted view of a secret string for logging.
// Delegates to platform.Redact so every service uses the same
// SHA-256-first-8-hex algorithm.
func redact(s string) string {
	return platform.Redact(s)
}

// Run blocks until ctx is cancelled (SIGTERM/SIGINT). Returns nil on
// clean shutdown.
//
// Order of work:
//  1. Seed OTLP defaults, set JSON slog, init OTel tracing.
//  2. Build event bus, backend HTTP clients.
//  3. Start Kafka tail (with reconnect loop). If KAFKA_BROKERS
//     and KAFKA_BROKER are both unset the tail is a no-op and
//     /events/stream returns 503 (the SPA sidebar shows "Live
//     events: disconnected").
//  4. Start the HTTP server (SvelteKit SPA + /api/* proxy +
//     /events/stream SSE).
//  5. Block on ctx, then flush tracing + close bus.
//
// NO-TRACING / NO-JSON-LOGGER / NO-OTLP-DEFAULTS fix: web now
// matches the four backend services — JSON slog, OTLP exporter
// defaults, OTel tracer provider with service.name="web" +
// service.version=Version, and a defer that flushes spans on
// shutdown. Pre-fix, web spans had no resource attributes so the
// Tempo service map couldn't identify them, and the default
// text-format slog made Loki log parsing impossible.
func Run(ctx context.Context) error {
	if os.Getenv("OTEL_EXPORTER") == "" {
		_ = os.Setenv("OTEL_EXPORTER", "otlp")
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	}
	slog.SetDefault(platform.NewLogger())
	logger := slog.Default()

	logger.Info("orderflow-web starting",
		"version", Version,
		"http_addr", envOrDefault("HTTP_ADDR", ":8085"),
		"order_url", redact(envOrDefault("ORDER_URL", "http://localhost:8081")),
		"payment_url", redact(envOrDefault("PAYMENT_URL", "http://localhost:8082")),
		"inventory_url", redact(envOrDefault("INVENTORY_URL", "http://localhost:8083")),
		"kafka_brokers", redact(envOrDefault("KAFKA_BROKERS", "")))

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

	// KAFKA-BROKER-NAME-MISMATCH fix: accept either KAFKA_BROKER
	// (singular, the legacy convention used by tests/chaos and the
	// v1.1.x Helm charts) or KAFKA_BROKERS (plural, the new
	// convention). Prefer the singular if set (the plural would
	// be empty in any of the test paths that still set only the
	// singular).
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "")
	if singular := os.Getenv("KAFKA_BROKER"); singular != "" && kafkaBrokers == "" {
		kafkaBrokers = singular
	}
	stopTail, err := kafkatail.Start(ctx, logger, kafkaBrokers, bus)
	if err != nil {
		return fmt.Errorf("kafka tail: %w", err)
	}
	if stopTail != nil {
		defer stopTail()
	}

	srv := server.New(server.Options{
		Name:         "web",
		Logger:       logger,
		Order:        bc,
		Payment:      bc,
		Inventory:    bc,
		Bus:          bus,
		EventsEnabled: stopTail != nil,
	})
	if err := srv.Start(ctx, httpAddr); err != nil {
		return fmt.Errorf("server start: %w", err)
	}
	return nil
}

// Main is the function called by cmd/web/main.go. It owns the
// exit-code translation; the actual run loop lives in runMain so
// the defer-cancel scope does NOT contain os.Exit (gocritic
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
