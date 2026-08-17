// Package saga is the Saga Service binary entry point. The saga
// orchestrator itself lives in services/saga (state.go,
// compensate.go, timeout.go); this package wires the binary:
// HTTP server with chi middleware + Prometheus /metrics.
//
// The saga service is a stub for sub-stage 3.10.d — the HTTP
// surface exposes /healthz and /metrics so the middleware stack
// (OTel HTTP, logger, request metrics) is exercised in CI. Real
// saga runtime (consumer wiring, watchdog) lands in later
// sub-stages.
package saga

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform"
	"github.com/t0pm1x/orderflow/platform/events"
	mw "github.com/t0pm1x/orderflow/platform/middleware"

	svcconsumer "github.com/t0pm1x/orderflow/services/saga/internal/consumer"
	svcoutbox "github.com/t0pm1x/orderflow/services/saga/internal/outbox"
)

// TableName is the outbox table identifier. The saga service does
// not currently use an outbox (saga state lives in order_sagas);
// the constant exists so the chi middleware Stack can label
// metrics + logs consistently with the other orderflow binaries.
const TableName = "saga_outbox"

// Version is the binary version (overridden at build via -ldflags).
var Version = "0.0.0-dev"

// boundAddr is the actual listen address Run is bound to when
// HTTP_ADDR is a ":0" form. Tests poll ListenAddr() after starting
// Run in a goroutine to discover the OS-picked port.
var boundAddr atomic.Value

// ListenAddr returns the address the embedded HTTP server is
// currently bound to, or "" if Run has not started yet. Test-only.
func ListenAddr() string {
	v, _ := boundAddr.Load().(string)
	return v
}

// Run blocks until ctx is cancelled (SIGTERM/SIGINT). Returns nil
// on clean shutdown.
func Run(ctx context.Context) error {
	logger := slog.Default()
	httpAddr := envOrDefault("HTTP_ADDR", ":8084")
	dbURL := envOrDefault("DATABASE_URL", "")
	broker := envOrDefault("KAFKA_BROKER", "")
	groupID := envOrDefault("KAFKA_GROUP_ID", "orderflow-saga")

	// Seed OTLP defaults so pkg/platform/otel.go (which reads them via os.Getenv) dials otel-collector:4317 when no override is set; export OTEL_EXPORTER=stdout for local dev.
	otelExporter := envOrDefault("OTEL_EXPORTER", "otlp")
	otelEndpoint := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	_ = os.Setenv("OTEL_EXPORTER", otelExporter)
	_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", otelEndpoint)

	logger.Info("orderflow-saga starting",
		"version", Version,
		"http_addr", httpAddr,
		"database", redact(dbURL),
		"kafka", broker,
		"kafka_group", groupID,
		"table", TableName)

	traceShutdown, err := platform.InitTracing(ctx, TableName, Version)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() { _ = traceShutdown(context.Background()) }()

	// Bring up the runtime: DB pool + consumer + outbox poller.
	// Disabled (no-op close) when DATABASE_URL or KAFKA_BROKER are
	// unset, mirroring the order/payment/inventory services.
	var (
		pool          *pgxpool.Pool
		consumerClose func(context.Context) error
		outboxClose   func(context.Context) error
	)
	if dbURL != "" && broker != "" {
		pool, err = pgxpool.New(ctx, dbURL)
		if err != nil {
			return fmt.Errorf("pgxpool: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return fmt.Errorf("postgres ping: %w", err)
		}

		consumerClose, err = svcconsumer.Start(ctx, logger, broker, groupID, pool)
		if err != nil {
			pool.Close()
			return fmt.Errorf("consumer start: %w", err)
		}
		defer func() { _ = consumerClose(context.Background()) }()

		outboxClose, err = startSagaOutbox(ctx, logger, pool, broker)
		if err != nil {
			pool.Close()
			return fmt.Errorf("outbox start: %w", err)
		}
		defer func() { _ = outboxClose(context.Background()) }()
	} else {
		logger.Info("saga runtime disabled: DATABASE_URL or KAFKA_BROKER not set")
	}

	if httpAddr == "" {
		logger.Info("http disabled: HTTP_ADDR not set")
		<-ctx.Done()
		return nil
	}

	r := chi.NewRouter()
	r.Use(mw.Stack(TableName, logger)...)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/metrics", promhttp.Handler())

	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		return fmt.Errorf("listen %s: %w", httpAddr, err)
	}
	boundAddr.Store(ln.Addr().String())
	httpSrv := &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	httpErr := make(chan error, 1)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("saga http exited", "err", err)
			httpErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-httpErr:
		return err
	}
}

// startSagaOutbox brings up the Saga Service outbox poller. Returns
// a close fn that stops the poller; the caller owns pool's lifecycle.
// Mirrors services/order/cmd/order/main.go's startOutbox.
func startSagaOutbox(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, broker string) (func(context.Context) error, error) {
	kafkaClient, err := events.NewClient(strings.Split(broker, ","), "saga")
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	src := svcoutbox.NewPGSource(pool)
	pub := pkgoutbox.NewKafkaPublisher(kafkaClient)
	dlq := pkgoutbox.NewKafkaDLQ(kafkaClient)
	metrics := pkgoutbox.NewPrometheusMetrics(TableName, prometheus.DefaultRegisterer)

	poller := pkgoutbox.New(pkgoutbox.PollerConfig{
		Table:       TableName,
		BatchSize:   100,
		Interval:    100 * time.Millisecond,
		MaxAttempts: 5,
	}, src, pub, dlq, metrics)

	go func() {
		if err := poller.Run(ctx); err != nil {
			logger.Error("saga outbox poller exited", "err", err)
		}
	}()

	return func(shutdownCtx context.Context) error {
		poller.Stop()
		kafkaClient.Close()
		return nil
	}, nil
}

func redact(s string) string {
	if s == "" {
		return "<unset>"
	}
	if len(s) > 12 {
		return s[:6] + "…" + s[len(s)-4:]
	}
	return "***"
}

// Main is the function called by cmd/saga/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "saga service: %v\n", err)
		os.Exit(1)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
