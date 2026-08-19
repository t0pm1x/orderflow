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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform"
	"github.com/t0pm1x/orderflow/platform/events"
	mw "github.com/t0pm1x/orderflow/platform/middleware"

	svcconsumer "github.com/t0pm1x/orderflow/services/saga/internal/consumer"
	svcoutbox "github.com/t0pm1x/orderflow/services/saga/internal/outbox"
	svcrepo "github.com/t0pm1x/orderflow/services/saga/internal/repository"
	svcwatchdog "github.com/t0pm1x/orderflow/services/saga/internal/watchdog"
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
	brokers := kafkaBrokers()
	broker := strings.Join(brokers, ",")
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

	// Bring up the runtime: DB pool + consumer + outbox poller + TTL sweep.
	// Disabled (no-op close) when DATABASE_URL or KAFKA_BROKERS are unset,
	// mirroring the order/payment/inventory services.
	var (
		wg            sync.WaitGroup
		pool          *pgxpool.Pool
		httpSrv       *http.Server
		ln            net.Listener
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

		deduper, derr := pkgconsumer.NewDeduperFromRedisURL(envOrDefault("REDIS_URL", ""), "orderflow-saga:dedup:", 0)
		if derr != nil {
			logger.Warn("saga deduper disabled: bad REDIS_URL", "err", derr)
			deduper = pkgconsumer.NoopDeduper{}
		}
		consumerClose, err = svcconsumer.Start(ctx, logger, broker, groupID, pool, deduper, nil)
		if err != nil {
			pool.Close()
			return fmt.Errorf("consumer start: %w", err)
		}

		outboxClose, err = startSagaOutbox(ctx, logger, pool, brokers, &wg)
		if err != nil {
			_ = consumerClose(context.Background())
			pool.Close()
			return fmt.Errorf("outbox start: %w", err)
		}

		// Start cross-restart TTL sweep — compensates sagas whose
		// expires_at has passed but never fired in-process.
		ttl := svcwatchdog.NewTTLSweep(pool, svcrepo.NewPGRepo(pool), svcoutbox.NewPGWriter(), 30*time.Second, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ttl.Run(ctx)
		}()
	} else {
		logger.Info("saga runtime disabled: DATABASE_URL or KAFKA_BROKERS not set")
	}

	if httpAddr == "" {
		logger.Info("http disabled: HTTP_ADDR not set")
		<-ctx.Done()
		wgWait(ctx, &wg, pool, consumerClose, outboxClose, httpSrv)
		return nil
	}

	r := chi.NewRouter()
	r.Use(mw.Stack(TableName, logger)...)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/metrics", promhttp.Handler())

	ln, err = net.Listen("tcp", httpAddr)
	if err != nil {
		listenShutdownCtx, listenCancel := context.WithTimeout(context.Background(), 5*time.Second)
		wgWait(listenShutdownCtx, &wg, pool, consumerClose, outboxClose, nil)
		listenCancel()
		return fmt.Errorf("listen %s: %w", httpAddr, err)
	}
	boundAddr.Store(ln.Addr().String())
	httpSrv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("saga http exited", "err", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wgWait(shutdownCtx, &wg, pool, consumerClose, outboxClose, httpSrv)
	return nil
}

// startSagaOutbox brings up the Saga Service outbox poller. The
// caller passes a WaitGroup so the poller goroutine is tracked and
// the close fn can wait for it on shutdown. Mirrors
// services/order/cmd/order/main.go's startOutbox + WaitGroup pattern.
func startSagaOutbox(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, brokers []string, wg *sync.WaitGroup) (func(context.Context) error, error) {
	kafkaClient, err := events.NewClient(brokers, "saga")
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := poller.Run(ctx); err != nil {
			logger.Error("saga outbox poller exited", "err", err)
		}
	}()

	return func(_ context.Context) error {
		poller.Stop()
		kafkaClient.Close()
		return nil
	}, nil
}

// wgWait blocks until all background goroutines (TTL sweep, outbox
// poller, HTTP server) have exited, OR the shutdown timeout
// expires. The order is: stop the poller (so it does not start new
// fetches), wait for all goroutines, then close the consumer and
// the DB pool. Mirrors services/order/cmd/order/main.go's shutdown
// path.
func wgWait(shutdownCtx context.Context, wg *sync.WaitGroup, pool *pgxpool.Pool, consumerClose, outboxClose func(context.Context) error, httpSrv *http.Server) {
	// Stop sources first so background goroutines exit promptly.
	if outboxClose != nil {
		_ = outboxClose(shutdownCtx)
	}
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
	}

	if consumerClose != nil {
		_ = consumerClose(shutdownCtx)
	}
	if pool != nil {
		pool.Close()
	}
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
	err := Run(ctx)
	cancel()
	if err != nil {
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

// kafkaBrokers returns the list of Kafka bootstrap brokers. Prefers
// KAFKA_BROKERS (CSV, e.g. "host1:9092,host2:9092"); falls back to
// the legacy singular KAFKA_BROKER for back-compat. Returns nil when
// both are unset (service runs in disabled mode).
func kafkaBrokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		raw = os.Getenv("KAFKA_BROKER")
	}
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}
