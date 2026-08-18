// Package order is the Order Service binary entry point. It wires
// the outbox runner (pgxpool + Kafka client + PGSource + Prometheus
// metrics) and blocks until SIGTERM. The HTTP server is mounted by
// the same Start call via pkg/outbox.Config.HTTPAddr; per-service
// REST handlers (POST /v1/orders, etc.) are added on top in a later
// sub-stage.
package order

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

	svchttp "github.com/t0pm1x/orderflow/services/order/internal/api"
	svcconsumer "github.com/t0pm1x/orderflow/services/order/internal/consumer"
	svcoutbox "github.com/t0pm1x/orderflow/services/order/internal/outbox"
	svcrepo "github.com/t0pm1x/orderflow/services/order/internal/repository"
)

// TableName is exported so the cmd/order top-level binary and any
// tests can agree on the table identifier.
const TableName = "order_outbox"

// Version is the binary version (overridden at build via -ldflags).
var Version = "0.0.0-dev"

// boundAddr holds the actual listen address Run is bound to when
// HTTP_ADDR is "127.0.0.1:0" (or any ":0" form). Tests poll
// ListenAddr() after starting Run in a goroutine to discover the
// OS-picked port without hard-coding one.
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
	dbURL := envOrDefault("DATABASE_URL", "")
	broker := envOrDefault("KAFKA_BROKER", "")
	groupID := envOrDefault("KAFKA_GROUP_ID", "orderflow-order")
	httpAddr := envOrDefault("HTTP_ADDR", ":8081")

	// Seed OTLP defaults so pkg/platform/otel.go (which reads them via os.Getenv) dials otel-collector:4317 when no override is set; export OTEL_EXPORTER=stdout for local dev.
	otelExporter := envOrDefault("OTEL_EXPORTER", "otlp")
	otelEndpoint := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	_ = os.Setenv("OTEL_EXPORTER", otelExporter)
	_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", otelEndpoint)

	logger.Info("orderflow-order starting",
		"version", Version,
		"database", redact(dbURL),
		"kafka", broker,
		"kafka_group", groupID,
		"http_addr", httpAddr,
		"table", TableName)

	traceShutdown, err := platform.InitTracing(ctx, TableName, Version)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() { _ = traceShutdown(context.Background()) }()

	outboxClose, pool, err := startOutbox(ctx, logger, dbURL, broker, httpAddr)
	if err != nil {
		return fmt.Errorf("outbox start: %w", err)
	}
	defer func() { _ = outboxClose(context.Background()) }()

	var consumerHandler *svcconsumer.Handler
	if pool != nil {
		consumerHandler = svcconsumer.NewHandler(pool, logger)
	}
	deduper, derr := pkgconsumer.NewDeduperFromRedisURL(envOrDefault("REDIS_URL", ""), "orderflow-order:dedup:", 0)
	if derr != nil {
		logger.Warn("order deduper disabled: bad REDIS_URL", "err", derr)
		deduper = pkgconsumer.NoopDeduper{}
	}
	consumerClose, err := svcconsumer.Start(ctx, logger, broker, groupID, consumerHandler, deduper, nil)
	if err != nil {
		return fmt.Errorf("consumer start: %w", err)
	}
	defer func() { _ = consumerClose(context.Background()) }()

	<-ctx.Done()
	return nil
}

// startOutbox brings up the poller + metrics HTTP server. Returns
// a no-op closeFn when DATABASE_URL or KAFKA_BROKER are unset; the
// HTTP server still starts as long as HTTP_ADDR is non-empty, so
// /healthz and /metrics remain reachable in disabled mode. The
// returned *pgxpool.Pool is non-nil only when the outbox is on
// (both DATABASE_URL and KAFKA_BROKER set); it is exposed so the
// consumer handler can update the orders table on the same pool
// the API uses.
func startOutbox(ctx context.Context, logger *slog.Logger, dbURL, broker, httpAddr string) (func(context.Context) error, *pgxpool.Pool, error) {
	var (
		wg       sync.WaitGroup
		httpSrv  *http.Server
		ln       net.Listener
		pool     *pgxpool.Pool
		poller   *pkgoutbox.Poller
		outboxOn = dbURL != "" && broker != ""
	)

	if outboxOn {
		var err error
		pool, err = pgxpool.New(ctx, dbURL)
		if err != nil {
			return nil, nil, fmt.Errorf("pgxpool: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, nil, fmt.Errorf("postgres ping: %w", err)
		}

		var kafkaClient *events.Client
		kafkaClient, err = events.NewClient(strings.Split(broker, ","), "order")
		if err != nil {
			pool.Close()
			return nil, nil, fmt.Errorf("kafka client: %w", err)
		}

		src := svcoutbox.NewPGSource(pool)
		pub := pkgoutbox.NewKafkaPublisher(kafkaClient)
		dlq := pkgoutbox.NewKafkaDLQ(kafkaClient)
		metrics := pkgoutbox.NewPrometheusMetrics(TableName, prometheus.DefaultRegisterer)

		poller = pkgoutbox.New(pkgoutbox.PollerConfig{
			Table:       TableName,
			BatchSize:   100,
			Interval:    100 * time.Millisecond,
			MaxAttempts: 5,
		}, src, pub, dlq, metrics)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := poller.Run(ctx); err != nil {
				logger.Error("poller exited", "err", err)
			}
		}()
	} else {
		logger.Info("outbox disabled: DATABASE_URL or KAFKA_BROKER not set")
	}

	if httpAddr != "" {
		r := chi.NewRouter()
		r.Use(mw.Stack(TableName, logger)...)
		r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		r.Handle("/metrics", promhttp.Handler())

		// Mount the Order REST handler only when the DB pool is
		// wired (i.e. DATABASE_URL was set). When the outbox is
		// disabled the pool is nil and PGRepo would have no DB to
		// talk to; the handler is intentionally absent so /healthz
		// and /metrics still respond without a DB.
		if pool != nil {
			repo := svcrepo.NewPGRepo(pool)
			r.Mount("/", svchttp.NewHandler(repo).Routes())
		}

		var err error
		ln, err = net.Listen("tcp", httpAddr)
		if err != nil {
			if pool != nil {
				pool.Close()
			}
			return nil, nil, fmt.Errorf("listen %s: %w", httpAddr, err)
		}
		boundAddr.Store(ln.Addr().String())
		httpSrv = &http.Server{Handler: r}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics http exited", "err", err)
			}
		}()
	}

	return func(shutdownCtx context.Context) error {
		if poller != nil {
			poller.Stop()
		}
		if httpSrv != nil {
			_ = httpSrv.Shutdown(shutdownCtx)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
		if pool != nil {
			pool.Close()
		}
		return nil
	}, pool, nil
}

// Main is the function called by cmd/order/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := Run(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "order service: %v\n", err)
		os.Exit(1)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
