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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform/events"

	svcoutbox "github.com/t0pm1x/orderflow/services/order/internal/outbox"
)

// TableName is exported so the cmd/order top-level binary and any
// tests can agree on the table identifier.
const TableName = "order_outbox"

// Version is the binary version (overridden at build via -ldflags).
var Version = "0.0.0-dev"

// Run blocks until ctx is cancelled (SIGTERM/SIGINT). Returns nil
// on clean shutdown.
func Run(ctx context.Context) error {
	logger := slog.Default()
	dbURL := envOrDefault("DATABASE_URL", "")
	broker := envOrDefault("KAFKA_BROKER", "")
	httpAddr := envOrDefault("HTTP_ADDR", ":8081")

	logger.Info("orderflow-order starting",
		"version", Version,
		"database", redact(dbURL),
		"kafka", broker,
		"http_addr", httpAddr,
		"table", TableName)

	closeFn, err := startOutbox(ctx, logger, dbURL, broker, httpAddr)
	if err != nil {
		return fmt.Errorf("outbox start: %w", err)
	}
	defer func() {
		_ = closeFn(context.Background())
	}()
	<-ctx.Done()
	return nil
}

// startOutbox brings up the poller + metrics HTTP server. Returns
// nil (no-op) closeFn when DATABASE_URL or KAFKA_BROKER are unset.
func startOutbox(ctx context.Context, logger *slog.Logger, dbURL, broker, httpAddr string) (func(context.Context) error, error) {
	if dbURL == "" || broker == "" {
		logger.Info("outbox disabled: DATABASE_URL or KAFKA_BROKER not set")
		return func(context.Context) error { return nil }, nil
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	kafkaClient, err := events.NewClient(strings.Split(broker, ","), "order")
	if err != nil {
		pool.Close()
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := poller.Run(ctx); err != nil {
			logger.Error("poller exited", "err", err)
		}
	}()

	var httpSrv *http.Server
	if httpAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		httpSrv = &http.Server{Addr: httpAddr, Handler: mux}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics http exited", "err", err)
			}
		}()
	}

	return func(shutdownCtx context.Context) error {
		poller.Stop()
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
		pool.Close()
		return nil
	}, nil
}

// Main is the function called by cmd/order/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx); err != nil {
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
