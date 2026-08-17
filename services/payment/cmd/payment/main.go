// Package payment is the Payment Service binary entry point.
package payment

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

	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform"
	"github.com/t0pm1x/orderflow/platform/events"
	mw "github.com/t0pm1x/orderflow/platform/middleware"

	svcconsumer "github.com/t0pm1x/orderflow/services/payment/internal/consumer"
	svcoutbox "github.com/t0pm1x/orderflow/services/payment/internal/outbox"
)

const TableName = "payment_outbox"

var Version = "0.0.0-dev"

// boundAddr is the actual listen address Run is bound to when
// HTTP_ADDR is a ":0" form. Tests poll ListenAddr() after starting
// Run in a goroutine to discover the OS-picked port.
var boundAddr atomic.Value

func ListenAddr() string {
	v, _ := boundAddr.Load().(string)
	return v
}

func Run(ctx context.Context) error {
	logger := slog.Default()
	dbURL := envOrDefault("DATABASE_URL", "")
	broker := envOrDefault("KAFKA_BROKER", "")
	groupID := envOrDefault("KAFKA_GROUP_ID", "orderflow-payment")
	httpAddr := envOrDefault("HTTP_ADDR", ":8082")

	logger.Info("orderflow-payment starting",
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

	outboxClose, err := startOutbox(ctx, logger, dbURL, broker, httpAddr)
	if err != nil {
		return fmt.Errorf("outbox start: %w", err)
	}
	defer func() { _ = outboxClose(context.Background()) }()

	consumerClose, err := svcconsumer.Start(ctx, logger, broker, groupID)
	if err != nil {
		return fmt.Errorf("consumer start: %w", err)
	}
	defer func() { _ = consumerClose(context.Background()) }()

	<-ctx.Done()
	return nil
}

func startOutbox(ctx context.Context, logger *slog.Logger, dbURL, broker, httpAddr string) (func(context.Context) error, error) {
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
			return nil, fmt.Errorf("pgxpool: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres ping: %w", err)
		}
		var kafkaClient *events.Client
		kafkaClient, err = events.NewClient(strings.Split(broker, ","), "payment")
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("kafka client: %w", err)
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

		var err error
		ln, err = net.Listen("tcp", httpAddr)
		if err != nil {
			if pool != nil {
				pool.Close()
			}
			return nil, fmt.Errorf("listen %s: %w", httpAddr, err)
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
	}, nil
}

func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "payment service: %v\n", err)
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
