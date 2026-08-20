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
	"github.com/redis/go-redis/v9"

	pkgconsumer "github.com/t0pm1x/orderflow/consumer"
	pkgoutbox "github.com/t0pm1x/orderflow/outbox"
	"github.com/t0pm1x/orderflow/platform"
	"github.com/t0pm1x/orderflow/platform/events"
	mw "github.com/t0pm1x/orderflow/platform/middleware"

	svcconsumer "github.com/t0pm1x/orderflow/services/payment/internal/consumer"
	svcidem "github.com/t0pm1x/orderflow/services/payment/internal/idempotency"
	svcoutbox "github.com/t0pm1x/orderflow/services/payment/internal/outbox"
	svcrepo "github.com/t0pm1x/orderflow/services/payment/internal/repository"
	svcwebhook "github.com/t0pm1x/orderflow/services/payment/internal/webhook"
)

// TableName is the outbox table the Payment Service owns. Exported
// so the cmd/payment top-level binary and any tests can agree on the
// table identifier.
const TableName = "payment_outbox"

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
	dbURL := envOrDefault("DATABASE_URL", "")
	brokers := kafkaBrokers()
	broker := strings.Join(brokers, ",")
	groupID := envOrDefault("KAFKA_GROUP_ID", "orderflow-payment")
	httpAddr := envOrDefault("HTTP_ADDR", ":8082")

	// Seed OTLP defaults so pkg/platform/otel.go (which reads them via os.Getenv) dials otel-collector:4317 when no override is set; export OTEL_EXPORTER=stdout for local dev.
	otelExporter := envOrDefault("OTEL_EXPORTER", "otlp")
	otelEndpoint := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	_ = os.Setenv("OTEL_EXPORTER", otelExporter)
	_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", otelEndpoint)

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

	outboxClose, err := startOutbox(ctx, logger, dbURL, brokers, httpAddr)
	if err != nil {
		return fmt.Errorf("outbox start: %w", err)
	}
	defer func() { _ = outboxClose(context.Background()) }()

	deduper, derr := pkgconsumer.NewDeduperFromRedisURL(envOrDefault("REDIS_URL", ""), "orderflow-payment:dedup:", 0)
	if derr != nil {
		logger.Warn("payment deduper disabled: bad REDIS_URL", "err", derr)
		deduper = pkgconsumer.NoopDeduper{}
	}
	consumerClose, err := svcconsumer.Start(ctx, logger, broker, groupID, deduper, nil)
	if err != nil {
		return fmt.Errorf("consumer start: %w", err)
	}
	defer func() { _ = consumerClose(context.Background()) }()

	<-ctx.Done()
	return nil
}

func startOutbox(ctx context.Context, logger *slog.Logger, dbURL string, brokers []string, httpAddr string) (func(context.Context) error, error) {
	var (
		wg          sync.WaitGroup
		httpSrv     *http.Server
		ln          net.Listener
		pool        *pgxpool.Pool
		kafkaClient *events.Client
		poller      *pkgoutbox.Poller
		outboxOn    = dbURL != "" && len(brokers) > 0
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
		svcconsumer.SetHandler(svcconsumer.NewHandler(pool, logger))
		kafkaClient, err = events.NewClient(brokers, "payment")
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("kafka client: %w", err)
		}
		src := svcoutbox.NewPGSource(pool)
		pub := pkgoutbox.NewKafkaPublisher(kafkaClient)
		dlq := pkgoutbox.NewKafkaDLQ(kafkaClient)
		metrics := pkgoutbox.NewPrometheusMetrics(TableName, prometheus.DefaultRegisterer)
		poller = pkgoutbox.New(pkgoutbox.PollerConfig{
			Table:          TableName,
			BatchSize:      100,
			Interval:       100 * time.Millisecond,
			MaxAttempts:    5,                // poison-message cap
			MaxRetryAge:    15 * time.Minute, // infrastructure-outage cap (OBX-004)
			MaxInterval:    5 * time.Second,  // exponential-backoff cap
			JitterFraction: 0.2,              // ±20% full jitter
		}, src, pub, dlq, metrics)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := poller.Run(ctx); err != nil {
				logger.Error("poller exited", "err", err)
			}
		}()
	} else {
		logger.Info("outbox disabled: DATABASE_URL or KAFKA_BROKERS not set")
	}

	if httpAddr != "" {
		r := chi.NewRouter()
		r.Use(mw.Stack(TableName, logger)...)
		r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		// OBS-1: /readyz returns 200 only when every dependency
		// probe succeeds. Pool and kafkaClient checks are only
		// registered when those subsystems are wired (outboxOn);
		// in disabled mode the endpoint reports 200 with no
		// checks so a service starting without DB/Kafka env vars
		// is still ready.
		var (
			readyzNames  []string
			readyzChecks []mw.Check
		)
		if pool != nil {
			readyzNames = append(readyzNames, "postgres")
			readyzChecks = append(readyzChecks, func(ctx context.Context) error {
				return pool.Ping(ctx)
			})
		}
		if kafkaClient != nil {
			readyzNames = append(readyzNames, "kafka")
			readyzChecks = append(readyzChecks, func(ctx context.Context) error {
				return kafkaClient.Ping(ctx)
			})
		}
		r.Get("/readyz", mw.ReadyHandler(readyzNames, readyzChecks))
		r.Handle("/metrics", promhttp.Handler())

		// Mount the payment webhook only when the DB pool is wired
		// (i.e. DATABASE_URL was set). Without a pool PGRepo would
		// have no DB to talk to; the handler is intentionally absent
		// so /healthz and /metrics still respond without a DB.
		//
		// Idempotency needs Redis: when REDIS_URL is unset the route
		// is mounted without the middleware. That is safe because the
		// saga drives order state from outbox events, not from this
		// endpoint's HTTP response.
		if pool != nil {
			var idemStore *svcidem.Store
			if redisURL := envOrDefault("REDIS_URL", ""); redisURL == "" {
				logger.Info("webhook idempotency disabled: REDIS_URL not set")
			} else if opt, perr := redis.ParseURL(redisURL); perr != nil {
				logger.Warn("webhook idempotency disabled: bad REDIS_URL", "err", perr)
			} else {
				idemStore = svcidem.NewStore(redis.NewClient(opt))
			}
			repo := svcrepo.NewPGRepo(pool)
			r.Mount("/", svcwebhook.NewHandler(repo, idemStore).Routes())
		}

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

// Main is the function called by cmd/payment/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	// OBS-6: install the JSON slog handler as the package default so
	// every downstream slog.Default() call emits parseable JSON to
	// stderr. Before this fix the binaries used Go's stdlib text
	// handler, which downstream log shipping (Tempo/Loki) could not
	// parse without a regex extractor.
	slog.SetDefault(platform.NewLogger())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := Run(ctx)
	cancel()
	if err != nil {
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

func redact(s string) string {
	if s == "" {
		return "<unset>"
	}
	if len(s) > 12 {
		return s[:6] + "…" + s[len(s)-4:]
	}
	return "***"
}
