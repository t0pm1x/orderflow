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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	mw "github.com/t0pm1x/orderflow/platform/middleware"
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

	logger.Info("orderflow-saga starting",
		"version", Version,
		"http_addr", httpAddr,
		"table", TableName)

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
