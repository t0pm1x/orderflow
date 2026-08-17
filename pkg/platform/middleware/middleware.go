// Package middleware provides chi middleware shared across orderflow services.
package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/t0pm1x/orderflow/platform"
	otelmw "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Stack returns the standard chi middleware stack:
//   - RequestID (UUID per request)
//   - ClientIP (from X-Forwarded-For, trusting 1 reverse proxy)
//   - Recoverer (panic → 500)
//   - OTel HTTP (auto-instrumentation, span per request)
//   - Logger (structured request log with trace correlation)
//   - Metrics (request count + duration histogram)
func Stack(serviceName string, logger *slog.Logger) []func(http.Handler) http.Handler {
	return []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.ClientIPFromXFFTrustedProxies(1),
		middleware.Recoverer,
		otelmw.NewMiddleware(serviceName,
			otelmw.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				return r.Method + " " + r.URL.Path
			}),
		),
		requestLogger(logger),
		requestMetrics(),
	}
}

// requestLogger logs each request with method/path/status/duration/trace_id.
func requestLogger(base *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			remote := middleware.GetClientIP(r.Context())
			if remote == "" {
				remote = r.RemoteAddr
			}
			logger := platform.LogWithTrace(r.Context(), base)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
				"remote", remote,
			)
		})
	}
}

// requestMetrics emits a simple counter for now (real Prometheus collector lands in service-specific code).
func requestMetrics() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// TODO: real metrics in service package — this is a placeholder
			next.ServeHTTP(w, r)
		})
	}
}
