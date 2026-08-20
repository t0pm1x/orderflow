// Package middleware provides chi middleware shared across orderflow services.
package middleware

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/t0pm1x/orderflow/platform"
	otelmw "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// requestCounter and requestDuration are the package-level
// Prometheus collectors the requestMetrics middleware writes to.
// They are package-level vars (not constants) so tests can swap them
// for fresh, isolated collectors without registering against the
// default global registry. The zero value of *prometheus.CounterVec
// is unusable (panics on WithLabelValues), so init() installs a
// default pair on the global registry on package import; production
// code never sees the swap-in path.
var (
	requestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests handled, labelled by service, method, path, and status code.",
	}, []string{"service", "method", "path", "status"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds, labelled by service, method, and path.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"service", "method", "path"})
)

func init() {
	// Register the default pair on the global registry so that
	// /metrics scrapes see http_requests_total even before any
	// request has been served (Prometheus counts/gauges only
	// appear once a child metric has been incremented at least
	// once).
	prometheus.MustRegister(requestCounter, requestDuration)
}

// Stack returns the standard chi middleware stack:
//   - RequestID (UUID per request)
//   - ClientIP (from X-Forwarded-For, trusting 1 reverse proxy)
//   - Recoverer (panic → 500)
//   - OTel HTTP (auto-instrumentation, span per request)
//   - Logger (structured request log with trace correlation)
//   - Metrics (request count + duration histogram, labelled by serviceName)
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
		requestMetrics(serviceName),
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

// requestMetrics emits http_requests_total{service,method,path,status}
// and http_request_duration_seconds{service,method,path} on every
// request passing through the chi stack. The `service` label binds to
// the Stack's serviceName argument so a Prometheus scrape across all
// orderflow pods can be filtered / grouped by service. The
// `status` label is the integer response code as written by the
// handler (e.g. "204", "418"), captured via chi's WrapResponseWriter
// so handlers that call WriteHeader without a body still register.
func requestMetrics(service string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			status := strconv.Itoa(ww.Status())
			requestCounter.WithLabelValues(service, r.Method, r.URL.Path, status).Inc()
			requestDuration.WithLabelValues(service, r.Method, r.URL.Path).Observe(time.Since(start).Seconds())
		})
	}
}
