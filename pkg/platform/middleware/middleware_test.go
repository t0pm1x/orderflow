package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStack_PassesThrough(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	chain := Stack("test", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var final http.Handler = h
	for i := len(chain) - 1; i >= 0; i-- {
		final = chain[i](final)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	final.ServeHTTP(w, req)

	if w.Code != http.StatusTeapot {
		t.Errorf("expected 418, got %d", w.Code)
	}
}

// TestRequestMetrics_IncrementsCounterOnSuccess is the OBS-2
// regression net. Before the fix, requestMetrics() was an empty
// no-op and the http_requests_total counter never incremented;
// Grafana panel 1 was always empty. The test asserts that a single
// GET /v1/orders request through the Stack results in a
// http_requests_total counter labelled (service, method, path,
// status) with value 1.
func TestRequestMetrics_IncrementsCounterOnSuccess(t *testing.T) {
	prevReq := requestCounter
	prevDur := requestDuration
	t.Cleanup(func() {
		requestCounter = prevReq
		requestDuration = prevDur
	})

	requestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests handled, labelled by service, method, path, and status code.",
	}, []string{"service", "method", "path", "status"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds, labelled by service, method, and path.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"service", "method", "path"})

	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	chain := Stack("test-svc", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var final http.Handler = h
	for i := len(chain) - 1; i >= 0; i-- {
		final = chain[i](final)
	}

	req := httptest.NewRequest("GET", "/v1/orders", nil)
	w := httptest.NewRecorder()
	final.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status: got %d want %d", w.Code, http.StatusNoContent)
	}

	got := testutil.ToFloat64(requestCounter.WithLabelValues("test-svc", "GET", "/v1/orders", "204"))
	if got != 1 {
		t.Errorf("http_requests_total{test-svc,GET,/v1/orders,204}: got %v want 1", got)
	}
}

// TestRequestMetrics_RecordsDurationHistogram asserts the histogram
// has at least one observation after a request. The histogram
// exposes count via testutil.CollectAndCount.
func TestRequestMetrics_RecordsDurationHistogram(t *testing.T) {
	prevReq := requestCounter
	prevDur := requestDuration
	t.Cleanup(func() {
		requestCounter = prevReq
		requestDuration = prevDur
	})

	requestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"service", "method", "path", "status"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
	}, []string{"service", "method", "path"})

	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	chain := Stack("svc", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var final http.Handler = h
	for i := len(chain) - 1; i >= 0; i-- {
		final = chain[i](final)
	}

	req := httptest.NewRequest("POST", "/v1/orders", strings.NewReader(""))
	w := httptest.NewRecorder()
	final.ServeHTTP(w, req)

	if got := testutil.CollectAndCount(requestDuration, "http_request_duration_seconds"); got != 1 {
		t.Errorf("histogram child metrics: got %d want 1", got)
	}
}

// TestRequestMetrics_LabelsStatusFromResponse asserts that the
// counter is labelled with the actual response status code, not a
// fixed string. Regression net for a bug where the middleware would
// label every increment as 200 regardless of what the handler
// wrote.
func TestRequestMetrics_LabelsStatusFromResponse(t *testing.T) {
	prevReq := requestCounter
	prevDur := requestDuration
	t.Cleanup(func() {
		requestCounter = prevReq
		requestDuration = prevDur
	})

	requestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"service", "method", "path", "status"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "method", "path"})

	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	chain := Stack("svc", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var final http.Handler = h
	for i := len(chain) - 1; i >= 0; i-- {
		final = chain[i](final)
	}

	req := httptest.NewRequest("GET", "/x", nil)
	w := httptest.NewRecorder()
	final.ServeHTTP(w, req)

	if got := testutil.ToFloat64(requestCounter.WithLabelValues("svc", "GET", "/x", "418")); got != 1 {
		t.Errorf("expected counter labelled 418 to be 1, got %v", got)
	}
}
