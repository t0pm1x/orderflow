package outbox

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// PrometheusMetrics is the production Metrics implementation. It
// records poll duration + row counts, publish success/failure, DLQ
// transitions, and outbox lag. All collectors carry a "table" label
// so a single Prometheus scrape covers all services.
//
// Register the returned collectors with your service's *prometheus.Registry
// (or use the default registry via prometheus.MustRegister).
type PrometheusMetrics struct {
	Table string

	pollDuration *prometheus.HistogramVec
	pollRows     *prometheus.CounterVec
	publish      *prometheus.CounterVec
	dlq          *prometheus.CounterVec
}

// NewPrometheusMetrics constructs and registers the collectors with
// reg. Pass prometheus.DefaultRegisterer if you don't have a custom
// registry.
func NewPrometheusMetrics(table string, reg prometheus.Registerer) *PrometheusMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &PrometheusMetrics{
		Table: table,
		pollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "outbox_poll_duration_seconds",
			Help:    "Time spent fetching a batch of PENDING outbox rows.",
			Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
		}, []string{"table", "outcome"}),
		pollRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_poll_rows_total",
			Help: "Total rows fetched from the outbox (any state).",
		}, []string{"table"}),
		publish: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_publish_total",
			Help: "Total outbox records handed to the publisher.",
		}, []string{"table", "outcome"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_dlq_total",
			Help: "Total outbox records routed to DLQ.",
		}, []string{"table"}),
	}
	reg.MustRegister(m.pollDuration, m.pollRows, m.publish, m.dlq)
	return m
}

// ObservePoll records one poll cycle.
func (m *PrometheusMetrics) ObservePoll(_ context.Context, rows int, dur time.Duration, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.pollDuration.WithLabelValues(m.Table, outcome).Observe(dur.Seconds())
	if rows > 0 {
		m.pollRows.WithLabelValues(m.Table).Add(float64(rows))
	}
}

// ObservePublish records one publish batch.
func (m *PrometheusMetrics) ObservePublish(_ context.Context, count int, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.publish.WithLabelValues(m.Table, outcome).Add(float64(count))
}

// ObserveDLQ records one DLQ transition.
func (m *PrometheusMetrics) ObserveDLQ(_ context.Context, _ outbox.Record, _ string) {
	m.dlq.WithLabelValues(m.Table).Inc()
}

// Compile-time interface check.
var _ Metrics = (*PrometheusMetrics)(nil)
