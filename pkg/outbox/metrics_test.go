package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

func TestPrometheusMetrics_ObservePoll(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics("order_outbox", reg)
	m.ObservePoll(context.Background(), 5, 12*time.Millisecond, nil)

	// Histograms expose count/sum via WithLabelValues(...).(Metric);
	// easier to assert via registry output.
	if got := testutil.CollectAndCount(m.pollDuration, "outbox_poll_duration_seconds"); got != 1 {
		t.Errorf("poll duration child metrics: got %d want 1", got)
	}
	if got := testutil.ToFloat64(m.pollRows.WithLabelValues("order_outbox")); got != 5 {
		t.Errorf("poll rows: got %v want 5", got)
	}
}

func TestPrometheusMetrics_ObservePollError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics("order_outbox", reg)
	m.ObservePoll(context.Background(), 0, 5*time.Millisecond, errors.New("boom"))

	if got := testutil.CollectAndCount(m.pollDuration, "outbox_poll_duration_seconds"); got != 1 {
		t.Errorf("error poll child metrics: got %d want 1", got)
	}
	// Verify the "error" label was used by gathering the registry.
	got, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, mf := range got {
		if mf.GetName() != "outbox_poll_duration_seconds" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			for _, l := range mm.GetLabel() {
				if l.GetName() == "outcome" && l.GetValue() == "error" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Errorf("error label not found in poll duration metrics")
	}
}

func TestPrometheusMetrics_ObservePublish(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics("payment_outbox", reg)
	m.ObservePublish(context.Background(), 10, nil)
	m.ObservePublish(context.Background(), 3, errors.New("kafka"))

	if got := testutil.ToFloat64(m.publish.WithLabelValues("payment_outbox", "ok")); got != 10 {
		t.Errorf("publish ok: got %v want 10", got)
	}
	if got := testutil.ToFloat64(m.publish.WithLabelValues("payment_outbox", "error")); got != 3 {
		t.Errorf("publish err: got %v want 3", got)
	}
}

func TestPrometheusMetrics_ObserveDLQ(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics("inventory_outbox", reg)
	m.ObserveDLQ(context.Background(), outbox.Record{EventID: "e1"}, "kafka down")
	m.ObserveDLQ(context.Background(), outbox.Record{EventID: "e2"}, "kafka down")

	if got := testutil.ToFloat64(m.dlq.WithLabelValues("inventory_outbox")); got != 2 {
		t.Errorf("dlq: got %v want 2", got)
	}
}

func TestPrometheusMetrics_GatherContainsAllNames(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics("t", reg)
	// Observe once so Prometheus exposes all metric families
	// (Gather returns empty for never-incremented counters).
	m.ObservePoll(context.Background(), 1, time.Millisecond, nil)
	m.ObservePublish(context.Background(), 1, nil)
	m.ObserveDLQ(context.Background(), outbox.Record{}, "x")
	got, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, mf := range got {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"outbox_poll_duration_seconds",
		"outbox_poll_rows_total",
		"outbox_publish_total",
		"outbox_dlq_total",
	} {
		if !names[want] {
			t.Errorf("metric %q missing; have %v", want, strings.Join(metricNames(names), ","))
		}
	}
}

func metricNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
