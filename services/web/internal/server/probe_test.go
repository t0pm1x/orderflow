package server_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/services/web/internal/server"
)

// fakeUpstream stands up a tiny /healthz server whose status code,
// body, and latency are all configurable.
type fakeUpstream struct {
	*httptest.Server
	calls atomic.Int32
}

func newFakeUpstream(status int, body string, delay time.Duration) *fakeUpstream {
	f := &fakeUpstream{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.calls.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	return f
}

func decodeSnapshot(t *testing.T, body []byte) server.HealthSnapshot {
	t.Helper()
	var snap server.HealthSnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		t.Fatalf("decode snapshot: %v (body=%s)", err, body)
	}
	return snap
}

func TestHealthAll_AllOK(t *testing.T) {
	order := newFakeUpstream(200, `{"status":"ok"}`, 0)
	payment := newFakeUpstream(200, ``, 0)
	inventory := newFakeUpstream(200, `{"status":"ok"}`, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return true },
	})
	req := httptest.NewRequest(http.MethodGet, "/api/health/all", nil)
	rec := httptest.NewRecorder()
	srv.HealthAll(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	snap := decodeSnapshot(t, rec.Body.Bytes())
	for name, h := range map[string]server.ServiceHealth{
		"order": snap.Order, "payment": snap.Payment,
		"inventory": snap.Inventory, "saga": snap.Saga,
	} {
		if h.Status != "ok" {
			t.Errorf("%s: status=%q want ok (detail=%q)", name, h.Status, h.Detail)
		}
		if h.LatencyMS < 0 {
			t.Errorf("%s: negative latency %d", name, h.LatencyMS)
		}
		if h.TakenAt == "" {
			t.Errorf("%s: empty taken_at", name)
		}
	}
	if snap.Kafka.Status != "ok" {
		t.Errorf("kafka: status=%q want ok", snap.Kafka.Status)
	}
	if snap.SnapshotAt == "" {
		t.Errorf("snapshot_at empty")
	}
}

func TestHealthAll_OneDown(t *testing.T) {
	order := newFakeUpstream(200, ``, 0)
	payment := newFakeUpstream(500, `internal error`, 0) // <-- down
	inventory := newFakeUpstream(200, ``, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return true },
	})
	rec := httptest.NewRecorder()
	srv.HealthAll(rec, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))

	snap := decodeSnapshot(t, rec.Body.Bytes())
	if snap.Order.Status != "ok" {
		t.Errorf("order=%q", snap.Order.Status)
	}
	if snap.Payment.Status != "down" {
		t.Errorf("payment=%q", snap.Payment.Status)
	}
	if snap.Payment.Detail == "" {
		t.Errorf("payment detail empty")
	}
	if snap.Inventory.Status != "ok" {
		t.Errorf("inventory=%q", snap.Inventory.Status)
	}
	if snap.Saga.Status != "ok" {
		t.Errorf("saga=%q", snap.Saga.Status)
	}
}

func TestHealthAll_Degraded(t *testing.T) {
	order := newFakeUpstream(200, ``, 1500*time.Millisecond) // >1s
	payment := newFakeUpstream(200, ``, 0)
	inventory := newFakeUpstream(200, `{"status":"degraded"}`, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return true },
	})
	rec := httptest.NewRecorder()
	srv.HealthAll(rec, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))

	snap := decodeSnapshot(t, rec.Body.Bytes())
	if snap.Order.Status != "degraded" {
		t.Errorf("order=%q want degraded (latency=%d)", snap.Order.Status, snap.Order.LatencyMS)
	}
	if snap.Order.LatencyMS < 1000 {
		t.Errorf("order latency=%d want >=1000", snap.Order.LatencyMS)
	}
	if snap.Inventory.Status != "degraded" {
		t.Errorf("inventory=%q want degraded", snap.Inventory.Status)
	}
	if snap.Payment.Status != "ok" {
		t.Errorf("payment=%q want ok", snap.Payment.Status)
	}
}

func TestHealthAll_Timeout(t *testing.T) {
	order := newFakeUpstream(200, ``, 3*time.Second) // >2s timeout
	payment := newFakeUpstream(200, ``, 0)
	inventory := newFakeUpstream(200, ``, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return true },
	})
	start := time.Now()
	rec := httptest.NewRecorder()
	srv.HealthAll(rec, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))
	if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
		t.Errorf("HealthAll took %v, want <2.5s (probe must enforce 2s timeout)", elapsed)
	}
	snap := decodeSnapshot(t, rec.Body.Bytes())
	if snap.Order.Status != "down" {
		t.Errorf("order=%q want down", snap.Order.Status)
	}
	if snap.Order.Detail == "" {
		t.Errorf("order detail empty, want timeout reason")
	}
	if snap.Payment.Status != "ok" {
		t.Errorf("payment=%q want ok", snap.Payment.Status)
	}
}

func TestHealthAll_KafkaDown(t *testing.T) {
	order := newFakeUpstream(200, ``, 0)
	payment := newFakeUpstream(200, ``, 0)
	inventory := newFakeUpstream(200, ``, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return false }, // <-- kafka tail down
	})
	rec := httptest.NewRecorder()
	srv.HealthAll(rec, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))

	snap := decodeSnapshot(t, rec.Body.Bytes())
	if snap.Kafka.Status != "down" {
		t.Errorf("kafka=%q want down", snap.Kafka.Status)
	}
	if snap.Kafka.Detail == "" {
		t.Errorf("kafka detail empty")
	}
	if snap.Order.Status != "ok" {
		t.Errorf("order=%q want ok", snap.Order.Status)
	}
}

// TestHealthAll_CacheHits exercises the 1-second snapshot cache:
// two back-to-back calls must only hit each upstream once. Without
// the cache, the second call would re-probe every upstream and
// the call counter would land at 2 — both for the happy path
// (200/OK) and on the slow path where the first call had time to
// populate the cache before the second one arrived.
func TestHealthAll_CacheHits(t *testing.T) {
	order := newFakeUpstream(200, ``, 0)
	payment := newFakeUpstream(200, ``, 0)
	inventory := newFakeUpstream(200, ``, 0)
	saga := newFakeUpstream(200, ``, 0)
	defer order.Close()
	defer payment.Close()
	defer inventory.Close()
	defer saga.Close()

	srv := server.New(server.Options{
		Name:   "test",
		Logger: slog.Default(),
		Urls: server.ServiceURLs{
			Order: order.URL, Payment: payment.URL,
			Inventory: inventory.URL, Saga: saga.URL,
		},
		KafkaHealth: func() bool { return true },
	})

	// First call populates the cache.
	rec1 := httptest.NewRecorder()
	srv.HealthAll(rec1, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))
	if rec1.Code != 200 {
		t.Fatalf("first call status=%d body=%s", rec1.Code, rec1.Body.String())
	}
	if got := order.calls.Load(); got != 1 {
		t.Fatalf("after first call: order.calls=%d want 1", got)
	}

	// Second call, well within the 1s cache window, must NOT
	// re-probe any upstream.
	rec2 := httptest.NewRecorder()
	srv.HealthAll(rec2, httptest.NewRequest(http.MethodGet, "/api/health/all", nil))
	if rec2.Code != 200 {
		t.Fatalf("second call status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if got := order.calls.Load(); got != 1 {
		t.Errorf("order.calls=%d after second call, want 1 (second call must hit cache)", got)
	}

	// Sanity-check the cached payload is byte-identical to the
	// fresh one: same status, same per-upstream state.
	snap1 := decodeSnapshot(t, rec1.Body.Bytes())
	snap2 := decodeSnapshot(t, rec2.Body.Bytes())
	if snap2.SnapshotAt != snap1.SnapshotAt {
		t.Errorf("cached snapshot has fresh SnapshotAt: %s vs %s", snap2.SnapshotAt, snap1.SnapshotAt)
	}
	for name, h2 := range map[string]server.ServiceHealth{
		"order": snap2.Order, "payment": snap2.Payment,
		"inventory": snap2.Inventory, "saga": snap2.Saga, "kafka": snap2.Kafka,
	} {
		var h1 server.ServiceHealth
		switch name {
		case "order":
			h1 = snap1.Order
		case "payment":
			h1 = snap1.Payment
		case "inventory":
			h1 = snap1.Inventory
		case "saga":
			h1 = snap1.Saga
		case "kafka":
			h1 = snap1.Kafka
		}
		if h1.Status != h2.Status {
			t.Errorf("%s: cached status=%q want %q", name, h2.Status, h1.Status)
		}
	}
}
