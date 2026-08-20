package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestReadyHandler_NoChecksReturns200 verifies the disabled-mode
// contract: when the service is running without dependencies
// (DATABASE_URL unset, KAFKA_BROKERS unset), /readyz must still
// return 200 so Kubernetes does not pull the pod out of rotation.
func TestReadyHandler_NoChecksReturns200(t *testing.T) {
	h := ReadyHandler(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	if body := w.Body.String(); body != `{"status":"ok"}` {
		t.Errorf("body: got %q want %q", body, `{"status":"ok"}`)
	}
}

// TestReadyHandler_AllChecksPassReturns200 is the happy-path OBS-1
// regression net: when every dependency probe returns nil, /readyz
// must return 200 with `{"status":"ok"}`. Pre-fix this endpoint did
// not exist on the order/payment/inventory/saga binaries.
func TestReadyHandler_AllChecksPassReturns200(t *testing.T) {
	checks := []Check{
		func(_ context.Context) error { return nil },
		func(_ context.Context) error { return nil },
	}
	h := ReadyHandler([]string{"postgres", "kafka"}, checks)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200; body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status: got %v want ok", body["status"])
	}
	if _, has := body["failed"]; has {
		t.Errorf("expected no failed key on success; got %v", body)
	}
}

// TestReadyHandler_CheckFailsReturns503 is the failure-path OBS-1
// regression net. When at least one dependency probe returns a
// non-nil error, /readyz must return 503 with the failing names
// listed in a `failed` array. This is what makes the endpoint
// useful to kubelet: a real DB outage flips /readyz to 503 and the
// pod is pulled out of the Service's endpoint list.
func TestReadyHandler_CheckFailsReturns503(t *testing.T) {
	checks := []Check{
		func(_ context.Context) error { return errors.New("connection refused") },
		func(_ context.Context) error { return nil },
	}
	h := ReadyHandler([]string{"postgres", "kafka"}, checks)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503; body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "down" {
		t.Errorf("status: got %v want down", body["status"])
	}
	failed, ok := body["failed"].([]any)
	if !ok || len(failed) != 1 {
		t.Fatalf("failed: got %v want [postgres]", body["failed"])
	}
	if failed[0] != "postgres" {
		t.Errorf("failed[0]: got %v want postgres", failed[0])
	}
}

// TestReadyHandler_AllChecksFailListsAll verifies that a total
// dependency outage reports every check as failed (not just the
// first). Operators triage by name; a partial report hides the
// scope of an outage.
func TestReadyHandler_AllChecksFailListsAll(t *testing.T) {
	checks := []Check{
		func(_ context.Context) error { return errors.New("db down") },
		func(_ context.Context) error { return errors.New("kafka down") },
	}
	h := ReadyHandler([]string{"postgres", "kafka"}, checks)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503", w.Code)
	}
	var body struct {
		Status string   `json:"status"`
		Failed []string `json:"failed"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Status != "down" {
		t.Errorf("status: got %q want down", body.Status)
	}
	if len(body.Failed) != 2 {
		t.Errorf("failed length: got %d want 2 (%v)", len(body.Failed), body.Failed)
	}
}

// TestReadyHandler_RespectsTimeout pins the 2-second cap: a Check
// that blocks longer than the cap is abandoned, the endpoint
// returns 503, and the goroutine does not leak past the request's
// lifetime. Without the cap, a wedged DB driver would hold the
// kubelet's HTTP request open past its 5-second probe timeout.
func TestReadyHandler_RespectsTimeout(t *testing.T) {
	// A check that blocks until the test cancels its context.
	checks := []Check{
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	h := ReadyHandler([]string{"slow"}, checks)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	start := time.Now()
	h(w, req)
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Errorf("readyz did not honor 2s cap: took %s", elapsed)
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d want 503", w.Code)
	}
}