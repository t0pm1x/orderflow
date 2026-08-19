package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/tests/harness"
)

func TestE2E_HappyPath_OrderConfirmed(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E requires docker")
	}
	h := harness.New(t)

	stopOrder := h.StartService(t, "order", "order", map[string]string{
		"DATABASE_URL": h.PostgresURLs["order"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    "127.0.0.1:18081",
	})
	defer stopOrder()
	stopPayment := h.StartService(t, "payment", "payment", map[string]string{
		"DATABASE_URL": h.PostgresURLs["payment"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    "127.0.0.1:18082",
	})
	defer stopPayment()
	stopInv := h.StartService(t, "inventory", "inventory", map[string]string{
		"DATABASE_URL": h.PostgresURLs["inventory"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"REDIS_URL":    h.RedisURL,
		"HTTP_ADDR":    "127.0.0.1:18083",
	})
	defer stopInv()
	stopSaga := h.StartService(t, "saga", "saga", map[string]string{
		"DATABASE_URL": h.PostgresURLs["order"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    "127.0.0.1:18084",
	})
	defer stopSaga()

	waitForHealth(t, "http://127.0.0.1:18081/healthz", 30*time.Second)
	waitForHealth(t, "http://127.0.0.1:18082/healthz", 30*time.Second)
	waitForHealth(t, "http://127.0.0.1:18083/healthz", 30*time.Second)
	waitForHealth(t, "http://127.0.0.1:18084/healthz", 30*time.Second)

	body, err := os.ReadFile("../../examples/order.json")
	if err != nil {
		t.Fatalf("read examples/order.json: %v", err)
	}

	resp, err := http.Post("http://127.0.0.1:18081/v1/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/orders: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /v1/orders: status=%d body=%s", resp.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.ID == "" {
		t.Fatal("empty order id")
	}

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		getResp, err := http.Get("http://127.0.0.1:18081/v1/orders/" + created.ID)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var got struct {
			State string `json:"state"`
		}
		_ = json.NewDecoder(getResp.Body).Decode(&got)
		_ = getResp.Body.Close()
		if got.State == "confirmed" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("order %s did not reach confirmed within %s", created.ID, 120*time.Second)
}

func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("service at %s did not become healthy within %s", url, timeout)
}
