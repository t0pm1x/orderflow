package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/tests/harness"
)

func TestE2E_Compensation_PaymentDeclined_CancelsOrder(t *testing.T) {
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

	body := []byte(`{
        "customer_id": "8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f",
        "items": [{"sku": "SKU-001", "quantity": 1, "unit_price_cents": 1999}],
        "payment": {"last_four": "0001"}
    }`)
	resp, err := http.Post("http://127.0.0.1:18081/v1/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/orders: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /v1/orders: status=%d body=%s", resp.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	deadline := time.Now().Add(60 * time.Second)
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
		if got.State == "cancelled" || got.State == "failed" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("order %s did not reach cancelled/failed within 60s (payment was declined)", created.ID)
}
