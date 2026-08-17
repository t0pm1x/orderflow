// Chaos test for stage 3.11.d: kill the Kafka broker mid-flight and
// verify the orderflow services tolerate the outage without spurious
// state progression.
//
// Scope of the assertion:
//
//   - The order service remains healthy and responsive after Kafka
//     dies (no cascade failure, HTTP /healthz still 200).
//   - The order does NOT progress to "confirmed" while Kafka is
//     unreachable (the saga cannot fire OrderConfirmed without its
//     dependency on the broker).
//
// A full recovery assertion (outbox retry, fresh broker, eventual
// confirmation) requires restarting Kafka at an address stable enough
// that the already-running service binaries can dial it; that lives
// in a follow-up stage once that mechanic is in the harness.
package chaos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/tests/harness"
)

func TestChaos_KafkaKill_OrderServiceSurvives(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos requires docker")
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

	body := osReadFile(t, "../../examples/order.json")
	resp, err := http.Post(
		"http://127.0.0.1:18081/v1/orders",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /v1/orders: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/orders: status=%d", resp.StatusCode)
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
	t.Logf("created order %s", created.ID)

	time.Sleep(1 * time.Second)
	preState := getOrderState(t, created.ID)
	t.Logf(">>> pre-kill order state: %s", preState)

	t.Log(">>> killing Kafka container")
	if err := h.KafkaContainer().Terminate(context.Background()); err != nil {
		t.Logf("terminate kafka: %v", err)
	}

	time.Sleep(5 * time.Second)

	// Assertion 1: the order service stays healthy after Kafka dies.
	// Without this, an outage would cascade through every service.
	waitForHealth(t, "http://127.0.0.1:18081/healthz", 5*time.Second)
	t.Log(">>> order service healthy post-kill")

	// Assertion 2: order must not have progressed to "confirmed".
	// The saga needs Kafka to publish OrderConfirmed; with the broker
	// down the order must stay at or before its pre-kill state.
	postState := getOrderState(t, created.ID)
	t.Logf(">>> post-kill order state: %s", postState)
	if postState == "confirmed" {
		t.Fatalf("order %s reached confirmed after Kafka kill; saga must not make progress without the broker", created.ID)
	}
}

func getOrderState(t *testing.T, id string) string {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1:18081/v1/orders/" + id)
	if err != nil {
		t.Fatalf("GET /v1/orders/%s: %v", id, err)
	}
	defer resp.Body.Close()
	var got struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode order %s: %v", id, err)
	}
	return got.State
}
