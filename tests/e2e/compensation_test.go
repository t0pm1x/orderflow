package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/t0pm1x/orderflow/tests/harness"
)

// TestE2E_Compensation_PaymentDeclined_CancelsOrder exercises the
// failure path of the orderflow chain:
//
//	orderflow  POST /v1/orders        (last_four=0001 → mock declines) → 201
//	    └─→  outbox OrderCreated      → kafka:order-events
//	          └─→ saga OrderCreated   → outbox StockReserveRequested
//	              └─→ inventory StockReserveRequested → Reserved + outbox StockReserved
//	                  └─→ saga StockReserved → outbox PaymentRequested
//	                      └─→ payment PaymentRequested → mock declines → outbox PaymentFailed
//	                          └─→ saga PaymentFailed → outbox StockReleaseRequested + OrderCancelled
//	                              └─→ inventory StockReleaseRequested → released + outbox StockReleased
//	                                  └─→ order StockReleased + OrderCancelled → order.state="cancelled"
//
// The expected final state is "cancelled" (or "failed"); the
// helper waitForStateFn's isDone fires on those, isFailure
// fires on "confirmed" (a chain-regression signature on the
// failure path).
//
// Shares infrastructure with order_confirmed_test.go via
// helpers_test.go.
func TestE2E_Compensation_PaymentDeclined_CancelsOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E requires docker")
	}

	ctx, cancel := context.WithTimeout(t.Context(), overallBudget)
	defer cancel()

	h := harness.New(t)

	orderPort := pickFreePort(t)
	paymentPort := pickFreePort(t)
	invPort := pickFreePort(t)
	sagaPort := pickFreePort(t)
	orderBase := fmt.Sprintf("http://127.0.0.1:%d", orderPort)
	paymentBase := fmt.Sprintf("http://127.0.0.1:%d", paymentPort)
	invBase := fmt.Sprintf("http://127.0.0.1:%d", invPort)
	sagaBase := fmt.Sprintf("http://127.0.0.1:%d", sagaPort)

	stopOrder := h.StartService(t, "order", "order", map[string]string{
		"DATABASE_URL": h.PostgresURLs["order"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    fmt.Sprintf("127.0.0.1:%d", orderPort),
	})
	defer stopOrder()
	stopPayment := h.StartService(t, "payment", "payment", map[string]string{
		"DATABASE_URL": h.PostgresURLs["payment"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    fmt.Sprintf("127.0.0.1:%d", paymentPort),
	})
	defer stopPayment()
	stopInventory := h.StartService(t, "inventory", "inventory", map[string]string{
		"DATABASE_URL": h.PostgresURLs["inventory"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"REDIS_URL":    h.RedisURL,
		"HTTP_ADDR":    fmt.Sprintf("127.0.0.1:%d", invPort),
	})
	defer stopInventory()
	stopSaga := h.StartService(t, "saga", "saga", map[string]string{
		"DATABASE_URL": h.PostgresURLs["order"],
		"KAFKA_BROKER": h.KafkaBrokers[0],
		"HTTP_ADDR":    fmt.Sprintf("127.0.0.1:%d", sagaPort),
	})
	defer stopSaga()

	client := httpClient()

	// See helpers_test.go on the readiness-vs-liveness split.
	waitForOrderReady(ctx, t, client, orderBase)
	waitForServiceUp(ctx, t, client, paymentBase)
	waitForServiceUp(ctx, t, client, invBase)
	waitForServiceUp(ctx, t, client, sagaBase)

	// last_four=0001 forces the mock provider to decline (see
	// services/payment/internal/provider/provider.go).
	body := []byte(`{
        "customer_id": "8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f",
        "items": [{"sku": "SKU-001", "quantity": 1, "unit_price_cents": 1999}],
        "payment": {"last_four": "0001"}
    }`)
	created := retryPost(ctx, t, client, orderBase, body)
	t.Logf("POST /v1/orders → id=%s", created.ID)

	// The failure path is terminal: cancelled or failed. A
	// 'confirmed' arrival on this path means the saga consumed
	// PaymentCompleted when it should have seen PaymentFailed —
	// chain regression.
	observed := waitForStateFn(ctx, t, client, orderBase, created.ID,
		func(s string) bool { return s == "cancelled" || s == "failed" },
		func(s string) bool { return s == "confirmed" },
	)

	if len(observed) == 0 {
		t.Fatalf("no GET /v1/orders/%s responses received within budget", created.ID)
	}
	last := observed[len(observed)-1].State
	if last != "cancelled" && last != "failed" {
		t.Fatalf("order %s expected cancelled/failed, got %q; observed=%s",
			created.ID, last, formatStates(observed))
	}
}
