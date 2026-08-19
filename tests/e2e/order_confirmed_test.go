// Package e2e_test — the happy-path E2E test for the orderflow
// chain. See helpers_test.go for the shared infrastructure (ctx
// discipline, polling helpers, harness access).
//
// The chain under test:
//
//	orderflow  POST /v1/orders        → 201 + order_id
//	    └─→  outbox OrderCreated      → kafka:order-events
//	          └─→ saga OrderCreated   → StateInitiated + outbox StockReserveRequested
//	              └─→ inventory StockReserveRequested → Reserved + outbox StockReserved
//	                  └─→ saga StockReserved → StateStockReserved + outbox PaymentRequested
//	                      └─→ payment PaymentRequested → mock provider.Charge
//	                          └─→ PaymentCompleted → outbox PaymentCompleted
//	                              └─→ saga PaymentCompleted → StateCompleted + outbox OrderConfirmed
//	                                  └─→ order OrderConfirmed → state="confirmed"
package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/t0pm1x/orderflow/tests/harness"
)

// TestE2E_OrderReachesConfirmed drives the full orderflow chain:
//
//	POST /v1/orders → outbox → kafka → saga → inventory → saga
//	  → payment → saga → order → confirmed
//
// The test polls the Order state with ctx-aware HTTP and aborts
// the moment the deadlined ctx cancels — no goroutine, sleep, or
// HTTP request can outlive overallBudget (helpers_test.go).
func TestE2E_OrderReachesConfirmed(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E requires docker")
	}

	ctx, cancel := context.WithTimeout(t.Context(), overallBudget)
	defer cancel()

	h := harness.New(t)

	// Pick free TCP ports on loopback so the test is not at the
	// mercy of a busy CI runner's port space. Bind+close is a
	// best-effort dance; services bind fast enough that the race
	// is a non-issue in serial test execution.
	orderPort := pickFreePort(t)
	paymentPort := pickFreePort(t)
	invPort := pickFreePort(t)
	sagaPort := pickFreePort(t)
	orderBase := fmt.Sprintf("http://127.0.0.1:%d", orderPort)
	paymentBase := fmt.Sprintf("http://127.0.0.1:%d", paymentPort)
	invBase := fmt.Sprintf("http://127.0.0.1:%d", invPort)
	sagaBase := fmt.Sprintf("http://127.0.0.1:%d", sagaPort)

	// Start the 4 services. Each StartService returns a stop
	// callback; defer them so order of cleanup matches start
	// (LIFO). The harness's t.Cleanup tears down containers last.
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

	// order's REST handler is mounted only after its DB pool is
	// wired; that's the real readiness signal. payment /
	// inventory / saga expose only /healthz as a probe — there is
	// no REST readiness endpoint for them in their command trees
	// (see helpers_test.go for the rationale). We therefore
	// "wait for the binary to be listening" on those three and
	// rely on the chain's state-machine outcome to surface any
	// deeper readiness bug.
	waitForOrderReady(ctx, t, client, orderBase)
	waitForServiceUp(ctx, t, client, paymentBase)
	waitForServiceUp(ctx, t, client, invBase)
	waitForServiceUp(ctx, t, client, sagaBase)

	body := readRepoFile(t, "examples", "order.json")
	created := postOrder(ctx, t, client, orderBase, body)
	t.Logf("POST /v1/orders → id=%s", created.ID)

	observed := waitForState(ctx, t, client, orderBase, created.ID, "confirmed")

	if len(observed) == 0 {
		t.Fatalf("no GET /v1/orders/%s responses received within budget", created.ID)
	}
	if observed[len(observed)-1].State != "confirmed" {
		t.Fatalf("order %s did not reach confirmed; final state=%q, observed=%s",
			created.ID,
			observed[len(observed)-1].State,
			formatStates(observed))
	}
}
