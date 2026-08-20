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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	t.Logf("POST /v1/orders → id=%s (kafka broker=%s, expected topic=order-events)",
		created.ID, h.KafkaBrokers[0])

	observed := waitForState(ctx, t, client, orderBase, created.ID, "confirmed")

	if len(observed) == 0 {
		t.Fatalf("no GET /v1/orders/%s responses received within %s; expected chain "+
			"Order→outbox→kafka(order-events)→saga→inventory→payment→order",
			created.ID, pollingBudget)
	}
	if observed[len(observed)-1].State != "confirmed" {
		dumpServiceLogs(t, 80)
		t.Fatalf("order %s did not reach confirmed; final state=%q, observed=%s; "+
			"check tests/logs/{order,saga,inventory,payment}.log on the CI runner for chain stall",
			created.ID,
			observed[len(observed)-1].State,
			formatStates(observed))
	}
}

// TestE2E_HappyPath_PaymentLastFour drives the full orderflow chain
// using a submit body that includes a payment.last_four hint, and
// asserts the wire-shape exposes last_four back to the GET response.
//
// Regression net for audit TEST-2 (P1): the pre-existing
// TestE2E_OrderReachesConfirmed reads examples/order.json, which has
// no `payment` block, so the order service falls back to the
// pre-v1.1.5 empty-LastFour path. A regression that drops the
// LastFour plumbing in submitRequest→OrderCreated→saga→PaymentRequested
// would still pass that test, because the empty-LastFour fallback
// masks the missing wire field. This test removes the fallback by
// sending last_four=0000 (a mock-provider success suffix; see
// services/payment/internal/provider/provider.go), so a regression
// that drops last_four end-to-end makes the payment provider pick
// the default-case success path off the empty string — masking the
// regression there too. Two wire-shape checks pin the regression:
//
//  1. The submit body MUST include `payment.last_four`. Without it
//     the order service would 400 or silently zero out last_four
//     depending on which side of the plumbing is broken.
//  2. The GET /v1/orders/{id} response MUST include `last_four`
//     because the order service persists it on the orders row
//     (services/order/migrations/0007_orders_last_four.sql) and the
//     GET handler returns it (services/order/internal/repository/pg_repo.go:103).
//
// Both checks would fail under any of the v1.1.5-plumbing regression
// modes (e.g., dropping the field on submitRequest, dropping it on
// OrderCreatedPayload, dropping it on the saga INSERT, or dropping it
// from the orders column SELECT).
//
// The final-state assertion is intentionally identical to the
// pre-existing happy-path test (state == "confirmed"). It is NOT a
// regression net for the last_four plumbing — it is a sanity check
// that the chain still completes when last_four is plumbed end to
// end. A failure on this final state without a failure on the
// last_four wire checks is the v1.1.5 chain-stall regression class,
// not the plumbing-regression class.
func TestE2E_HappyPath_PaymentLastFour(t *testing.T) {
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

	waitForOrderReady(ctx, t, client, orderBase)
	waitForServiceUp(ctx, t, client, paymentBase)
	waitForServiceUp(ctx, t, client, invBase)
	waitForServiceUp(ctx, t, client, sagaBase)

	// last_four=0000 forces the mock provider's success branch
	// (default case in services/payment/internal/provider/provider.go).
	// Any other 4-digit suffix would also be "success" by default
	// but using 0000 keeps the intent explicit and avoids the
	// orderID-derived coincidental success that the v1.x fallback
	// path relied on.
	body := []byte(`{
        "customer_id": "8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f",
        "items": [{"sku": "SKU-001", "quantity": 1, "unit_price_cents": 1999}],
        "payment": {"last_four": "0000"}
    }`)
	created := postOrder(ctx, t, client, orderBase, body)
	t.Logf("POST /v1/orders → id=%s (kafka broker=%s, last_four=0000 forces success)",
		created.ID, h.KafkaBrokers[0])

	// Final-state assertion: identical shape to the pre-existing
	// happy-path test. NOT the regression net for this test (see
	// file doc) — the regression nets are the wire-shape checks
	// below.
	observed := waitForState(ctx, t, client, orderBase, created.ID, "confirmed")
	if len(observed) == 0 {
		t.Fatalf("no GET /v1/orders/%s responses received within %s; expected chain "+
			"Order→outbox→kafka(order-events)→saga→inventory→payment→order",
			created.ID, pollingBudget)
	}
	if observed[len(observed)-1].State != "confirmed" {
		dumpServiceLogs(t, 80)
		t.Fatalf("order %s did not reach confirmed; final state=%q, observed=%s; "+
			"check tests/logs/{order,saga,inventory,payment}.log on the CI runner for chain stall",
			created.ID,
			observed[len(observed)-1].State,
			formatStates(observed))
	}

	// Wire-shape regression net: GET /v1/orders/{id} MUST include
	// `last_four` so the playground's summary card can rebind it
	// (web/internal/handlers/pages.go:PageOrderDetail). Without
	// this assertion a regression that drops last_four from the
	// orders column SELECT, or from the JSON tag, or from the
	// migration, would pass the chain-complete check above.
	gotLastFour := fetchLastFourFromOrder(ctx, t, client, orderBase, created.ID)
	if gotLastFour != "0000" {
		t.Fatalf("GET /v1/orders/%s: last_four wire-shape regression; got %q, want %q "+
			"(v1.1.5 plumbing regression — submitRequest→OrderCreated→orders.last_four→GET response)",
			created.ID, gotLastFour, "0000")
	}
}

// fetchLastFourFromOrder issues GET /v1/orders/{id} once and returns
// the `last_four` field from the JSON body. The order service's GET
// handler returns the persisted orders row, so this verifies the
// full submit→INSERT→SELECT round-trip on the last_four column.
// Uses ctx-aware HTTP via httpDo (helpers_test.go) so the request
// cannot outlive the parent budget.
func fetchLastFourFromOrder(ctx context.Context, t *testing.T, client *http.Client, baseURL, orderID string) string {
	t.Helper()
	stage, cancel, _ := stageContext(ctx, perRequestBudget)
	defer cancel()
	url := baseURL + "/v1/orders/" + orderID
	req, err := http.NewRequestWithContext(stage, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("fetchLastFourFromOrder(%s): build request: %v", url, err)
	}
	status, body, err := httpDo(stage, client, req)
	if err != nil {
		t.Fatalf("fetchLastFourFromOrder(%s): %v", url, err)
	}
	if status != http.StatusOK {
		t.Fatalf("fetchLastFourFromOrder(%s): status=%d body=%s", url, status, body)
	}
	// Parse only the field we need; unknown fields are ignored.
	// Matches the same minimal-decode strategy used by
	// orderStateResponse in helpers_test.go so future schema
	// additions don't churn this test.
	var resp struct {
		LastFour string `json:"last_four"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&resp); err != nil {
		t.Fatalf("fetchLastFourFromOrder(%s): decode body: %v body=%s", url, err, body)
	}
	// Drain any unread body so the connection can be reused for
	// keep-alive (httpDo already drains — this is a belt-and-suspenders
	// no-op for the body bytes the caller passed).
	_, _ = io.Copy(io.Discard, bytes.NewReader(body))
	return resp.LastFour
}
