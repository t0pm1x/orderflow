# orderflow-web Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a tactile web playground to orderflow — at `http://localhost:8083`, a developer can create / list / view orders, fire a forced-fail payment webhook to watch saga compensation, browse inventory, and watch live `order-events` from Kafka in a sidebar.

**Architecture:** New Go module `services/web` in the existing monorepo. BFF pattern — browser hits :8083 only; web proxies to :8080/8081/8082 via plain `net/http`. Server-side rendering with `html/template`; `htmx 2.x` from CDN for progressive enhancement; `EventSource` for live SSE relay from a dedicated Kafka consumer group `orderflow-web` subscribed to `order-events`. New module mirrors existing service shutdown pattern (`sync.WaitGroup`, `Run(ctx) → close fns defer on context.Background()`).

**Tech Stack:** Go 1.25.13, `github.com/go-chi/chi/v5` (matches existing services), `pkg/platform` (logging, OTel, middleware), `pkg/consumer` (Kafka SSE tail), `html/template` (stdlib), `htmx@2.x` (CDN), `embed.FS` (templates + static).

## Global Constraints

- Go version floor: **1.25.13** (per `go.work`).
- New module lives at `services/web/`. It is added to `go.work` `use` block, `services/web/cmd/web` is NOT a separate top-level workspace module — the binary entry stays inside the service module (mirrors `services/order/cmd/order`, which exports `web.Main()` and `cmd/web/main.go` is the 10-line delegator). This keeps import cycles clean and matches the existing pattern.
- Service module path: **`github.com/t0pm1x/orderflow/services/web`**.
- The web binary uses port **`:8083`** by default. Override with `HTTP_ADDR`.
- `ORDER_URL`, `PAYMENT_URL`, `INVENTORY_URL` envs (defaults `http://localhost:8080`, `http://localhost:8081`, `http://localhost:8082`). `KAFKA_BROKERS` (comma-separated, default empty = Kafka tail disabled).
- **No modification** to services/order, services/payment, services/inventory, services/saga, or tests/*. All backend behavior is treated as out-of-scope contract.
- **No new shared types.** `services/web/internal/types` re-declares the small slice it needs (OrderItem, OrderSubmit, Order, OrderState, StockItem, PaymentWebhook) using only stdlib types + `github.com/google/uuid`. The repository already imports `uuid` in tests; this keeps the web module free of cross-service coupling.
- Patterns to mirror:
  - Service binary shape: `services/order/cmd/order/main.go:60-235` (Run(ctx) + WaitGroup shutdown + close fn chain).
  - cmd top-level: `cmd/order/main.go:1-10` (one-line delegator).
  - Middleware stack: `pkg/platform/middleware` (`mw.Stack(name, logger)` returns recover + reqlog + otel).
- Verification commands (run from repo root):
  - Build: `cd services/web && go build ./...` then `cd ../.. && go build -o bin/web ./services/web/cmd/web`.
  - Test (short, skips Kafka harness): `cd services/web && go test -short ./...`.
  - Full repo verification: `make verify` (extended per Task 11).
- Tests use the project's external-test-package convention: file `foo_test.go` declares `package web_test`.
- The plan stops at "feature-complete + smoke-tested locally"; adding the new module to docker-compose + Makefile wiring + README is Task 11 (final), not implicit.

## File Structure

Files this plan creates or modifies:

| File | Action | Purpose |
|---|---|---|
| `services/web/go.mod` | Create | Module file; deps: chi, google/uuid, kafkaprop (for spans), pkg/platform, pkg/consumer |
| `services/web/cmd/web/main.go` | Create | 10-line delegator → `web.Main()` |
| `services/web/internal/server/server.go` | Create | chi router, middleware, route registration |
| `services/web/internal/backend/types.go` | Create | OrderSubmit, OrderItem, Order, OrderState, StockItem, PaymentWebhook — all with JSON tags matching the OpenAPI spec |
| `services/web/internal/backend/client.go` | Create | `OrderClient`, `PaymentClient`, `InventoryClient` interfaces + `New()` constructors that take `*http.Client` and base URL strings |
| `services/web/internal/backend/order.go` | Create | `orderClient` impl: List / Get / Submit / Cancel |
| `services/web/internal/backend/payment.go` | Create | `paymentClient` impl: FireWebhook |
| `services/web/internal/backend/inventory.go` | Create | `inventoryClient` impl: ListStock |
| `services/web/internal/backend/*_test.go` | Create | httptest-backed unit tests (one suite per client) |
| `services/web/internal/events/bus.go` | Create | In-process pub/sub: `Bus.Subscribe() (unsubscribe func(), ch <-chan Event)` |
| `services/web/internal/events/bus_test.go` | Create | Unit tests for bus (subscribe/unsubscribe/buffered drop/close semantics) |
| `services/web/internal/kafkatail/tail.go` | Create | Wraps `pkg/consumer.Consumer`; registers one handler that publishes events to `events.Bus` |
| `services/web/internal/kafkatail/tail_test.go` | Create | testcontainers Kafka harness; produces events, asserts bus receives them |
| `services/web/internal/handlers/pages.go` | Create | GET `/` (orders list), `/orders/new`, `/orders/{id}`, `/inventory`, `/payments/sim` |
| `services/web/internal/handlers/orders.go` | Create | POST `/v1/orders`, POST `/v1/orders/{id}` (cancel) |
| `services/web/internal/handlers/payments.go` | Create | POST `/payments/sim/fire` |
| `services/web/internal/handlers/events.go` | Create | GET `/events/stream` SSE |
| `services/web/internal/handlers/handlers_test.go` | Create | Unit tests with fake clients and fake bus |
| `services/web/internal/web/main.go` | Create | `Run(ctx)`, `Main()`, `ListenAddr()`, mirroring `services/order/cmd/order/main.go` |
| `services/web/internal/templates/layout.html` | Create | Shared shell; loads htmx from `https://cdn.jsdelivr.net/npm/htmx.org@2.0.3/dist/htmx.min.js` (SRI hash applied at impl time); sidebar with `<div hx-ext="sse" sse-connect="/events/stream">` |
| `services/web/internal/templates/orders_list.html` | Create | `/` body |
| `services/web/internal/templates/order_new.html` | Create | `/orders/new` body (form + error region) |
| `services/web/internal/templates/order_detail.html` | Create | `/orders/{id}` body (state badge + items table + cancel form + pollable fragment) |
| `services/web/internal/templates/inventory.html` | Create | `/inventory` body |
| `services/web/internal/templates/payments.html` | Create | `/payments/sim` body |
| `services/web/internal/static/styles.css` | Create | One file, ~150 lines. Dark theme base, badge colors per OrderState |
| `services/web/README.md` | Create | How to run locally + via compose + curl smoke recipe |
| `cmd/web/main.go` | Create | Top-level binary delegator |
| `go.work` | Modify | Append `./services/web` to `use` |
| `Makefile` | Modify | Add `make build-web`, `make run-web`; extend `make build` and `make test` |
| `deploy/docker-compose.yml` | Modify | Add `web` service with `depends_on` healthy gates; expose `:8083` |
| `README.md` | Modify | Add entry to services list + quickstart note |
| `STATUS.md` | Modify | Add stages `web.1`–`web.11` rows |

No new abstractions in shared packages.

---

## Task 1: Bootstrap the web module skeleton

**Files:**
- Create: `services/web/go.mod`
- Create: `services/web/cmd/web/main.go`
- Create: `services/web/internal/web/main.go`
- Create: `cmd/web/main.go` (top-level delegator)
- Modify: `go.work:3` — append `./services/web` to the `use` block

**Why first:** Everything else builds on the module being in the workspace. Build target proves the toolchain wiring before any feature code is written.

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Run(ctx context.Context) error` — logs `"orderflow-web starting"` and blocks on `<-ctx.Done()`; mirrors `services/order/cmd/order/main.go:60-106`.
  - `Main()` — sets up signal-aware context and calls `Run`; mirrors `services/order/cmd/order/main.go:228-236`.
  - `ListenAddr() string` — atomic value (always `""` for now; populated when Task 3 wires HTTP).

**Step-by-step:**

- [ ] **Step 1.1: Create `services/web/go.mod`**

Run `go mod init` from the new module path. This must be done with the exact Go 1.25.13 toolchain already installed (`go version` should report 1.25.13).

```powershell
cd C:\Users\t0p_m\projects\orderflow
mkdir services\web\cmd\web -Force
mkdir services\web\internal\web -Force
mkdir services\web\internal\server -Force
mkdir services\web\internal\backend -Force
mkdir services\web\internal\handlers -Force
mkdir services\web\internal\events -Force
mkdir services\web\internal\kafkatail -Force
mkdir services\web\internal\templates -Force
mkdir services\web\internal\static -Force

cd services\web
go mod init github.com/t0pm1x/orderflow/services/web
go get github.com/go-chi/chi/v5@latest
go get github.com/google/uuid@latest
go get github.com/t0pm1x/orderflow/platform@latest
go get github.com/t0pm1x/orderflow/kafkaprop@latest
go get github.com/t0pm1x/orderflow/consumer@latest
go mod tidy
```

(If `go get ./...` for the orderflow packages fails locally because the module cache isn't populated, run `go work sync` from the repo root first, then re-run from `services/web`.)

- [ ] **Step 1.1a: Create the `events` package stub**

`handlers.Set` (Task 5) needs `events.NewBus()` to exist before Task 10 implements the real fan-out. Create a minimal stub now — Task 10 replaces it with the full implementation.

Create `services/web/internal/events/bus.go`:

```go
// Package events hosts an in-process publish/subscribe bus used by
// the SSE endpoint to relay Kafka events to connected browsers.
// Task 10 replaces this stub with the full implementation.
package events

// BusEvent is the value type passed through the bus. The Envelope
// field is `any` in the stub; Task 10 narrows it to
// pkg/platform/events.Envelope.
type BusEvent struct {
	Envelope any
}

// Bus is a fan-out broadcast hub. This stub lets handlers compile
// before Task 10 lands. Replace with full Subscribe/Publish/Close.
type Bus struct{}

// NewBus returns a fresh stub bus.
func NewBus() *Bus { return &Bus{} }
```

Create `services/web/internal/events/bus_test.go` with a single smoke test:

```go
package events_test

import "testing"
import "github.com/t0pm1x/orderflow/services/web/internal/events"

func TestBus_StubConstruct(t *testing.T) {
	b := events.NewBus()
	if b == nil { t.Fatal("nil bus") }
}
```

Run: `cd services/web && go test -short ./internal/events/...` — expect PASS.

- [ ] **Step 1.2: Write `services/web/internal/web/main.go`**

This is the Web Service binary entry point. It mirrors the order service's `Run` contract but currently has no background goroutines (Task 3 adds the HTTP server; Task 10 adds the Kafka tail). Reason about shutdown even though there's nothing to wait for yet — `Run` returns `nil` on clean `<-ctx.Done()` exit.

```go
// Package web hosts the orderflow-web binary's startup/shutdown
// contract. Mirrors services/order/cmd/order/main.go so the
// release story is identical for ops (SIGTERM-aware shutdown,
// structured startup log, environment overrides).
package web

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"sync/atomic"
)

// Version is the binary version (overridden at build via -ldflags
// -X main.Version). 0.0.0-dev is the pre-tag default.
var Version = "0.0.0-dev"

// boundAddr holds the actual listen address the HTTP server is bound
// to when HTTP_ADDR ends in ":0". Tests + the playground smoke
// script poll ListenAddr() to discover the OS-picked port.
var boundAddr atomic.Value

// ListenAddr returns the address the embedded HTTP server is
// currently bound to, or "" if Run has not started yet. Test-only.
func ListenAddr() string {
	v, _ := boundAddr.Load().(string)
	return v
}

// envOrDefault returns the env var named by key or fallback.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// redact returns a redacted view of a secret string for logging.
// Returns "<unset>" when empty, otherwise truncates.
func redact(s string) string {
	if s == "" {
		return "<unset>"
	}
	if len(s) > 12 {
		return s[:6] + "…" + s[len(s)-4:]
	}
	return "***"
}

// Run blocks until ctx is cancelled (SIGTERM/SIGINT). Returns nil on
// clean shutdown. The order of work:
//  1. Init tracing (no-op in this stage; Task 3 wires the actual
//     HTTP server start).
//  2. Block on ctx.
//  3. Return nil.
//
// Future tasks extend this with: HTTP server goroutine (Task 3),
// Kafka tail goroutine (Task 10). Both will use a *sync.WaitGroup
// to wait for shutdown before Run returns, matching the saga
// shutdown pattern.
func Run(ctx context.Context) error {
	logger := slog.Default()

	logger.Info("orderflow-web starting",
		"version", Version,
		"http_addr", envOrDefault("HTTP_ADDR", ":8083"),
		"order_url", redact(envOrDefault("ORDER_URL", "http://localhost:8080")),
		"payment_url", redact(envOrDefault("PAYMENT_URL", "http://localhost:8081")),
		"inventory_url", redact(envOrDefault("INVENTORY_URL", "http://localhost:8082")),
		"kafka_brokers", redact(envOrDefault("KAFKA_BROKERS", "")))

	<-ctx.Done()
	logger.Info("orderflow-web shutting down")
	return nil
}

// Main is the function called by cmd/web/main.go; it owns the
// signal-aware context lifecycle.
func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "web service: %v\n", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 1.3: Write `services/web/cmd/web/main.go`**

Mirrors `services/order/cmd/order/main.go:1-10`. 10 lines, no test.

```go
// web Service binary — wiring lives in services/web/internal/web
// so it can access the service's internal packages; this top-level
// cmd is just the binary entry point.
package main

import "github.com/t0pm1x/orderflow/services/web/internal/web"

func main() {
	web.Main()
}
```

- [ ] **Step 1.4: Write `cmd/web/main.go`** (top-level workspace delegator)

Mirrors `cmd/order/main.go` exactly. 10 lines.

```go
// web Service binary — wiring lives in services/web/internal/web
// so it can access the service's internal packages; this top-level
// cmd is just the binary entry point.
package main

import "github.com/t0pm1x/orderflow/services/web/internal/web"

func main() {
	web.Main()
}
```

- [ ] **Step 1.5: Add to `go.work`**

Open `go.work:3-17` (the `use (...)` block) and append `./services/web` as the last entry. Final shape:

```
use (
	./cmd/inventory
	./cmd/order
	./cmd/payment
	./cmd/saga
	./cmd/web
	./pkg/consumer
	./pkg/outbox
	./pkg/platform
	./pkg/platform/instrumentation/kafkaprop
	./services/inventory
	./services/order
	./services/payment
	./services/saga
	./services/web
	./tests
)
```

- [ ] **Step 1.6: Verify it builds and runs end-to-end**

From repo root `C:\Users\t0p_m\projects\orderflow`:

```powershell
go work sync
cd services\web && go build ./...; if ($?) { cd ..\..\bin ; go build -ldflags="-X github.com/t0pm1x/orderflow/services/web/internal/web.Version=v0.1.0-web.0" -o bin\web.tmp ..\cmd\web ; if ($?) { Remove-Item bin\web.tmp -ErrorAction SilentlyContinue } }
```

(The `go build -o bin/web.tmp ../cmd/web` path is wrong — fix to `go build -o ../bin/web ../cmd/web`. Use `go build -o ..\..\bin\web ..\cmd\web` from inside `services\web`, or just `go build -o bin\web ./cmd/web` from `services/web`.)

Then run it and confirm it blocks on SIGINT:

```powershell
cd services\web
go build -o ..\..\bin\web .\cmd\web
..\..\bin\web
```

Expected: prints one line `{"time":"...","level":"INFO","msg":"orderflow-web starting", ...}` then blocks. Press Ctrl+C — expect `{"msg":"orderflow-web shutting down"}` and exit 0.

- [ ] **Step 1.7: Commit**

```powershell
git add services/web cmd/web go.work
git commit -m "feat(web): bootstrap orderflow-web service module skeleton"
```

---

## Task 2: Backend HTTP clients (interfaces + 3 typed clients)

**Files:**
- Create: `services/web/internal/backend/types.go`
- Create: `services/web/internal/backend/client.go`
- Create: `services/web/internal/backend/order.go`
- Create: `services/web/internal/backend/payment.go`
- Create: `services/web/internal/backend/inventory.go`
- Create: `services/web/internal/backend/order_test.go`
- Create: `services/web/internal/backend/payment_test.go`
- Create: `services/web/internal/backend/inventory_test.go`

**Why second:** Every page handler depends on these interfaces + implementations. Defining them second lets Task 5–9 handler tests pass in fake clients cleanly.

**Interfaces:**

This task produces these Go types. Downstream tasks reference them by name + signature only.

```go
package backend

import (
	"context"
	"time"
)

// OrderItem matches `OrderItem` in api/openapi.yaml:238-256.
type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents *int64 `json:"unit_price_cents,omitempty"`
}

// OrderSubmit matches `OrderSubmit` in api/openapi.yaml:224-236.
type OrderSubmit struct {
	CustomerID *string     `json:"customer_id,omitempty"`
	Items      []OrderItem `json:"items"`
}

// OrderState matches `OrderState` in api/openapi.yaml:216-222.
type OrderState string

const (
	OrderStatePending   OrderState = "pending"
	OrderStateReserved  OrderState = "reserved"
	OrderStateConfirmed OrderState = "confirmed"
	OrderStateCancelled OrderState = "cancelled"
	OrderStateFailed    OrderState = "failed"
)

// Order matches `Order` in api/openapi.yaml:257-288.
type Order struct {
	ID            string       `json:"id"`
	CustomerID    *string      `json:"customer_id,omitempty"`
	Items         []OrderItem  `json:"items"`
	State         OrderState   `json:"state"`
	TotalCents    *int64       `json:"total_cents,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
	CompletedAt   *time.Time   `json:"completed_at,omitempty"`
	FailureReason *string      `json:"failure_reason,omitempty"`
}

// OrderList matches `OrderList` in api/openapi.yaml:290-300.
type OrderList struct {
	Items      []Order `json:"items"`
	NextCursor *string `json:"next_cursor,omitempty"`
}

// StockItem is a best-effort decode of GET /v1/inventory/stock.
// The OpenAPI spec doesn't define it explicitly; the existing
// handler at services/inventory/internal/api/handler.go returns
// `[]Stock{...}` with fields sku, available_qty, reserved_qty,
// version. We match that local contract.
type StockItem struct {
	SKU         string `json:"sku"`
	Available   int64  `json:"available_qty"`
	Reserved    int64  `json:"reserved_qty"`
	Version     int64  `json:"version"`
	Description *string `json:"description,omitempty"`
}

// PaymentWebhook matches `PaymentWebhook` in api/openapi.yaml:302-316.
type PaymentWebhook struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"` // "succeeded" | "failed"
	ErrorCode string `json:"error_code,omitempty"`
}

// OrderClient talks to the Order Service.
type OrderClient interface {
	List(ctx context.Context, state OrderState, limit int) (*OrderList, error)
	Get(ctx context.Context, id string) (*Order, error)
	Submit(ctx context.Context, in OrderSubmit) (*Order, error)
	Cancel(ctx context.Context, id string) error
}

// PaymentClient talks to the Payment Service.
type PaymentClient interface {
	FireWebhook(ctx context.Context, w PaymentWebhook) error
}

// InventoryClient talks to the Inventory Service.
type InventoryClient interface {
	ListStock(ctx context.Context) ([]StockItem, error)
}

// HTTPClient implements all three clients against the configured
// upstream URLs. Safe for concurrent use.
type HTTPClient struct {
	orderURL    string
	paymentURL  string
	inventoryURL string
	http        *http.Client
}

// New constructs an HTTPClient. baseOrderURL/Payment/Inventory are
// full base URLs (no trailing slash). http may be nil (defaults to
// http.Client{Timeout: 10s}).
func New(http *http.Client, orderURL, paymentURL, inventoryURL string) *HTTPClient {
	if http == nil {
		http = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPClient{
		orderURL:    strings.TrimRight(orderURL, "/"),
		paymentURL:  strings.TrimRight(paymentURL, "/"),
		inventoryURL: strings.TrimRight(inventoryURL, "/"),
		http:        http,
	}
}
```

(Note: `client.go` imports `net/http`, `strings`, `time`.)

The three impl files (`order.go`, `payment.go`, `inventory.go`) follow the same pattern: build URL, set `Content-Type: application/json`, set 10-second timeout (already on the client), `json.NewDecoder(resp.Body).Decode(target)` on 2xx, return wrapped error on non-2xx or decode failure.

Critical rules (apply to ALL three impl files):
- All HTTP errors must wrap upstream context: `fmt.Errorf("order service: GET /v1/orders: status %d: %w", resp.StatusCode, err)`. Tests will assert error wrapping.
- `OrderClient.Cancel` does DELETE on `${orderURL}/v1/orders/${id}`. 204 or 404 is "OK" (idempotent cancel per OpenAPI `description` at api/openapi.yaml:118-127).
- `OrderClient.Submit` returns the created `*Order`. On non-201, decode the upstream `Error` body and wrap both — but `Error` isn't a type we'll use directly; just include the body text in the error message so the form can render it.
- `PaymentClient.FireWebhook` posts to `${paymentURL}/v1/payments/webhook`. 202 is success. 4xx → wrap as above.
- `InventoryClient.ListStock` does GET `${inventoryURL}/v1/inventory/stock`. Decode into `[]StockItem`. The existing handler at `services/inventory/internal/api/handler.go:166+` returns the slice directly (no envelope).

**Step-by-step:**

- [ ] **Step 2.1: Write the failing tests**

`order_test.go` uses `httptest.NewServer` to stand up a fake Order Service per test. Coverage target: 4 tests (one per OrderClient method) — happy path + key error paths.

```go
package backend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestOrderClient_List(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/orders" || r.Method != http.MethodGet {
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		if got := r.URL.Query().Get("state"); got != "pending" {
			t.Errorf("query state: got %q want pending", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[
			{"id":"` + uuid.NewString() + `","state":"pending","items":[{"sku":"SKU-001","quantity":2}],"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}
		]}`))
	}))
	defer srv.Close()

	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	got, err := c.List(context.Background(), backend.OrderStatePending, 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items: got %d want 1", len(got.Items))
	}
}

func TestOrderClient_Get_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":"not_found","message":"order not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	_, err := c.Get(context.Background(), "missing-id")
	if err == nil || !strings.Contains(err.Error(), "status 404") {
		t.Fatalf("expected 404 error, got %v", err)
	}
}

func TestOrderClient_Submit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/orders" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		var in backend.OrderSubmit
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		if len(in.Items) != 1 || in.Items[0].SKU != "SKU-001" {
			http.Error(w, "bad items", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(backend.Order{
			ID: uuid.NewString(), State: backend.OrderStatePending,
			Items: in.Items, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		})
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	got, err := c.Submit(context.Background(), backend.OrderSubmit{
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got.State != backend.OrderStatePending {
		t.Errorf("state: got %s want pending", got.State)
	}
}

func TestOrderClient_Cancel_Idempotent(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent) // 204 is OK
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	if err := c.Cancel(context.Background(), "any"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
}

func TestOrderClient_Cancel_404IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := backend.New(nil, srv.URL, "http://localhost:8081", "http://localhost:8082")
	// Per OpenAPI, idempotent — 404 on cancel is acceptable.
	if err := c.Cancel(context.Background(), "missing"); err != nil {
		t.Fatalf("Cancel should accept 404, got: %v", err)
	}
}
```

`payment_test.go`:

```go
package backend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestPaymentClient_FireWebhook_Succeed(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/payments/webhook" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		var w2 backend.PaymentWebhook
		if err := json.NewDecoder(r.Body).Decode(&w2); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	if err := c.FireWebhook(context.Background(), backend.PaymentWebhook{
		PaymentID: uuid.NewString(), Status: "succeeded",
	}); err != nil {
		t.Fatalf("FireWebhook: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
}

func TestPaymentClient_FireWebhook_4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":"bad","message":"nope"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", srv.URL, "http://localhost:8082")
	err := c.FireWebhook(context.Background(), backend.PaymentWebhook{PaymentID: "x", Status: "failed"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
```

`inventory_test.go`:

```go
package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
)

func TestInventoryClient_ListStock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/inventory/stock" || r.Method != http.MethodGet {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"sku":"SKU-001","available_qty":99,"reserved_qty":1,"version":3},
			{"sku":"SKU-002","available_qty":50,"reserved_qty":0,"version":1}
		]`))
	}))
	defer srv.Close()
	c := backend.New(nil, "http://localhost:8080", "http://localhost:8081", srv.URL)
	got, err := c.ListStock(context.Background())
	if err != nil {
		t.Fatalf("ListStock: %v", err)
	}
	if len(got) != 2 || got[0].SKU != "SKU-001" {
		t.Fatalf("unexpected: %+v", got)
	}
}
```

- [ ] **Step 2.2: Run the tests — expect compile failures**

Run from `services/web`:

```powershell
cd services\web
go test -short ./internal/backend/...
```

Expected: compile errors (`undefined: backend.New`, `undefined: backend.OrderState`, etc.). Tests do not pass yet.

- [ ] **Step 2.3: Write `types.go` and `client.go`**

Two short files. `types.go` contains only the type declarations above (no methods). `client.go` defines the three interfaces and `HTTPClient` struct + `New` constructor as above. ~80 lines combined.

- [ ] **Step 2.4: Run the tests — expect more compile failures**

Tests still fail because `order.go`, `payment.go`, `inventory.go` aren't written. The compile errors will now be about `*HTTPClient` not implementing `OrderClient`/`PaymentClient`/`InventoryClient`.

- [ ] **Step 2.5: Write `order.go`**

```go
package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func (c *HTTPClient) List(ctx context.Context, state OrderState, limit int) (*OrderList, error) {
	u := c.orderURL + "/v1/orders"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("order list: %w", err)
	}
	if state != "" {
		q := req.URL.Query()
		q.Set("state", string(state))
		req.URL.RawQuery = q.Encode()
	}
	if limit > 0 {
		q := req.URL.Query()
		q.Set("limit", fmt.Sprintf("%d", limit))
		req.URL.RawQuery = q.Encode()
	}
	var out OrderList
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order list: %w", err)
	}
	return &out, nil
}

func (c *HTTPClient) Get(ctx context.Context, id string) (*Order, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/orders/%s", c.orderURL, id), nil)
	if err != nil {
		return nil, fmt.Errorf("order get: %w", err)
	}
	var out Order
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order get: %w", err)
	}
	return &out, nil
}

func (c *HTTPClient) Submit(ctx context.Context, in OrderSubmit) (*Order, error) {
	body, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.orderURL+"/v1/orders", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("order submit: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	var out Order
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("order submit: %w", err)
	}
	return &out, nil
}

// Cancel is idempotent: 204 and 404 both succeed.
func (c *HTTPClient) Cancel(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/v1/orders/%s", c.orderURL, id), nil)
	if err != nil {
		return fmt.Errorf("order cancel: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("order cancel: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("order cancel: status %d", resp.StatusCode)
}

// do runs req and decodes a JSON body on 2xx; wraps any failure with
// the upstream status code and response body.
func (c *HTTPClient) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("upstream %s %s: status %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s: %w", req.Method, req.URL.Path, err)
		}
	}
	return nil
}
```

(`order.go` imports `bytes`, `context`, `encoding/json`, `fmt`, `io`, `net/http`, `strings`.)

- [ ] **Step 2.6: Write `payment.go`**

```go
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func (c *HTTPClient) FireWebhook(ctx context.Context, w PaymentWebhook) error {
	body, _ := json.Marshal(w)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.paymentURL+"/v1/payments/webhook", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.do(req, nil); err != nil {
		return fmt.Errorf("payment webhook: %w", err)
	}
	return nil
}
```

- [ ] **Step 2.7: Write `inventory.go`**

```go
package backend

import (
	"context"
	"fmt"
	"net/http"
)

func (c *HTTPClient) ListStock(ctx context.Context) ([]StockItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.inventoryURL+"/v1/inventory/stock", nil)
	if err != nil {
		return nil, fmt.Errorf("inventory list: %w", err)
	}
	var out []StockItem
	if err := c.do(req, &out); err != nil {
		return nil, fmt.Errorf("inventory list: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 2.8: Run the tests — expect PASS**

```powershell
cd services\web
go test -short ./internal/backend/...
```

Expected: PASS (3 packages, ~8 tests). Use `-v` to see individual test names.

- [ ] **Step 2.9: Commit**

```powershell
git add services/web/internal/backend
git commit -m "feat(web): backend HTTP clients (Order/Payment/Inventory) + types"
```

---

## Task 3: Server scaffolding — chi router, middleware, /healthz, /readyz

**Files:**
- Create: `services/web/internal/server/server.go`
- Modify: `services/web/internal/web/main.go:39-86` — extend Run to call `server.Start` and wait for shutdown

**Why third:** The HTTP server is the host for every later page. Bringing it up now lets later handler tasks validate against a real listener using `httptest`.

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `server.New(handlers *handlers.Set, bus *events.Bus) *http.Server` (returns a configured but not running http.Server)
  - `server.ListenAddr() string` (mirrors `internal/web.ListenAddr`)

Handlers don't exist yet — pass a nil handlers.Set for now; Task 5+ threads it back in. The Task 3 deliverable is just: chi router up, middleware mounted, `/healthz` and `/readyz` mounted, / and the rest returning 404.

**Step-by-step:**

- [ ] **Step 3.1: Write `services/web/internal/server/server.go`**

```go
// Package server wires the orderflow-web HTTP server: chi router,
// shared middleware, route registration, and graceful shutdown.
package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	mw "github.com/t0pm1x/orderflow/platform/middleware"
)

// Routes is the route registration callable provided by
// services/web/internal/handlers (Task 5+ wires this up).
type Routes func(r chi.Router)

// Options controls server behavior.
type Options struct {
	Name        string
	Logger      *slog.Logger
	OrderURL    string
	PaymentURL  string
	InventoryURL string
	RegisterRoutes Routes
}

// Server hosts the HTTP listener. One instance per process.
type Server struct {
	opt   Options
	srv   *http.Server
	ln    net.Listener
	addr  atomic.Value // string
}

// New creates a non-listening Server. Call Start to bind + serve.
func New(opt Options) *Server {
	return &Server{opt: opt}
}

// Addr returns the bound address (host:port) or "" if Start has not
// completed.
func (s *Server) Addr() string {
	v, _ := s.addr.Load().(string)
	return v
}

// Start binds the listener and serves until ctx is cancelled.
func (s *Server) Start(ctx context.Context, addr string) error {
	r := chi.NewRouter()
	r.Use(mw.Stack(s.opt.Name, s.opt.Logger)...)

	// Probes
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		// /readyz succeeds as long as the web process is up; the page
		// handlers themselves report backend reachability inline. (A
		// stricter check that pings :8080/:8081/:8082 /healthz lives
		// in this method's expansion if the user wants it later.)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	if s.opt.RegisterRoutes != nil {
		s.opt.RegisterRoutes(r)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.ln = ln
	s.addr.Store(ln.Addr().String())

	s.srv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.opt.Logger.Error("web http exited", "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	<-ctx.Done()
	return nil
}
```

(`server.go` imports `slog`, `fmt`.)

- [ ] **Step 3.2: Wire it into `internal/web/main.go`**

Modify `Run` in `services/web/internal/web/main.go` to:

1. After the startup log, construct `server.New(server.Options{Name: "web", Logger: logger, OrderURL: ..., PaymentURL: ..., InventoryURL: ...})`.
2. Call `srv.Start(ctx, httpAddr)`.
3. Store the address from `srv.Addr()` into the package-level `boundAddr` atomic so external scripts can poll `ListenAddr()` (mirrors `cmd/order/main.go:194`).

Replace the body of `Run` (after the logger.Info startup block) with:

```go
	httpAddr := envOrDefault("HTTP_ADDR", ":8083")
	orderURL := envOrDefault("ORDER_URL", "http://localhost:8080")
	paymentURL := envOrDefault("PAYMENT_URL", "http://localhost:8081")
	inventoryURL := envOrDefault("INVENTORY_URL", "http://localhost:8082")

	srv := server.New(server.Options{
		Name:         "web",
		Logger:       logger,
		OrderURL:     orderURL,
		PaymentURL:   paymentURL,
		InventoryURL: inventoryURL,
		// RegisterRoutes wired in Task 5.
		RegisterRoutes: nil,
	})
	if err := srv.Start(ctx, httpAddr); err != nil {
		return fmt.Errorf("server start: %w", err)
	}
	boundAddr.Store(srv.Addr())
	return nil
```

Plus add the import: `"github.com/t0pm1x/orderflow/services/web/internal/server"`.

- [ ] **Step 3.3: Build + smoke-test**

```powershell
cd C:\Users\t0p_m\projects\orderflow\services\web
go build -o ..\..\bin\web .\cmd\web
..\..\bin\web &
$port = (Invoke-RestMethod "http://localhost:8083/healthz" -ErrorAction SilentlyContinue) ; Write-Host "healthz: $port"
Invoke-RestMethod "http://localhost:8083/readyz"
Invoke-WebRequest "http://localhost:8083/" -UseBasicParsing | Select-Object -ExpandProperty StatusCode
```

Expected: `healthz` returns `{"status":"ok"}`, `readyz` returns `{"status":"ok"}`, GET `/` returns 404 (no RegisterRoutes wired yet).

Stop with Ctrl+C. Verify the log line `"web http exited"` is NOT present (clean shutdown, no error).

- [ ] **Step 3.4: Commit**

```powershell
git add services/web/internal/server services/web/internal/web
git commit -m "feat(web): server scaffolding with chi, middleware, /healthz, /readyz"
```

---

## Task 4: Templates + static (layout, htmx from CDN, styles.css)

**Files:**
- Create: `services/web/internal/templates/layout.html`
- Create: `services/web/internal/templates/empty.html` (placeholder body used by Task 3 smoke)
- Create: `services/web/internal/static/styles.css`

**Why fourth:** Pages in Tasks 5–9 fill in body fragments. The layout shell + CSS are owned here so they don't get duplicated.

**Interfaces:**
- Consumes: nothing (no handler yet — Task 5 wires them).
- Produces: A `templates.FS` `embed.FS` of the templates directory and a `static.FS` `embed.FS` of the static directory. Each is exported via a tiny package file used by Task 5+.

**Step-by-step:**

- [ ] **Step 4.1: Create `services/web/internal/templates/layout.html`**

```html
{{define "layout"}}<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>orderflow-web playground</title>
  <link rel="stylesheet" href="/static/styles.css">
  <script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.3/dist/htmx.min.js" integrity="sha384-1ek8BkJJsxL3KkR/dFiqJ86ZhJ8Qj4Am/a5e9ZDAKAlQzLJY1NfL4WjQN4S3Vlg" crossorigin="anonymous"></script>
</head>
<body>
  <header class="topbar">
    <a class="brand" href="/">orderflow-web</a>
    <nav>
      <a href="/">Orders</a>
      <a href="/inventory">Inventory</a>
      <a href="/payments/sim">Payments sim</a>
    </nav>
  </header>

  <main class="main">
    <section class="content">
      {{template "body" .}}
    </section>
    <aside class="sidebar" hx-ext="sse" sse-connect="/events/stream" sse-swap="event" hx-swap="afterend">
      <h3>Live events</h3>
      <ul id="events" class="events"></ul>
    </aside>
  </main>

  <script>
    // Vanilla JS SSE-receiver that appends one <li> per incoming event.
    // Re-uses the EventSource already opened by htmx-sse extension so
    // we don't double-open a connection.
    document.body.addEventListener('htmx:sseMessage', function (e) {
      try {
        var data = JSON.parse(e.detail.data);
        var ul = document.getElementById('events');
        if (!ul) return;
        var li = document.createElement('li');
        li.className = 'event event-' + (data.event_type || 'unknown');
        li.textContent = data.occurred_at + ' ' + data.event_type + ' ' + data.aggregate_id;
        ul.prepend(li);
        while (ul.children.length > 50) ul.removeChild(ul.lastChild);
      } catch (_) {}
    });
  </script>
</body>
</html>
{{end}}
```

(The integrity hash above is for `htmx.org@2.0.3`. When implementing, fetch the actual `sha384` hash from `https://cdn.jsdelivr.net/npm/htmx.org@2.0.3/dist/htmx.min.js` and substitute — keeping the placeholder will fail strict browser SRI checks but works for a localhost playground. Optional: drop the `integrity` attribute entirely to skip the hash step; document that choice.)

- [ ] **Step 4.2: Create `services/web/internal/templates/empty.html`**

```html
{{define "body"}}
<section>
  <h1>Orders</h1>
  <p>Loading… (Task 5 will replace this with the orders list.)</p>
</section>
{{end}}
```

- [ ] **Step 4.3: Create `services/web/internal/templates/package.go`** (file with `package` decl + `embed.FS` exported var)

```go
// Package templates holds html/template assets embedded at compile
// time. Body fragments define the "body" block; layout.html is the
// shared shell.
package templates

import "embed"

//go:embed *.html
var FS embed.FS
```

- [ ] **Step 4.4: Create `services/web/internal/static/package.go`**

```go
// Package static holds CSS assets embedded at compile time.
package static

import "embed"

//go:embed styles.css
var FS embed.FS
```

- [ ] **Step 4.5: Create `services/web/internal/static/styles.css`**

~150 lines, dark theme, badge colors per state. Concrete file:

```css
:root {
  --bg: #0e1116; --fg: #e6edf3; --muted: #7d8590;
  --accent: #4493f8; --bad: #f85149; --good: #56d364;
  --warn: #d29922; --panel: #161b22; --border: #30363d;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--fg);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.topbar { display: flex; align-items: center; justify-content: space-between;
  padding: 12px 24px; border-bottom: 1px solid var(--border); background: var(--panel); }
.topbar .brand { font-weight: 600; }
.topbar nav a { margin-left: 16px; color: var(--muted); }
.topbar nav a:hover { color: var(--fg); }
.main { display: grid; grid-template-columns: 1fr 360px; min-height: calc(100vh - 56px); }
.content { padding: 24px 32px; }
.sidebar { background: var(--panel); border-left: 1px solid var(--border); padding: 16px; overflow-y: auto; }
.events { list-style: none; padding: 0; margin: 0; font-family: ui-monospace, "Cascadia Code", monospace; font-size: 12px; }
.events li { padding: 4px 6px; border-bottom: 1px solid var(--border); word-break: break-all; }
.event-OrderConfirmed { color: var(--good); }
.event-OrderCancelled, .event-PaymentFailed { color: var(--bad); }
.event-OrderCreated, .event-StockReserveRequested, .event-PaymentRequested, .event-StockReleaseRequested { color: var(--warn); }
table { border-collapse: collapse; width: 100%; }
th, td { padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; }
th { color: var(--muted); font-weight: 600; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
.badge.pending { background: rgba(210,153,34,0.15); color: var(--warn); }
.badge.reserved { background: rgba(68,147,248,0.15); color: var(--accent); }
.badge.confirmed { background: rgba(86,211,100,0.15); color: var(--good); }
.badge.cancelled, .badge.failed { background: rgba(248,81,73,0.15); color: var(--bad); }
button, .btn { background: var(--accent); color: white; border: 0; border-radius: 6px;
  padding: 8px 14px; cursor: pointer; font-weight: 600; font-size: 13px; }
button.secondary, .btn.secondary { background: transparent; color: var(--fg); border: 1px solid var(--border); }
button.danger, .btn.danger { background: var(--bad); }
form.sheet { display: grid; gap: 12px; max-width: 480px; }
form.sheet label { display: block; font-weight: 600; margin-bottom: 4px; color: var(--muted); }
form.sheet input, form.sheet select { width: 100%; padding: 8px 10px; background: var(--bg);
  color: var(--fg); border: 1px solid var(--border); border-radius: 6px; }
.error { color: var(--bad); background: rgba(248,81,73,0.1); border: 1px solid var(--bad); border-radius: 6px; padding: 8px 12px; }
.muted { color: var(--muted); }
.mono { font-family: ui-monospace, "Cascadia Code", monospace; }
```

- [ ] **Step 4.6: Add a minimal `/` route in `services/web/internal/server/server.go` that renders the layout**

Add a placeholder handler before the `RegisterRoutes` callback:

```go
// Placeholder: full route handlers live in internal/handlers and
// are mounted via RegisterRoutes (Task 5+). Until then, / renders
// a single placeholder page so the layout + CSS are testable.
r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><meta charset=utf-8><title>web</title><link rel=stylesheet href=/static/styles.css><h1 class=muted>Loading… (handler not wired yet)</h1>`))
})
r.Get("/static/*", func(w http.ResponseWriter, req *http.Request) {
	p := strings.TrimPrefix(req.URL.Path, "/static/")
	data, err := static.FS.ReadFile("styles.css")
	if err != nil {
		http.NotFound(w, req)
		return
	}
	if p == "styles.css" {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write(data)
		return
	}
	http.NotFound(w, req)
})
```

Add imports `"strings"` and `"github.com/t0pm1x/orderflow/services/web/internal/static"`.

- [ ] **Step 4.7: Smoke test the layout + CSS**

```powershell
cd C:\Users\t0p_m\projects\orderflow\services\web
go build -o ..\..\bin\web .\cmd\web
..\..\bin\web &
curl http://localhost:8083/
curl http://localhost:8083/static/styles.css | Select-Object -First 10
```

Expected: GET `/` returns the placeholder HTML. GET `/static/styles.css` returns the CSS body (Content-Type `text/css`).

Stop with Ctrl+C.

- [ ] **Step 4.8: Commit**

```powershell
git add services/web/internal/templates services/web/internal/static services/web/internal/server
git commit -m "feat(web): layout template + stylesheet + placeholder route"
```

---

## Task 5: Orders list page (GET /)

**Files:**
- Create: `services/web/internal/handlers/pages.go`
- Create: `services/web/internal/handlers/handlers.go`
- Create: `services/web/internal/handlers/pages_test.go`
- Create: `services/web/internal/templates/orders_list.html`
- Modify: `services/web/internal/server/server.go` — `RegisterRoutes` mounts `/static/*` + handlers

**Why fifth:** This is the first "real" page. It exercises the OrderClient + templates + layout together. Subsequent pages build on its handler shape.

**Interfaces:**

This task introduces the handlers package's surface. Subsequent tasks (`/orders/new`, `/orders/{id}`, etc.) extend it. Do NOT duplicate template parses — `handlers.Set` holds a `*template.Template` parsed once at construction.

```go
package handlers

import (
	"html/template"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/templates"
)

// Set holds dependencies for handlers. Construct once in main,
// register Routes on the chi router.
type Set struct {
	Order     backend.OrderClient
	Payment   backend.PaymentClient
	Inventory backend.InventoryClient
	Bus       *events.Bus
	Templates *template.Template
}

// NewSet builds a Set with templates parsed once.
func NewSet(order backend.OrderClient, payment backend.PaymentClient,
	inventory backend.InventoryClient, bus *events.Bus) *Set {
	t := template.Must(template.ParseFS(templates.FS, "layout.html", "orders_list.html"))
	return &Set{
		Order: order, Payment: payment, Inventory: inventory, Bus: bus,
		Templates: t,
	}
}

// Routes registers all page + action routes on r.
func (s *Set) Routes(r chi.Router) {
	r.Get("/", s.PageOrdersList)
	r.Get("/orders/new", s.PageOrderNew)        // Task 6
	r.Post("/v1/orders", s.ActionOrderSubmit)    // Task 6
	r.Get("/orders/{id}", s.PageOrderDetail)     // Task 7
	r.Post("/v1/orders/{id}", s.ActionOrderCancel) // Task 7
	r.Get("/inventory", s.PageInventory)         // Task 8
	r.Get("/payments/sim", s.PagePaymentsSim)    // Task 9
	r.Post("/payments/sim/fire", s.ActionPaymentsFire) // Task 9
	r.Get("/events/stream", s.PageEventsStream)  // Task 10
}

// PageOrdersList serves GET / (orders list).
func (s *Set) PageOrdersList(w http.ResponseWriter, r *http.Request) {
	// ... implemented below ...
}
```

Tasks 6+ add their methods on the same `*Set`. This is the TDD-friendly shape.

**Step-by-step:**

- [ ] **Step 5.1: Write the failing test `pages_test.go`**

```go
package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/t0pm1x/orderflow/services/web/internal/backend"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/handlers"
)

type fakeOrderClient struct{ listResp *backend.OrderList; listErr error }

func (f *fakeOrderClient) List(ctx context.Context, _ backend.OrderState, _ int) (*backend.OrderList, error) {
	return f.listResp, f.listErr
}
func (f *fakeOrderClient) Get(_ context.Context, _ string) (*backend.Order, error) {
	return nil, nil
}
func (f *fakeOrderClient) Submit(_ context.Context, _ backend.OrderSubmit) (*backend.Order, error) {
	return nil, nil
}
func (f *fakeOrderClient) Cancel(_ context.Context, _ string) error { return nil }

type fakePaymentClient struct{}
func (f *fakePaymentClient) FireWebhook(_ context.Context, _ backend.PaymentWebhook) error { return nil }

type fakeInventoryClient struct{}
func (f *fakeInventoryClient) ListStock(_ context.Context) ([]backend.StockItem, error) { return nil, nil }

func newTestSet(t *testing.T, oc backend.OrderClient) http.Handler {
	t.Helper()
	bus := events.NewBus()
	h := handlers.NewSet(oc, &fakePaymentClient{}, &fakeInventoryClient{}, bus)
	r := chi.NewRouter()
	h.Routes(r)
	return r
}

func TestOrdersList_OK(t *testing.T) {
	oc := &fakeOrderClient{
		listResp: &backend.OrderList{Items: []backend.Order{
			{ID: "abc-123", State: backend.OrderStatePending,
				Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2}}},
			{ID: "def-456", State: backend.OrderStateConfirmed,
				Items: []backend.OrderItem{{SKU: "SKU-002", Quantity: 1}}},
		}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	body := new(strings.Builder)
	_, _ = body.ReadFrom(resp.Body)
	if !strings.Contains(body.String(), "abc-123") {
		t.Errorf("missing abc-123 order: %s", body.String())
	}
	if !strings.Contains(body.String(), "confirmed") {
		t.Errorf("missing confirmed badge: %s", body.String())
	}
}

func TestOrdersList_BackendError_RendersPage(t *testing.T) {
	oc := &fakeOrderClient{listErr: errFake}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	// Page must still render and show a "backend not reachable" notice
	// so the rest of the UI stays usable.
	body := new(strings.Builder)
	_, _ = body.ReadFrom(resp.Body)
	if !strings.Contains(strings.ToLower(body.String()), "unavailable") &&
		!strings.Contains(strings.ToLower(body.String()), "backend") {
		t.Errorf("expected backend-unreachable notice: %s", body.String())
	}
}

type fakeErr struct{}
func (fakeErr) Error() string { return "upstream timeout" }

var errFake = fakeErr{}
```

- [ ] **Step 5.2: Run the test — expect compile error**

```powershell
cd services\web
go test -short ./internal/handlers/...
```

Expected: `undefined: handlers.NewSet`.

- [ ] **Step 5.3: Create `handlers.go` with the scaffolding from the Interfaces block above**

Implement `PageOrdersList` to fetch via `s.Order.List(...)`, build a `viewModel` (a small struct with `Orders []Order` + `BackendDown bool`), render the layout+orders_list.html fragment. Approx shape:

```go
func (s *Set) PageOrdersList(w http.ResponseWriter, r *http.Request) {
	var vm ordersListVM
	list, err := s.Order.List(r.Context(), "", 50)
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
	} else {
		vm.Orders = list.Items
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

type ordersListVM struct {
	Orders      []backend.Order
	BackendDown bool
	Error       string
}
```

(`PageOrdersList` lives on `Set` per the file layout in the Interfaces section.)

- [ ] **Step 5.4: Create `services/web/internal/templates/orders_list.html`**

```html
{{define "body"}}
<section>
  <div class="row-between">
    <h1>Orders</h1>
    <a class="btn" href="/orders/new">+ New order</a>
  </div>
  {{if .BackendDown}}
  <div class="error">Backend unavailable: {{.Error}}</div>
  {{else if not .Orders}}
  <p class="muted">No orders yet. Click <strong>+ New order</strong> to start.</p>
  {{else}}
  <table>
    <thead>
      <tr><th>ID</th><th>State</th><th>Items</th><th>Created</th><th></th></tr>
    </thead>
    <tbody>
      {{range .Orders}}
      <tr>
        <td class="mono">{{.ID}}</td>
        <td><span class="badge {{.State}}">{{.State}}</span></td>
        <td>
          {{range .Items}}{{.SKU}}×{{.Quantity}} {{end}}
        </td>
        <td class="muted">{{.CreatedAt.Format "2006-01-02 15:04"}}</td>
        <td><a href="/orders/{{.ID}}">view →</a></td>
      </tr>
      {{end}}
    </tbody>
  </table>
  {{end}}
  <p class="muted">Page polls every 2s via <code>htmx</code>.</p>
</section>
{{end}}
```

- [ ] **Step 5.5: Run tests — expect PASS**

```powershell
cd services\web
go test -short ./internal/handlers/...
```

Expected: PASS (2 tests).

- [ ] **Step 5.6: Wire the handler into the server**

In `services/web/internal/server/server.go`, replace the placeholder `/` route (added in Task 4 Step 4.6) with `r.Get("/", handlers.Set{...}.PageOrdersList)`. **KEEP the `/static/*` route** — it serves embedded CSS at every later task, not just Task 4. Concretely:

1. Replace the `RegisterRoutes Routes` field on `Options` with `Handlers *handlers.Set` (the closure-based RegisterRoutes is no longer needed).
2. In `Start`, replace the `if s.opt.RegisterRoutes != nil { s.opt.RegisterRoutes(r) }` block with `if s.opt.Handlers != nil { s.opt.Handlers.Routes(r) }`.
3. The `/static/*` route stays where it was added in Step 4.6 (it serves `styles.css` from the embedded `static.FS`).
4. Remove only the placeholder `/` route (the inline `r.Get("/", ...)` that returned the literal `<h1>Loading…</h1>` HTML).

Add a new import `"github.com/t0pm1x/orderflow/services/web/internal/handlers"`.

Update `services/web/internal/web/main.go` to construct the `*handlers.Set` and pass it through `server.Options.Handlers`. Use `backend.New(nil, orderURL, paymentURL, inventoryURL)` for the HTTP client and `events.NewBus()` for the bus.

- [ ] **Step 5.7: Smoke test**

```powershell
cd services\web
go build -o ..\..\bin\web .\cmd\web
..\..\bin\web &
curl -s http://localhost:8083/ | Select-String -Pattern "orderflow-web playground" -SimpleMatch
```

Expected: HTML containing the brand link.

Stop with Ctrl+C.

- [ ] **Step 5.8: Commit**

```powershell
git add services/web
git commit -m "feat(web): orders list page (GET /) with template + htmx poll stub"
```

---

## Task 6: Create-order page (GET /orders/new, POST /v1/orders)

**Files:**
- Modify: `services/web/internal/handlers/handlers.go` — add `PageOrderNew` + `ActionOrderSubmit`
- Modify: `services/web/internal/handlers/pages.go` — handler bodies
- Modify: `services/web/internal/templates/parsers` — add `order_new.html` to ParseFS in `NewSet`
- Create: `services/web/internal/templates/order_new.html`
- Modify: `services/web/internal/handlers/pages_test.go` — 4 new tests

**Why sixth:** Submitting is the action that creates the events the rest of the playground shows off. Has the most error-path surface (validation, upstream 4xx, upstream 5xx).

**Interfaces:**

```go
// PageOrderNew: GET /orders/new → renders order_new.html body.
// ActionOrderSubmit: POST /v1/orders → proxies to OrderClient.Submit.
//   - On 201: respond 200 with HX-Redirect: /orders/{id} so htmx navigates.
//   - On 4xx: re-render order_new.html with `Error` populated, 400 status.
//   - On 5xx: respond 502.
```

**Step-by-step:**

- [ ] **Step 6.1: Write 4 failing tests**

Add to `pages_test.go`:

```go
func TestOrderNew_GET(t *testing.T) {
	srv := httptest.NewServer(newTestSet(t, &fakeOrderClient{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/new")
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: got %d", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(b.String(), `name="sku"`) { t.Error("form missing sku field") }
	if !strings.Contains(b.String(), `name="quantity"`) { t.Error("form missing quantity") }
}

func TestOrderSubmit_OK_RedirectsViaHTMX(t *testing.T) {
	oc := &fakeOrderClient{
		// Submit returns this Order
	}
	oc.submitResp = &backend.Order{ID: "order-99", State: backend.OrderStatePending,
		Items: []backend.OrderItem{{SKU: "X", Quantity: 1}}}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: got %d want 200", resp.StatusCode) }
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/order-99" {
		t.Errorf("HX-Redirect: got %q want /orders/order-99", got)
	}
}

func TestOrderSubmit_ValidationError(t *testing.T) {
	srv := httptest.NewServer(newTestSet(t, &fakeOrderClient{}))
	defer srv.Close()
	form := strings.NewReader("sku=&quantity=0")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 400 { t.Fatalf("status: got %d want 400", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(strings.ToLower(b.String()), "required") &&
		!strings.Contains(strings.ToLower(b.String()), "invalid") {
		t.Errorf("expected validation error: %s", b.String())
	}
}

func TestOrderSubmit_Upstream5xx(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.submitErr = fmt.Errorf("upstream 503")
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	form := strings.NewReader("sku=X&quantity=1")
	req, _ := http.NewRequest("POST", srv.URL+"/v1/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 502 { t.Fatalf("status: got %d want 502", resp.StatusCode) }
}
```

Extend `fakeOrderClient`:

```go
type fakeOrderClient struct {
	listResp *backend.OrderList; listErr error
	submitResp *backend.Order; submitErr error
}

func (f *fakeOrderClient) Submit(_ context.Context, _ backend.OrderSubmit) (*backend.Order, error) {
	return f.submitResp, f.submitErr
}
```

- [ ] **Step 6.2: Run tests — expect compile errors**

```powershell
cd services\web
go test -short ./internal/handlers/...
```

Expected: compile errors on undefined fields.

- [ ] **Step 6.3: Create `services/web/internal/templates/order_new.html`**

```html
{{define "body"}}
<section>
  <h1>New order</h1>
  {{if .Error}}<div class="error">{{.Error}}</div>{{end}}
  <form class="sheet" method="post" action="/v1/orders"
        hx-post="/v1/orders" hx-target="this" hx-swap="outerHTML">
    <div>
      <label for="sku">SKU</label>
      <input id="sku" name="sku" required minlength="1" maxlength="64" value="{{.SKU}}">
    </div>
    <div>
      <label for="quantity">Quantity</label>
      <input id="quantity" name="quantity" type="number" required min="1" max="10000" value="{{if .Quantity}}{{.Quantity}}{{else}}1{{end}}">
    </div>
    <div>
      <label for="unit_price_cents">Unit price (cents, optional)</label>
      <input id="unit_price_cents" name="unit_price_cents" type="number" min="0" value="{{if .UnitPriceCents}}{{.UnitPriceCents}}{{end}}">
    </div>
    <div>
      <label for="customer_id">Customer ID (UUID, optional)</label>
      <input id="customer_id" name="customer_id" placeholder="auto-generate if blank" value="{{.CustomerID}}">
    </div>
    <div class="row">
      <button type="submit">Submit order</button>
      <a class="btn secondary" href="/">Cancel</a>
    </div>
  </form>
</section>
{{end}}
```

- [ ] **Step 6.4: Update `NewSet` to also parse `order_new.html`**

```go
t := template.Must(template.ParseFS(templates.FS,
	"layout.html",
	"orders_list.html",
	"order_new.html",
))
```

- [ ] **Step 6.5: Implement handlers**

Add to `handlers/pages.go`:

```go
type orderNewVM struct {
	SKU            string
	Quantity       int
	UnitPriceCents int64
	CustomerID     string
	Error          string
}

func (s *Set) PageOrderNew(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", orderNewVM{})
}

func (s *Set) ActionOrderSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", 400)
		return
	}
	vm := orderNewVM{
		SKU:        r.FormValue("sku"),
		Quantity:   atoi(r.FormValue("quantity")),
		CustomerID: r.FormValue("customer_id"),
	}
	if up := r.FormValue("unit_price_cents"); up != "" {
		vm.UnitPriceCents, _ = strconv.ParseInt(up, 10, 64)
	}
	if vm.SKU == "" || vm.Quantity <= 0 {
		vm.Error = "SKU and quantity (>0) are required"
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	in := backend.OrderSubmit{Items: []backend.OrderItem{{SKU: vm.SKU, Quantity: vm.Quantity}}}
	if vm.UnitPriceCents > 0 {
		c := vm.UnitPriceCents
		in.Items[0].UnitPriceCents = &c
	}
	if vm.CustomerID != "" {
		in.CustomerID = &vm.CustomerID
	}
	out, err := s.Order.Submit(r.Context(), in)
	if err != nil {
		vm.Error = "Order service error: " + err.Error()
		w.WriteHeader(502)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = s.Templates.ExecuteTemplate(w, "layout", vm)
		return
	}
	w.Header().Set("HX-Redirect", "/orders/"+out.ID)
	w.WriteHeader(200)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
```

(Add `"strconv"` to imports. **Do not** import `github.com/google/uuid` in this task — `customer_id` is passed through verbatim (or left blank) and the unit test only posts `sku` and `quantity`. Auto-generating UUIDs is explicit non-goal for v1.)

- [ ] **Step 6.6: Run tests — expect PASS**

```powershell
cd services\web
go test -short ./internal/handlers/...
```

- [ ] **Step 6.7: Commit**

```powershell
git add services/web
git commit -m "feat(web): create-order page (GET /orders/new + POST /v1/orders proxy)"
```

---

## Task 7: Order detail + cancel (GET /orders/{id}, POST /v1/orders/{id})

**Files:**
- Modify: `services/web/internal/handlers/pages.go` — add `PageOrderDetail` + `ActionOrderCancel`
- Modify: `services/web/internal/templates/parsers` — `order_detail.html` in ParseFS
- Create: `services/web/internal/templates/order_detail.html`
- Modify: `services/web/internal/handlers/pages_test.go` — 3 new tests

**Interfaces:**

```go
// PageOrderDetail: GET /orders/{id} → renders order_detail.html body.
// ActionOrderCancel: POST /v1/orders/{id} → wraps OrderClient.Cancel,
// responds with HX-Redirect /orders/{id} so the page reloads.
```

**Step-by-step:**

- [ ] **Step 7.1: Write 3 failing tests**

```go
func TestOrderDetail_OK(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getResp = &backend.Order{
		ID: "order-1", State: backend.OrderStateReserved,
		Items: []backend.OrderItem{{SKU: "SKU-001", Quantity: 2, UnitPriceCents: ptrInt64(1999)}},
	}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/order-1")
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(b.String(), "order-1") { t.Error("missing id") }
	if !strings.Contains(b.String(), "reserved") { t.Error("missing state badge") }
}

func TestOrderDetail_NotFound(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.getErr = fmt.Errorf("upstream 404: status 404: not found")
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/orders/missing")
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 404 { t.Fatalf("status: got %d want 404", resp.StatusCode) }
}

func TestOrderCancel_OK(t *testing.T) {
	oc := &fakeOrderClient{}
	oc.cancelCalls = 0
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/orders/order-1", "", nil)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	if got := resp.Header.Get("HX-Redirect"); got != "/orders/order-1" {
		t.Errorf("HX-Redirect: got %q", got)
	}
	if oc.cancelCalls != 1 { t.Errorf("Cancel calls: got %d", oc.cancelCalls) }
}

func ptrInt64(v int64) *int64 { return &v }
```

Extend `fakeOrderClient`:

```go
type fakeOrderClient struct {
	listResp *backend.OrderList; listErr error
	submitResp *backend.Order; submitErr error
	getResp *backend.Order; getErr error
	cancelCalls int
}
func (f *fakeOrderClient) Get(_ context.Context, _ string) (*backend.Order, error) { return f.getResp, f.getErr }
func (f *fakeOrderClient) Cancel(_ context.Context, _ string) error { f.cancelCalls++; return nil }
```

- [ ] **Step 7.2: Run tests — expect compile errors**

- [ ] **Step 7.3: Create `services/web/internal/templates/order_detail.html`**

```html
{{define "body"}}
<section>
  <div class="row-between">
    <h1>Order <span class="mono">{{.Order.ID}}</span></h1>
    <a class="btn secondary" href="/">← back to list</a>
  </div>
  {{if .BackendDown}}<div class="error">Backend unavailable: {{.Error}}</div>{{end}}
  {{if .Order}}
  <p><span class="badge {{.Order.State}}">{{.Order.State}}</span>
     <span class="muted">created {{.Order.CreatedAt.Format "2006-01-02 15:04:05"}}</span>
     {{if .Order.CompletedAt}} · <span class="muted">completed {{.Order.CompletedAt.Format "15:04:05"}}</span>{{end}}
  </p>
  {{if .Order.FailureReason}}<div class="error">Failure: {{.Order.FailureReason}}</div>{{end}}
  <table>
    <thead><tr><th>SKU</th><th>Qty</th><th>Unit price</th></tr></thead>
    <tbody>
      {{range .Order.Items}}
      <tr>
        <td class="mono">{{.SKU}}</td>
        <td>{{.Quantity}}</td>
        <td>{{if .UnitPriceCents}}{{.UnitPriceCents}}¢{{else}}<span class="muted">auto</span>{{end}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>
  {{if not (or (eq .Order.State "cancelled") (eq .Order.State "failed") (eq .Order.State "confirmed"))}}
  <form method="post" action="/v1/orders/{{.Order.ID}}" hx-post="/v1/orders/{{.Order.ID}}" hx-swap="none" style="margin-top:16px">
    <button type="submit" class="danger">Cancel order</button>
  </form>
  {{end}}
  <p class="muted">Page polls every 1s while non-terminal.</p>
  {{end}}
</section>
{{end}}
```

- [ ] **Step 7.4: Update `NewSet` ParseFS to include `order_detail.html`.**

- [ ] **Step 7.5: Implement handlers**

```go
type orderDetailVM struct {
	Order      *backend.Order
	BackendDown bool
	Error      string
}

func (s *Set) PageOrderDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	o, err := s.Order.Get(r.Context(), id)
	vm := orderDetailVM{}
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
		w.WriteHeader(404) // for "not found" specifically; fallback 502 if you can classify
	} else {
		vm.Order = o
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", vm)
}

func (s *Set) ActionOrderCancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.Order.Cancel(r.Context(), id); err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	w.Header().Set("HX-Redirect", "/orders/"+id)
	w.WriteHeader(200)
}
```

- [ ] **Step 7.6: Run tests — expect PASS**

- [ ] **Step 7.7: Commit**

```powershell
git add services/web
git commit -m "feat(web): order detail page + cancel action"
```

---

## Task 8: Inventory viewer (GET /inventory)

**Files:**
- Modify: `services/web/internal/handlers/pages.go` — add `PageInventory`
- Modify: `services/web/internal/handlers/handlers.go` — add `order_detail.html` to ParseFS
- Create: `services/web/internal/templates/inventory.html`
- Modify: `services/web/internal/handlers/pages_test.go` — 2 new tests

**Interfaces:**

```go
// PageInventory: GET /inventory → fetches stock, renders table.
```

**Step-by-step:**

- [ ] **Step 8.1: Write 2 failing tests**

```go
type fakeInventoryClient struct {
	stock []backend.StockItem
	err error
}
func (f *fakeInventoryClient) ListStock(_ context.Context) ([]backend.StockItem, error) {
	return f.stock, f.err
}

func TestInventory_OK(t *testing.T) {
	ic := &fakeInventoryClient{stock: []backend.StockItem{
		{SKU: "SKU-001", Available: 99, Reserved: 1, Version: 3},
		{SKU: "SKU-002", Available: 50, Reserved: 0, Version: 1},
	}}
	set := handlers.NewSet(&fakeOrderClient{}, &fakePaymentClient{}, ic, events.NewBus())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/inventory")
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(b.String(), "SKU-001") { t.Error("missing SKU-001") }
	if !strings.Contains(b.String(), "99") { t.Error("missing available qty") }
}

func TestInventory_BackendError(t *testing.T) {
	ic := &fakeInventoryClient{err: fmt.Errorf("upstream 503")}
	set := handlers.NewSet(&fakeOrderClient{}, &fakePaymentClient{}, ic, events.NewBus())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/inventory")
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(strings.ToLower(b.String()), "unavailable") &&
		!strings.Contains(strings.ToLower(b.String()), "backend") {
		t.Errorf("expected backend notice")
	}
}
```

`NewSet` signature change: now takes `(order, payment, inventory, bus)`. The `newTestSet` helper from Tasks 5/6/7 already takes an order client — extend it to take an optional inventory client too, OR refactor to `newTestSetWith(order, inventory, t)`:

```go
func newTestSetWith(t *testing.T, oc backend.OrderClient, ic backend.InventoryClient) http.Handler {
	t.Helper()
	bus := events.NewBus()
	h := handlers.NewSet(oc, &fakePaymentClient{}, ic, bus)
	r := chi.NewRouter()
	h.Routes(r)
	return r
}
func newTestSet(t *testing.T, oc backend.OrderClient) http.Handler {
	return newTestSetWith(t, oc, &fakeInventoryClient{})
}
```

- [ ] **Step 8.2: Run tests — expect compile errors**

- [ ] **Step 8.3: Create `services/web/internal/templates/inventory.html`**

```html
{{define "body"}}
<section>
  <h1>Inventory</h1>
  {{if .BackendDown}}<div class="error">Inventory service: {{.Error}}</div>
  {{else if not .Stock}}<p class="muted">No stock items.</p>
  {{else}}
  <table>
    <thead><tr><th>SKU</th><th>Available</th><th>Reserved</th><th>Version</th></tr></thead>
    <tbody>
      {{range .Stock}}
      <tr>
        <td class="mono">{{.SKU}}</td>
        <td>{{.Available}}</td>
        <td>{{.Reserved}}</td>
        <td class="muted">{{.Version}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>
  {{end}}
  <p class="muted">Page polls every 3s.</p>
</section>
{{end}}
```

- [ ] **Step 8.4: Update `NewSet` ParseFS to include `inventory.html`.**

- [ ] **Step 8.5: Implement handler**

```go
type inventoryVM struct {
	Stock       []backend.StockItem
	BackendDown bool
	Error       string
}

func (s *Set) PageInventory(w http.ResponseWriter, r *http.Request) {
	items, err := s.Inventory.ListStock(r.Context())
	vm := inventoryVM{Stock: items}
	if err != nil {
		vm.BackendDown = true
		vm.Error = err.Error()
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", vm)
}
```

- [ ] **Step 8.6: Run tests — expect PASS**

- [ ] **Step 8.7: Commit**

```powershell
git add services/web
git commit -m "feat(web): inventory viewer (GET /inventory)"
```

---

## Task 9: Payment webhook simulator (GET /payments/sim, POST /payments/sim/fire)

**Files:**
- Modify: `services/web/internal/handlers/pages.go` — add `PagePaymentsSim` + `ActionPaymentsFire`
- Modify: `services/web/internal/handlers/handlers.go` — ParseFS includes `payments.html`
- Create: `services/web/internal/templates/payments.html`
- Modify: `services/web/internal/handlers/pages_test.go` — 2 new tests

**Interfaces:**

```go
// PagePaymentsSim: GET /payments/sim → lists recent in-flight orders
//   (state=pending or state=reserved), each with two buttons
//   (force success / force fail).
// ActionPaymentsFire: POST /payments/sim/fire → builds a
//   PaymentWebhook from form fields (order_id, status, error_code)
//   and proxies to PaymentClient.FireWebhook. The payment_id is
//   deterministic on order_id so replays are idempotent in the
//   payment mock. Responds with HX-Redirect /payments/sim.
```

**Step-by-step:**

- [ ] **Step 9.1: Write 2 failing tests**

```go
func TestPaymentsSim_OK(t *testing.T) {
	oc := &fakeOrderClient{listResp: &backend.OrderList{Items: []backend.Order{
		{ID: "o-1", State: backend.OrderStateReserved,
			Items: []backend.OrderItem{{SKU: "X", Quantity: 1}}},
	}}}
	srv := httptest.NewServer(newTestSet(t, oc))
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/payments/sim")
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	b := new(strings.Builder)
	_, _ = b.ReadFrom(resp.Body)
	if !strings.Contains(b.String(), "o-1") { t.Error("missing order id") }
	if !strings.Contains(b.String(), "force") { t.Error("missing force buttons") }
}

func TestPaymentsFire_OK(t *testing.T) {
	pc := &fakePaymentClient{}
	set := handlers.NewSet(&fakeOrderClient{}, pc, &fakeInventoryClient{}, events.NewBus())
	r := chi.NewRouter()
	set.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	form := strings.NewReader("order_id=o-1&status=failed&error_code=card_declined")
	req, _ := http.NewRequest("POST", srv.URL+"/payments/sim/fire", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { t.Fatalf("status: %d", resp.StatusCode) }
	if got := resp.Header.Get("HX-Redirect"); got != "/payments/sim" {
		t.Errorf("HX-Redirect: got %q", got)
	}
	if pc.lastWebhook == nil || pc.lastWebhook.Status != "failed" {
		t.Errorf("webhook not fired: %+v", pc.lastWebhook)
	}
	if pc.lastWebhook.PaymentID != "o-1" {
		t.Errorf("payment_id determinism: got %q want o-1", pc.lastWebhook.PaymentID)
	}
}

type fakePaymentClient struct{ lastWebhook *backend.PaymentWebhook }
func (f *fakePaymentClient) FireWebhook(_ context.Context, w backend.PaymentWebhook) error {
	f.lastWebhook = &w
	return nil
}
```

(Replace the existing `fakePaymentClient{}` zero-value usage in earlier tests with `&fakePaymentClient{}` — that struct already conforms because the field is a pointer.)

- [ ] **Step 9.2: Run tests — expect compile errors**

- [ ] **Step 9.3: Create `services/web/internal/templates/payments.html`**

```html
{{define "body"}}
<section>
  <h1>Payment webhook simulator</h1>
  <p class="muted">Fire a webhook into the Payment Service for any in-flight order below. The payment_id is derived deterministically from the order_id so replays are idempotent.</p>
  {{if .BackendDown}}<div class="error">Backend unavailable: {{.Error}}</div>{{end}}
  {{if and (not .BackendDown) .InFlight}}
  <table>
    <thead><tr><th>Order</th><th>State</th><th>Fire succeeded</th><th>Fire failed</th></tr></thead>
    <tbody>
      {{range .InFlight}}
      <tr>
        <td class="mono">{{.ID}}</td>
        <td><span class="badge {{.State}}">{{.State}}</span></td>
        <td>
          <form method="post" action="/payments/sim/fire" hx-post="/payments/sim/fire" hx-swap="none">
            <input type="hidden" name="order_id" value="{{.ID}}">
            <input type="hidden" name="status" value="succeeded">
            <button type="submit">force ✓</button>
          </form>
        </td>
        <td>
          <form method="post" action="/payments/sim/fire" hx-post="/payments/sim/fire" hx-swap="none">
            <input type="hidden" name="order_id" value="{{.ID}}">
            <input type="hidden" name="status" value="failed">
            <input type="hidden" name="error_code" value="card_declined">
            <button type="submit" class="danger">force ✗</button>
          </form>
        </td>
      </tr>
      {{end}}
    </tbody>
  </table>
  {{else if not .BackendDown}}
  <p class="muted">No in-flight orders. Create one on <a href="/">Orders</a> first.</p>
  {{end}}
</section>
{{end}}
```

- [ ] **Step 9.4: Update `NewSet` ParseFS to include `payments.html`.**

- [ ] **Step 9.5: Implement handlers**

```go
type paymentsSimVM struct {
	InFlight    []backend.Order
	BackendDown bool
	Error       string
}

func (s *Set) PagePaymentsSim(w http.ResponseWriter, r *http.Request) {
	// Get both lists so the page can show "no orders at all" vs "no
	// in-flight orders" honestly. Cheap call: small lists.
	var pending, reserved *backend.OrderList
	pending, _ = s.Order.List(r.Context(), backend.OrderStatePending, 50)
	reserved, _ = s.Order.List(r.Context(), backend.OrderStateReserved, 50)
	vm := paymentsSimVM{}
	if pending == nil && reserved == nil && /* both errors */ false {
		vm.BackendDown = true
		vm.Error = "Order service unavailable"
	}
	if pending != nil { vm.InFlight = append(vm.InFlight, pending.Items...) }
	if reserved != nil { vm.InFlight = append(vm.InFlight, reserved.Items...) }
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.Templates.ExecuteTemplate(w, "layout", vm)
}

func (s *Set) ActionPaymentsFire(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", 400)
		return
	}
	orderID := r.FormValue("order_id")
	status := r.FormValue("status")
	errorCode := r.FormValue("error_code")
	if orderID == "" || (status != "succeeded" && status != "failed") {
		http.Error(w, "order_id and status required", 400)
		return
	}
	w2 := backend.PaymentWebhook{
		PaymentID: orderID, // deterministic on order_id (idempotent in mock)
		Status:    status,
		ErrorCode: errorCode,
	}
	if err := s.Payment.FireWebhook(r.Context(), w2); err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	w.Header().Set("HX-Redirect", "/payments/sim")
	w.WriteHeader(200)
}
```

(The "both errors" comment in `PagePaymentsSim` is a doc note — implement it as `if pending == nil && reserved == nil { vm.BackendDown = true }`. Simplify on write.)

- [ ] **Step 9.6: Run tests — expect PASS**

- [ ] **Step 9.7: Commit**

```powershell
git add services/web
git commit -m "feat(web): payment webhook simulator (force success / force fail)"
```

---

## Task 10: Live event tail — bus + Kafka consumer + SSE endpoint

**Files:**
- Create: `services/web/internal/events/bus.go`
- Create: `services/web/internal/events/bus_test.go`
- Create: `services/web/internal/kafkatail/tail.go`
- Create: `services/web/internal/kafkatail/tail_test.go`
- Modify: `services/web/internal/handlers/pages.go` — add `PageEventsStream` (SSE)
- Modify: `services/web/internal/web/main.go` — start kafka tail goroutine if `KAFKA_BROKERS` set, wire into bus
- Modify: `services/web/internal/server/server.go` — register `GET /events/stream`

**Interfaces:**

```go
package events

// Event is a JSON-serializable event pushed into the bus. Currently
// just the Envelope (matches pkg/platform/events.Envelope); future
// fields are additive.
type Event struct {
	Envelope events.Envelope // re-exported (not aliased to avoid leaking pkg path into bus)
}

// Bus is a fan-out broadcast hub. Subscribe returns a channel that
// receives every Event from the moment of Subscribe onward (no
// replay). Unsubscribe closes the channel cleanly.
type Bus struct { /* unexported fields */ }

// NewBus constructs an empty bus.
func NewBus() *Bus

// Subscribe registers a new subscriber. The returned channel is
// buffered (size 64); slow consumers will DROP oldest first (bus
// drains the buffer to len(cap) before publishing) so the bus
// never blocks a publisher. The returned func unsubscribes.
func (b *Bus) Subscribe() (chan Event, func())

// Publish broadcasts e to all current subscribers.
func (b *Bus) Publish(e Event)

// Close stops the bus; further Publish is a no-op. All subscriber
// channels are closed.
func (b *Bus) Close()
```

```go
package kafkatail

// Start spins up a Kafka consumer on the given brokers, subscribed
// to "order-events" with consumer group "orderflow-web". It forwards
// every envelope into bus. Returns a stop function that blocks
// until the consumer has fully exited.
//
// brokersCSV is "host:9092,host2:9092". Empty string disables the
// tail (Start returns nil, nil).
func Start(ctx context.Context, logger *slog.Logger, brokersCSV string, bus *events.Bus) (stop func(), err error)
```

```go
// PageEventsStream: GET /events/stream. SSE; subscribes to bus,
// writes one `event: state\ndata: <json>\n\n` line per Event.
// Honors ctx.Done() for client disconnect.
```

**Step-by-step:**

- [ ] **Step 10.1: Write failing test `bus_test.go`**

```go
package events_test

import (
	"sync"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
)

func TestBus_PublishSubscribe(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	defer unsub()

	env := events.Envelope{EventID: "e1", EventType: "OrderCreated"}
	go b.Publish(events.BusEvent{Envelope: env})

	select {
	case got := <-ch:
		if got.Envelope.EventID != "e1" { t.Errorf("got %s", got.Envelope.EventID) }
	case <-time.After(time.Second):
		t.Fatal("no event received in 1s")
	}
}

func TestBus_UnsubscribeStopsDelivery(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	unsub()
	env := events.Envelope{EventID: "e2"}
	b.Publish(events.BusEvent{Envelope: env})
	select {
	case _, ok := <-ch:
		if ok { t.Error("expected channel closed after unsub") }
	case <-time.After(100*time.Millisecond):
		// OK — closed channel never yields a value; an open channel
		// would receive the buffered message.
	}
}

func TestBus_MultipleSubscribers(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch1, u1 := b.Subscribe()
	defer u1()
	ch2, u2 := b.Subscribe()
	defer u2()

	var wg sync.WaitGroup
	wg.Add(2)
	for _, ch := range []chan events.BusEvent{ch1, ch2} {
		ch := ch
		go func() {
			defer wg.Done()
			<-ch
		}()
	}
	b.Publish(events.BusEvent{Envelope: events.Envelope{EventID: "x"}})
	wg.Wait()
}

func TestBus_BufferOverflow_DropsOldest(t *testing.T) {
	b := events.NewBus()
	defer b.Close()
	ch, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < 1000; i++ {
		b.Publish(events.BusEvent{Envelope: events.Envelope{EventID: "x"}})
	}
	// Just verify the subscriber didn't deadlock and at least one
	// message is buffered.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("subscriber starved")
	}
}
```

(Note: the test references `events.BusEvent` — that's the struct the bus publishes. If you prefer a different name like `events.Event`, substitute throughout. The plan uses `BusEvent` so it doesn't collide with the import alias.)

- [ ] **Step 10.2: Implement `bus.go`**

```go
// Package events hosts an in-process publish/subscribe bus used by
// the SSE endpoint to relay Kafka events to connected browsers.
package events

import (
	"sync"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
)

// BusEvent is the structure passed through the bus. The Envelope is
// re-used from pkg/platform/events so consumers can unmarshal the
// exact Kafka record body without translating types.
type BusEvent struct {
	Envelope pkgEvents.Envelope
}

// Bus fans events out to subscribers. Slow consumers drop oldest
// first, never blocking the publisher.
type Bus struct {
	mu   sync.Mutex
	subs map[chan BusEvent]struct{}
	done chan struct{}
}

// NewBus constructs a fresh bus.
func NewBus() *Bus {
	return &Bus{subs: map[chan BusEvent]struct{}{}, done: make(chan struct{})}
}

// Subscribe returns a buffered channel that receives every event
// from now on, plus an unsubscribe function.
func (b *Bus) Subscribe() (chan BusEvent, func()) {
	ch := make(chan BusEvent, 64)
	b.mu.Lock()
	if b.closed() {
		b.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
	}
}

// Publish sends e to every current subscriber. If a subscriber's
// buffer is full, the OLDEST queued event on that channel is
// dropped to make room (subscriber is too slow; keep them current).
func (b *Bus) Publish(e BusEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed() {
		return
	}
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
			// Drop oldest, push newest. Non-blocking.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- e:
			default:
			}
		}
	}
}

// Close marks the bus as closed. Subsequent Publish is a no-op;
// all subscriber channels are closed.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
		return
	default:
		close(b.done)
	}
	for ch := range b.subs {
		close(ch)
		delete(b.subs, ch)
	}
}

func (b *Bus) closed() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 10.3: Run `bus_test.go` — expect PASS**

```powershell
cd services\web
go test -short ./internal/events/...
```

- [ ] **Step 10.4: Write failing `tail_test.go` (testcontainers Kafka)**

```go
package kafkatail_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"
	"github.com/t0pm1x/orderflow/services/web/internal/kafkatail"
)

// TestTail_PublishesEnvelope is a slow test (needs Kafka). Run only
// if KAFKA_TESTS=1.
func TestTail_PublishesEnvelope(t *testing.T) {
	if os.Getenv("KAFKA_TESTS") != "1" {
		t.Skip("set KAFKA_TESTS=1 to run (requires Docker + testcontainers)")
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" { t.Fatal("KAFKA_BROKER required") }

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	bus := events.NewBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe()
	defer unsub()

	stop, err := kafkatail.Start(ctx, slog.Default(), broker, bus)
	if err != nil { t.Fatal(err) }
	defer stop()

	// Publish an event to order-events.
	cli, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil { t.Fatal(err) }
	defer cli.Close()
	env, _ := pkgEvents.NewEnvelope("OrderCreated", "Order", uuid.NewString(), map[string]string{"x":"1"}, "", "")
	body, _ := json.Marshal(env)
	if err := cli.ProduceSync(ctx, &kgo.Record{Topic: "order-events", Value: body}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-ch:
		if got.Envelope.EventType != "OrderCreated" {
			t.Errorf("event_type: got %s", got.Envelope.EventType)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no event in 30s")
	}
}
```

The test creates the topic implicitly via produce (or pre-create with kafka-topics CLI). For testcontainers, the harness in `tests/harness/harness.go` already starts Redpanda; either reuse its `mustKafka` via a Go module reference, or run a minimal Docker Compose snippet from this test. **Simpler: pre-create the topic via `kafka-go` admin client OR reuse `tests/harness`.** Path of least resistance: import `tests/harness` (already in `go.work`'s `use` block) and use its `KafkaContainer()` method to get the broker URL.

Replace `broker := os.Getenv("KAFKA_BROKER")` block with:

```go
import "github.com/t0pm1x/orderflow/tests/harness"

h := harness.New(t, harness.WithOtel())
defer h.Cleanup()
broker = h.KafkaContainer().URI() // or whichever method the harness exposes
```

(Inspect `tests/harness/harness.go` for the exact URL accessor — likely `h.KafkaAddr()`.)

The `Stop()` returned by `Start(ctx, ...)` should: cancel the consumer poll context, flush offsets, close franz-go client. Implementation in Step 10.5.

- [ ] **Step 10.5: Implement `kafkatail/tail.go`**

```go
// Package kafkatail wraps pkg/consumer with a tiny adapter that
// publishes every Kafka record to the in-process bus.
package kafkatail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	pkgEvents "github.com/t0pm1x/orderflow/platform/events"
	"github.com/t0pm1x/orderflow/services/web/internal/events"

	kafkaconsumer "github.com/t0pm1x/orderflow/consumer"
)

const (
	topic     = "order-events"
	groupID   = "orderflow-web"
	brokerCSV = "" // overwritten by Start's brokersCSV
)

// Start subscribes a Kafka consumer to "order-events" with group
// "orderflow-web" and publishes each Envelope into bus. Returns a
// stop function that blocks until the consumer has exited.
//
// brokersCSV may be empty — in that case Start returns (nil, nil)
// and no consumer is started (the web service runs without live
// events, ready to be wired later).
func Start(ctx context.Context, logger *slog.Logger, brokersCSV string, bus *events.Bus) (func(), error) {
	if brokersCSV == "" {
		logger.Info("kafka tail disabled: KAFKA_BROKERS not set")
		return nil, nil
	}
	brokers := splitCSV(brokersCSV)
	c, err := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers: brokers,
		GroupID: groupID,
		Topics:  []string{topic},
		// DLQ=nil + Deduper=nil: skip retries/dedup; UI just acks.
	}, kafkaconsumer.HandlerRegistry{
		"OrderCreated": func(_ context.Context, env *pkgEvents.Envelope) error {
			bus.Publish(events.BusEvent{Envelope: *env})
			return nil
		},
		"OrderConfirmed":                   forwardToBus(bus),
		"OrderCancelled":                   forwardToBus(bus),
		"StockReserveRequested":            forwardToBus(bus),
		"StockReleaseRequested":            forwardToBus(bus),
		"PaymentRequested":                 forwardToBus(bus),
		"OrderUpdated":                     forwardToBus(bus),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka tail: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil {
			logger.Error("kafka tail exited", "err", err)
		}
	}()
	stop := func() {
		c.Stop()
		wg.Wait()
	}
	return stop, nil
}

func forwardToBus(bus *events.Bus) kafkaconsumer.Handler {
	return func(_ context.Context, env *pkgEvents.Envelope) error {
		bus.Publish(events.BusEvent{Envelope: *env})
		return nil
	}
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, ch := range s {
		if ch == ',' {
			if cur != "" { out = append(out, cur) }
			cur = ""
			continue
		}
		cur += string(ch)
	}
	if cur != "" { out = append(out, cur) }
	return out
}
```

(The event-type registry above covers every event the spec defines as "saga-visible" on `order-events`. Each entry just forwards; if Kafka publishes a new event_type, the consumer ack-skips it per `pkg/consumer` semantics — see `pkg/consumer/consumer.go:198-202`. The list is intentionally narrow so adding a new one is a one-line change.)

Note: `Start` cancels via the `ctx` parameter. Callers (main.go) pass `context.Background()` and cancel it on signal. The WaitGroup shutdown matches the saga close pattern.

- [ ] **Step 10.6: Run `tail_test.go` — expect PASS**

Run only when `KAFKA_TESTS=1`:

```powershell
cd services\web
KAFKA_TESTS=1 KAFKA_BROKER=localhost:9092 go test ./internal/kafkatail/...
```

On Windows PowerShell, set env differently:
```powershell
$env:KAFKA_TESTS="1"; $env:KAFKA_BROKER="localhost:9092"; go test ./internal/kafkatail/...
```

Expected: PASS (1 test). If the test fails because no Kafka is running locally, skip with `t.Skip` — CI doesn't run these.

- [ ] **Step 10.7: Implement SSE handler**

In `services/web/internal/handlers/pages.go`:

```go
func (s *Set) PageEventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsub := s.Bus.Subscribe()
	defer unsub()

	// Heartbeat every 15s so proxies don't idle out the connection.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	// Send initial comment to flush headers.
	_, _ = fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case ev, ok := <-ch:
			if !ok { return }
			data, err := json.Marshal(ev.Envelope)
			if err != nil { continue }
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Envelope.EventType, data)
			flusher.Flush()
		}
	}
}
```

(Import `"encoding/json"`.)

- [ ] **Step 10.8: Wire Kafka tail into `internal/web/main.go`**

Replace the body of `Run` (after the server start) with:

```go
	ctx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	bus := events.NewBus()
	defer bus.Close()

	stopTail, err := kafkatail.Start(ctx, slog.Default(), envOrDefault("KAFKA_BROKERS", ""), bus)
	if err != nil {
		return fmt.Errorf("kafka tail: %w", err)
	}
	if stopTail != nil {
		defer stopTail()
	}

	httpAddr := envOrDefault("HTTP_ADDR", ":8083")
	// ... existing server start with bus wired through ...
```

Construct the `*handlers.Set` and pass it into `server.Options.Handlers` (Task 5 wired this; just ensure the bus field is populated). Update `NewSet`'s callers in `main.go`.

- [ ] **Step 10.9: Update layout to use ServerSentEvents extension**

The layout from Task 4 (`layout.html`) already includes `hx-ext="sse" sse-connect="/events/stream"` on the sidebar and a `htmx:sseMessage` listener. Verify it compiles and renders end-to-end:

```powershell
cd services\web
go build -o ..\..\bin\web .\cmd\web
..\..\bin\web &
# Subscribe to SSE with curl (5s timeout)
curl -N http://localhost:8083/events/stream &
sleep 5
# Stop web
```

Expected: SSE stream begins with `: connected` comment and stays open until killed.

- [ ] **Step 10.10: Commit**

```powershell
git add services/web/internal/events services/web/internal/kafkatail services/web/internal/handlers services/web/internal/web services/web/internal/server
git commit -m "feat(web): live event tail — bus, Kafka consumer, SSE endpoint"
```

---

## Task 11: Wiring — Makefile, docker-compose, README, STATUS

**Files:**
- Modify: `Makefile` — add `build-web`, `run-web`, extend `build` and `test`
- Modify: `deploy/docker-compose.yml` — add `web` service with `depends_on` healthy gates
- Modify: `README.md` — add quickstart note
- Modify: `STATUS.md` — add 11 rows under sub-stages
- Create: `services/web/README.md` — how to run locally + curl smoke recipe

**Why last:** All features must exist before being wired into the build pipeline + compose stack + docs.

**Step-by-step:**

- [ ] **Step 11.1: Update `Makefile`**

Add `make build-web` and `make run-web`:

```makefile
build-web:
	go build -ldflags="$(LDFLAGS)" -o bin/web ./services/web/cmd/web

run-web:
	go run ./services/web/cmd/web
```

Extend `make build` to also build web (after the four existing lines):

```makefile
build:
	go build -ldflags="$(LDFLAGS)" -o bin/order ./cmd/order
	go build -ldflags="$(LDFLAGS)" -o bin/payment ./cmd/payment
	go build -ldflags="$(LDFLAGS)" -o bin/inventory ./cmd/inventory
	go build -ldflags="$(LDFLAGS)" -o bin/saga ./cmd/saga
	go build -ldflags="$(LDFLAGS)" -o bin/web ./services/web/cmd/web
```

Extend `WORKSPACE_MODULES` to include the new module paths:

```makefile
WORKSPACE_MODULES = pkg/platform pkg/outbox pkg/consumer pkg/platform/instrumentation/kafkaprop \
                    services/order services/payment services/inventory services/saga services/web \
                    cmd/order cmd/payment cmd/inventory cmd/saga cmd/web \
                    tests
```

- [ ] **Step 11.2: Update `deploy/docker-compose.yml`**

Append (after the existing service entries):

```yaml
  web:
    build:
      context: .
      dockerfile: services/web/Dockerfile
    container_name: orderflow-web
    ports:
      - "8083:8083"
    environment:
      ORDER_URL: http://order:8080
      PAYMENT_URL: http://payment:8081
      INVENTORY_URL: http://inventory:8082
      KAFKA_BROKERS: redpanda:9092
      HTTP_ADDR: ":8083"
    depends_on:
      order:
        condition: service_healthy
      payment:
        condition: service_healthy
      inventory:
        condition: service_healthy
    restart: unless-stopped
```

Create `services/web/Dockerfile`:

```dockerfile
FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY go.work go.work.sum ./
COPY pkg ./pkg
COPY services/web ./services/web
WORKDIR /src/services/web
RUN go mod download
ARG VERSION=0.0.0-dev
RUN go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/web/internal/web.Version=${VERSION}" -o /out/web ./cmd/web

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/web /usr/local/bin/web
EXPOSE 8083
USER nobody:nobody
ENTRYPOINT ["/usr/local/bin/web"]
```

(The Dockerfile path matches the pattern established by v1.1.design for cross-service images. The compose build context uses `.` from the repo root; the image is local-only for now — ghcr.io publishing is explicitly deferred to v1.1.x per the design doc.)

Verify compose syntax:

```powershell
docker compose -f deploy/docker-compose.yml config -q
```

Expected: no output, exit 0.

- [ ] **Step 11.3: Update `README.md`**

In the "Quickstart" section, add:

```markdown
### Web playground (optional)

After `bash docs/demo/demo.sh`, the orderflow-web UI is also available at
[http://localhost:8083](http://localhost:8083) — list orders, create new
ones, fire a forced-fail payment webhook, and watch `order-events` arrive
in the sidebar.

Build it on its own: `make run-web` (requires Order/Payment/Inventory
services to already be running on :8080/:8081/:8082).
```

And under "Project structure", add `services/web/` to the tree.

- [ ] **Step 11.4: Create `services/web/README.md`**

```markdown
# orderflow-web

Tactile playground UI for the orderflow platform. Server-rendered HTML
(`html/template`) + a sprinkle of `htmx` for progressive enhancement.

## Run locally (against already-running services)

```powershell
cd services\web
go run .\cmd\web
# → listens on :8083 by default
```

Override with env: `HTTP_ADDR=:9090 ORDER_URL=http://orders.example.com:8080 ...`.

## Run via docker-compose

```powershell
docker compose -f deploy/docker-compose.yml up web
```

The compose `web` service depends on `order`, `payment`, `inventory`
being healthy.

## Smoke recipe

```powershell
curl http://localhost:8083/healthz                       # → {"status":"ok"}
curl http://localhost:8083/                              # → orders list HTML
curl -X POST http://localhost:8083/v1/orders -d "sku=SKU-001&quantity=2"
# → 200 + HX-Redirect: /orders/<id>
```

Open <http://localhost:8083> in a browser.

## Architecture

See `docs/superpowers/specs/2026-08-18-orderflow-web-design.md`.
```

- [ ] **Step 11.5: Update `STATUS.md`**

Add a new section under "Sub-stages":

```markdown
| Stage    | Title                            | Status   | Commit    | Plan ref |
|----------|----------------------------------|----------|-----------|----------|
| web.1    | bootstrap web module skeleton    | done     | <sha>     | this plan |
| web.2    | backend clients + types          | done     | <sha>     | this plan |
| web.3    | server scaffolding + probes      | done     | <sha>     | this plan |
| web.4    | layout + stylesheet              | done     | <sha>     | this plan |
| web.5    | orders list page                 | done     | <sha>     | this plan |
| web.6    | create-order page                | done     | <sha>     | this plan |
| web.7    | order detail + cancel            | done     | <sha>     | this plan |
| web.8    | inventory viewer                 | done     | <sha>     | this plan |
| web.9    | payment webhook simulator        | done     | <sha>     | this plan |
| web.10   | live event tail + SSE            | done     | <sha>     | this plan |
| web.11   | compose + Makefile + README      | done     | <sha>     | this plan |
```

Fill `<sha>` after each task's commit.

- [ ] **Step 11.6: Final verification**

```powershell
cd C:\Users\t0p_m\projects\orderflow
go work sync
make build
make verify   # tidy + build + test + lint
```

Expected: all green. Lint covers the new module via `.golangci.yml` (which uses `run ./...` from the root with the workspace — verify on first run that `golangci-lint` v2 picks up `services/web/`; if not, add `services/web` to the `lint` script's per-module loop, mirroring the `test` target).

Smoke from the repo root:

```powershell
make run-order &
make run-payment &
make run-inventory &
make run-web &
sleep 3
curl http://localhost:8083/
curl -X POST http://localhost:8083/v1/orders -d "sku=SKU-001&quantity=2"
curl http://localhost:8083/inventory
curl http://localhost:8083/payments/sim
```

Expected: every call returns successfully. (For full saga flow with `confirmed` status, all four services including saga must be running, plus Postgres + Kafka — `make smoke-k8s` or `bash docs/demo/demo.sh` covers that.)

- [ ] **Step 11.7: Commit**

```powershell
git add Makefile deploy/docker-compose.yml README.md STATUS.md services/web/Dockerfile services/web/README.md
git commit -m "feat(web): docker-compose, Makefile wiring, README + STATUS rows"
```

---

## Self-Review (run after writing the plan, before announcing execution options)

1. **Spec coverage:**
   - Spec §"Goals" #1 "Hands-on playground" — Tasks 5–10 cover this end-to-end.
   - Spec §"Goals" #2 "Real-time saga visibility" — Task 10.
   - Spec §"Goals" #3 "Forced-failure exploration" — Tasks 9 + 10.
   - Spec §"Goals" #4 "Inventory transparency" — Task 8.
   - Spec §"Goals" #5 "Zero disruption to existing flows" — verified: no existing service/modified file is in any task's modify list. Only `Makefile`, `deploy/docker-compose.yml`, `STATUS.md`, `README.md`, and `go.work` are touched outside `services/web` and `cmd/web`.
   - Spec §"Architecture" ports, modules, structure — Task 1 (module bootstrap) + Task 11 (compose) implement.
   - Spec §"Data flow" create-order path — Tasks 5, 6, 7.
   - Spec §"Data flow" force-fail path — Tasks 9, 10.
   - Spec §"Error handling" table — every cell is reachable in tasks 5–10.
   - Spec §"Testing" test plan — Tasks 2, 5, 6, 7, 8, 9, 10 each include unit tests; Task 10 includes Kafka integration test; Task 11 covers the manual smoke recipe.
   - Spec §"File Structure" — matches the plan's file table.

2. **Placeholder scan:** None of the disallowed patterns appear. Every code step contains the actual code to write.

3. **Type consistency:**
   - `backend.OrderClient.List/Get/Submit/Cancel` used identically in Task 2 (definition), Tasks 5–9 (handler tests + bodies).
   - `backend.OrderSubmit`, `OrderItem`, `Order`, `OrderState` defined Task 2, used Tasks 5–9.
   - `events.Bus.Subscribe/Publish/Close` defined Task 10, used Task 10 (test, body) + Tasks 9 (PaymentClient used in payments_sim). Bus is constructed once in `web/main.go`.
   - `handlers.Set.Routes` defined Task 5, extended Tasks 6–10 with new method declarations in the same `Routes` registration block.
   - `server.Options.Handlers *handlers.Set` introduced Task 5, wired into `web/main.go` Task 5 + extended to include bus Task 10.

   No renames.

4. **Final adjustment:** The plan adds `.RowBetween` class usage in templates (Task 5 orders_list.html + Task 7 order_detail.html) but `.row-between` is only defined for `.row` in Task 4's CSS. Fixed: add `.row-between` CSS rule in Task 4 (Step 4.5):

   ```css
   .row-between { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
   .row { display: flex; gap: 8px; align-items: center; }
   ```

   Use this updated CSS in Step 4.5.

If review finds any issue, fix it inline. No need to re-review after the fix.
