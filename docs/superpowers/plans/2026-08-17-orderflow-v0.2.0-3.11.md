# Stage 3.11 — E2E / Chaos / Load Tests

**Why:** `tests/{e2e,chaos,load,k8s}/` are empty (only `.gitkeep`). testcontainers is not in any go.mod. We need black-box coverage of the happy path, compensation, chaos (broker failures), and a load profile before claiming v0.2.0.

**Depends on:** Stage 3.10 done (trace IDs in envelopes, chi middleware, OTLP defaults). 3.11.a creates the shared test harness the other tasks reuse.

### Task 3.11.a — testcontainers harness + shared `tests/` module (SEQ)

**Files:**
- Create: `tests/go.mod` (new module `github.com/t0pm1x/orderflow/tests`)
- Create: `tests/harness/harness.go` (start 3 postgres + redis + redpanda + otel-collector; wait for ready; teardown)
- Create: `tests/harness/harness_test.go` (self-test that asserts all containers come up healthy in <60s)
- Create: `tests/testdata/seed/seed.sql` (fixtures: SKUs, customers)
- Create: `examples/order.json` (sample POST /v1/orders body)

**Interfaces:**
- `harness.New(t *testing.T, opts ...Option) *Harness` returns struct with `OrderURL`, `PaymentURL`, `InventoryURL`, `KafkaBrokers []string`, `KafkaTopics map[string]bool`, `PostgresURLs map[string]string`, `RedisURL string`, `OtelEndpoint string`.
- `h.Cleanup()` runs in `t.Cleanup` automatically.
- `h.WaitForOrderState(id string, want domain.OrderState, timeout time.Duration)` polls Order REST.

- [ ] **Step 1: New module**

Create `tests/go.mod`:
```go
module github.com/t0pm1x/orderflow/tests

go 1.25.13

require (
    github.com/testcontainers/testcontainers-go v0.31.1
    github.com/testcontainers/testcontainers-go/modules/postgres v0.31.1
    github.com/testcontainers/testcontainers-go/modules/redis v0.31.1
    github.com/testcontainers/testcontainers-go/modules/kafka v0.31.1
    github.com/twmb/franz-go v1.17.1
    github.com/jackc/pgx/v5 v5.7.1
)
```
Run from repo root:
```powershell
cd C:\Users\t0p_m\projects\orderflow
go work use ./tests
go work sync
```

- [ ] **Step 2: Harness skeleton**

`tests/harness/harness.go`:
```go
package harness

import (
    "context"
    "testing"
    "time"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
    "github.com/testcontainers/testcontainers-go/modules/redis"
    "github.com/testcontainers/testcontainers-go/modules/kafka"
)

type Harness struct {
    OrderURL    string
    PaymentURL  string
    InventoryURL string
    KafkaBrokers []string
    PostgresURLs map[string]string
    RedisURL     string
    OtelEndpoint string
}

type Option func(*config)

type config struct {
    withOtel bool
}

func WithOtel() Option { return func(c *config) { c.withOtel = true } }

func New(t *testing.T, opts ...Option) *Harness {
    t.Helper()
    ctx := context.Background()
    cfg := &config{}
    for _, o := range opts { o(cfg) }

    pgOrder := mustPostgres(ctx, t, "order", "order_order")
    pgPay   := mustPostgres(ctx, t, "payment", "payment_payment")
    pgInv   := mustPostgres(ctx, t, "inventory", "inventory_inventory")
    rd      := mustRedis(ctx, t)
    kf      := mustKafka(ctx, t)
    brokers := []string{kf.Brokers[0]}

    if cfg.withOtel {
        // start otel-collector container, expose 4317
    }

    h := &Harness{
        OrderURL:     pgOrder.URL, // service binaries not started here; callers boot them with these URLs
        PaymentURL:   pgPay.URL,
        InventoryURL: pgInv.URL,
        KafkaBrokers: brokers,
        PostgresURLs: map[string]string{"order": pgOrder.URL, "payment": pgPay.URL, "inventory": pgInv.URL},
        RedisURL:     rd.URL,
    }
    t.Cleanup(func() {
        _ = pgOrder.Terminate(ctx)
        _ = pgPay.Terminate(ctx)
        _ = pgInv.Terminate(ctx)
        _ = rd.Terminate(ctx)
        _ = kf.Terminate(ctx)
    })
    return h
}
```
Helpers `mustPostgres/mustRedis/mustKafka` use `tcpostgres.Run(...)` with image `postgres:16-alpine` and `WaitStrategy: wait.ForLog("database system is ready")`. Run migrations with `pgxpool` after container start using existing `services/<svc>/migrations/*.sql`.

- [ ] **Step 3: Self-test**

`tests/harness/harness_test.go`:
```go
func TestHarness_StartsAllContainers(t *testing.T) {
    if testing.Short() { t.Skip("harness requires docker") }
    h := New(t)
    if len(h.KafkaBrokers) == 0 { t.Fatal("no kafka brokers") }
    if len(h.PostgresURLs) != 3 { t.Fatalf("want 3 pg URLs, got %d", len(h.PostgresURLs)) }
}
```
Run:
```powershell
cd C:\Users\t0p_m\projects\orderflow\tests
go test ./harness/... -v
```
Expected: PASS (docker is available; test will pull images on first run).

- [ ] **Step 4: Sample request body**

`examples/order.json`:
```json
{
  "customer_id": "8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f",
  "items": [
    {"sku": "SKU-001", "quantity": 2, "unit_price_cents": 1999}
  ]
}
```

- [ ] **Step 5: Commit**

```powershell
git add tests examples/order.json go.work go.work.sum
git commit -m "orderflow/3.11.a: testcontainers harness for E2E/chaos/load"
```

### Task 3.11.b — E2E happy path (PAR with 3.11.c, 3.11.d, 3.11.e)

**Files:**
- Create: `tests/e2e/happy_test.go`

**Interfaces:**
- Reuses `harness.Harness`. Boots order/payment/inventory binaries via `exec.Command` with env `DATABASE_URL=...`, `KAFKA_BROKER=...`, `REDIS_URL=...`.
- Drives POST /v1/orders, polls order state until `confirmed` or timeout 30s.

- [ ] **Step 1: Service booter helper**

Add to `tests/harness/harness.go`:
```go
func (h *Harness) StartService(t *testing.T, name, binPath string, env map[string]string) (stop func()) {
    t.Helper()
    cmd := exec.Command(binPath)
    cmd.Env = os.Environ()
    for k, v := range env { cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v)) }
    if err := cmd.Start(); err != nil { t.Fatal(err) }
    return func() { _ = cmd.Process.Signal(syscall.SIGTERM); _, _ = cmd.Process.Wait() }
}
```

- [ ] **Step 2: Failing test**

`tests/e2e/happy_test.go`:
```go
func TestE2E_HappyPath_OrderConfirmed(t *testing.T) {
    if testing.Short() { t.Skip() }
    h := harness.New(t)
    stopO := h.StartService(t, "order",    "../../bin/order.exe",    map[string]string{"DATABASE_URL": h.PostgresURLs["order"],    "KAFKA_BROKER": h.KafkaBrokers[0], "HTTP_ADDR": ":18081"})
    stopP := h.StartService(t, "payment",  "../../bin/payment.exe",  map[string]string{"DATABASE_URL": h.PostgresURLs["payment"],  "KAFKA_BROKER": h.KafkaBrokers[0], "HTTP_ADDR": ":18082"})
    stopI := h.StartService(t, "inventory","../../bin/inventory.exe",map[string]string{"DATABASE_URL": h.PostgresURLs["inventory"],"KAFKA_BROKER": h.KafkaBrokers[0], "REDIS_URL": h.RedisURL, "HTTP_ADDR": ":18083"})
    defer stopO(); defer stopP(); defer stopI()

    time.Sleep(3 * time.Second) // boot

    body, _ := os.ReadFile("../../examples/order.json")
    resp := mustPost(t, "http://127.0.0.1:18081/v1/orders", body)
    var created struct{ ID string `json:"id"` }
    json.Unmarshal(resp, &created)

    deadline := time.Now().Add(30 * time.Second)
    for time.Now().Before(deadline) {
        st := mustGet(t, fmt.Sprintf("http://127.0.0.1:18081/v1/orders/%s", created.ID))
        if strings.Contains(string(st), `"state":"confirmed"`) { return }
        time.Sleep(500 * time.Millisecond)
    }
    t.Fatal("order did not reach confirmed in 30s")
}
```

- [ ] **Step 3: Run; expect PASS**

```powershell
cd C:\Users\t0p_m\projects\orderflow\tests
go test ./e2e/... -v -run TestE2E_HappyPath_OrderConfirmed
```

- [ ] **Step 4: Commit**

```powershell
git add tests
git commit -m "orderflow/3.11.b: E2E happy path test"
```

### Task 3.11.c — E2E compensation (PAR)

**Files:**
- Create: `tests/e2e/compensation_test.go`

**Interfaces:**
- Same harness. POST order with a `lastFour=0001` (card_declined per `services/payment/internal/provider/provider.go` contract from checkpoint).

- [ ] **Step 1: Failing test**

```go
func TestE2E_Compensation_PaymentDeclined(t *testing.T) {
    if testing.Short() { t.Skip() }
    h := harness.New(t)
    // boot all 3 services (same as happy)
    body := []byte(`{"customer_id":"8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f","items":[{"sku":"SKU-001","quantity":1,"unit_price_cents":1999}],"payment":{"last_four":"0001"}}`)
    resp := mustPost(t, "http://127.0.0.1:18081/v1/orders", body)
    // poll for state == cancelled within 30s
    // also assert: inventory Redis reservation released (key TTL or absent)
}
```

- [ ] **Step 2: Run; expect PASS**

```powershell
cd C:\Users\t0p_m\projects\orderflow\tests
go test ./e2e/... -v -run TestE2E_Compensation
```

- [ ] **Step 3: Commit**

```powershell
git add tests
git commit -m "orderflow/3.11.c: E2E compensation test (payment declined -> cancel)"
```

### Task 3.11.d — Chaos: redpanda kill mid-flow (PAR)

**Files:**
- Create: `tests/chaos/kafka_kill_test.go`

**Interfaces:**
- Add to `harness`: `h.Kafka.Terminate(ctx)` and `h.RestartKafka(ctx)` helpers.

- [ ] **Step 1: Failing test**

```go
func TestChaos_RedpandaKill_OrderStillCompletes(t *testing.T) {
    if testing.Short() { t.Skip() }
    h := harness.New(t)
    // boot all 3 services
    // POST /v1/orders
    // after 500ms, h.Kafka.Terminate(); sleep 5s; h.RestartKafka()
    // poll for state == confirmed within 60s
}
```

- [ ] **Step 2: Run; expect PASS** (outbox retries until kafka back up)

```powershell
cd C:\Users\t0p_m\projects\orderflow\tests
go test ./chaos/... -v
```

- [ ] **Step 3: Commit**

```powershell
git add tests
git commit -m "orderflow/3.11.d: chaos test — redpanda kill mid-order"
```

### Task 3.11.e — Load: 100 RPS for 60s with k6 (PAR)

**Files:**
- Create: `tests/load/k6.js`
- Create: `tests/load/load_test.go` (testcontainers-spawned k6 binary; fails if p95 > 1s)
- Modify: `Makefile` (add `make load` target)

**Interfaces:**
- k6 script POSTs 100 RPS for 60s, asserts p95 < 1000ms.

- [ ] **Step 1: Install k6 in CI container**

In `tests/load/k6.js`:
```javascript
import http from 'k6/http';
import { check } from 'k6';
export const options = { vus: 50, duration: '60s', thresholds: { http_req_duration: ['p(95)<1000'] } };
export default function () {
    const res = http.post('http://host.docker.internal:18081/v1/orders', JSON.stringify({
        customer_id: '8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f',
        items: [{ sku: 'SKU-001', quantity: 1, unit_price_cents: 1999 }],
    }), { headers: { 'Content-Type': 'application/json' } });
    check(res, { '201': r => r.status === 201 });
}
```

- [ ] **Step 2: Go wrapper**

```go
func TestLoad_100RPS_p95Under1s(t *testing.T) {
    if testing.Short() { t.Skip() }
    // spawn k6 container, mount k6.js, point at host.docker.internal:18081
    // assert exit code 0
}
```

- [ ] **Step 3: Makefile target**

Add:
```make
load:
	go test ./tests/load/... -v -timeout 5m
```

- [ ] **Step 4: Run; expect PASS** (requires docker; CPU-bound)

```powershell
cd C:\Users\t0p_m\projects\orderflow
make load
```

- [ ] **Step 5: Commit**

```powershell
git add tests Makefile
git commit -m "orderflow/3.11.e: load test — 100 RPS p95<1s via k6"
```

### Task 3.11.f — CI integration (SEQ)

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- New job `e2e` that runs on PR, depends on `build`, uses docker service.

- [ ] **Step 1: Add job**

```yaml
e2e:
  runs-on: ubuntu-latest
  needs: build
  steps:
    - uses: actions/checkout@v4
    - uses: docker/setup-buildx-action@v3
    - run: make build
    - run: go test ./tests/... -timeout 15m
```

- [ ] **Step 2: Commit**

```powershell
git add .github/workflows/ci.yml
git commit -m "orderflow/3.11.f: CI job for E2E/chaos/load"
```