# orderflow-web Dashboard + Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the bare `/` → `/orders` redirect with a real dashboard at `/dashboard` showing four KPI tiles, five upstream-health chips (Order / Payment / Inventory / Saga / Kafka tail), a recent-orders list, and a Welcome empty-state card. All backend changes live inside `services/web`; domain services are untouched.

**Architecture:** SvelteKit SPA (Svelte 5 runes) embedded into the Go BFF via `//go:embed`. The BFF adds one new endpoint — `GET /api/health/all` — that fans out to each upstream service's `/healthz` in parallel (2s per probe, 1s snapshot cache) and reports the in-process Kafka-tail state. KPI tiles are computed client-side from the existing `GET /api/orders?limit=10` window. Health is polled every 5s, orders every 2s; both intervals pause on `document.visibilityState === 'hidden'` and resume on `visibilitychange`.

**Tech Stack:** Go 1.25.13, chi v5.3.1, net/http, slog, SvelteKit 2 + Svelte 5 runes, TypeScript, Vitest-free (manual smoke only for this spec).

**Spec:** `docs/superpowers/specs/2026-08-30-dashboard-design.md`

## Global Constraints

- Go version floor: **1.25.13** (per `go.work`).
- All backend changes inside `services/web/`. No edits to `services/order/`, `services/payment/`, `services/inventory/`, `services/saga/`, `pkg/`, or `cmd/`.
- All frontend changes inside `services/web/frontend/`.
- Reuse the existing fake backend clients (`fakeOrder`, `fakePayment`, `fakeInventory`) in `internal/server/api_test.go` for new tests — do NOT introduce new fake implementations.
- Tests use the existing external-test-package convention: `package server_test` for new probe_test.go.
- The probe must always return HTTP 200 with the snapshot JSON; degraded/down states are valid payload contents, never HTTP errors.
- 1-second snapshot cache is in-process; safe because the playground runs single-process.
- The SvelteKit SPA was already built — `frontend/dist/` exists and is embedded into the Go binary. Frontend changes that affect what the user sees require a rebuild (`npm run build`) before `go build` so the embed picks up the new bundle.
- No new external Go dependencies. No new npm dependencies.
- Commit message style: `feat(web): <short description>` or `fix(web): <short description>`, mirroring `7476ca9`, `d0a3a55`, `d21da3d`.
- All identifiers stay ASCII-only (no emoji in code).
- Visibility-aware polling: `document.visibilityState !== 'visible'` pauses both intervals; `visibilitychange` resumes them.

---

## File Structure Map

### New files

| Path | Created by | Purpose |
|---|---|---|
| `services/web/internal/server/probe_test.go` | T3 | Table-driven tests for `HealthAll` using `httptest.NewServer` |
| `services/web/frontend/src/lib/dashboard.ts` | T7 | KPI derivation + `hasDown` + `statusClass` helpers |
| `services/web/frontend/src/routes/dashboard/+page.svelte` | T8 | Dashboard page (KPIs + HealthPanel + RecentOrders + Welcome) |

### Modified files

| Path | Modified by | Change |
|---|---|---|
| `services/web/internal/web/main.go` | T1 | Read `SAGA_URL`; pass it through `server.Options.Urls` |
| `services/web/internal/kafkatail/tail.go` | T2 | Expose `Healthy() bool` on the running tail |
| `services/web/internal/server/probe.go` | T3 | Add `ServiceHealth` / `HealthSnapshot` types and `HealthAll` handler with 1s cache |
| `services/web/internal/server/server.go` | T4 | Extend `Options` with `Urls` and `KafkaHealth`; register `GET /api/health/all` |
| `services/web/frontend/src/lib/types.ts` | T5 | Add `ServiceHealth` + `HealthSnapshot` types |
| `services/web/frontend/src/lib/api.ts` | T6 | Add `getHealthAll()` |
| `services/web/frontend/src/routes/+page.svelte` | T9 | Change `goto('/orders')` to `goto('/dashboard')` |
| `services/web/frontend/src/routes/+layout.svelte` | T10 | Add Dashboard to nav, extend active-state logic |

---

# STAGE 1 — Backend plumbing

## Task 1: Expose upstream URLs as a package-level config struct

**Files:**
- Modify: `services/web/internal/web/main.go:105-112` (the block that reads env vars and constructs `bc`)

**Why first:** Everything in Stage 1+ needs to know the order/payment/inventory/saga URLs. Today only `bc` (a `backend.HTTPClient`) holds them; the probe handler needs a separate read-only view. We pass them via `server.Options.Urls` to keep `internal/server` independent of `internal/web` (which already imports `internal/server` — a reverse import would cycle).

**Interfaces:**
- Consumes: nothing (reads `os.Getenv` directly).
- Produces: a fully populated `server.ServiceURLs` value handed to `server.Options.Urls`.

- [ ] **Step 1.1: Add `SAGA_URL` to `Run`'s env reads**

Open `services/web/internal/web/main.go`. In `Run`, immediately after the `inventoryURL` line (currently `internal/web/main.go:108`), add:

```go
sagaURL := envOrDefault("SAGA_URL", "http://localhost:8084")
```

Also extend the `logger.Info("orderflow-web starting", ...)` block to log `saga_url`. Add a new entry to the existing `logger.Info(...)` call:

```go
"saga_url", redact(sagaURL),
```

Place it next to the other upstream URLs (after `inventory_url`).

- [ ] **Step 1.2: Define `ServiceURLs` in `internal/server`**

Open `services/web/internal/server/server.go`. Above the `Options` struct (currently around line 51), add:

```go
// ServiceURLs captures the resolved upstream base URLs. The
// probe handler reads these via Options.Urls so it can fan out
// to /healthz without re-reading env vars.
type ServiceURLs struct {
    Order     string
    Payment   string
    Inventory string
    Saga      string
}
```

- [ ] **Step 1.3: Add `Urls` field to `Options`**

Still in `internal/server/server.go`, extend the `Options` struct (around line 51-59) with:

```go
Urls ServiceURLs
```

Add it as the last field. The existing struct already has `Name`, `Logger`, `Order`, `Payment`, `Inventory`, `Bus`, `EventsEnabled` — `Urls` joins them at the bottom.

- [ ] **Step 1.4: Populate `Options.Urls` before `srv.Start()`**

In `services/web/internal/web/main.go`, modify the `server.New(server.Options{...})` block (around line 132-140) to include the URLs:

```go
srv := server.New(server.Options{
    Name:         "web",
    Logger:       logger,
    Order:        bc,
    Payment:      bc,
    Inventory:    bc,
    Bus:          bus,
    EventsEnabled: stopTail != nil,
    Urls: server.ServiceURLs{
        Order:     orderURL,
        Payment:   paymentURL,
        Inventory: inventoryURL,
        Saga:      sagaURL,
    },
})
```

- [ ] **Step 1.5: Build + smoke-check**

```bash
cd C:\Users\t0p_m\projects\orderflow
cd services/web && go build ./...
cd ../..
```

Expected: build succeeds with no errors. (No tests touched — existing tests must still pass.)

Run `cd services/web && go test ./...` and confirm all green.

- [ ] **Step 1.6: Commit**

```bash
git add services/web/internal/web/main.go services/web/internal/server/server.go
git commit -m "feat(web): add SAGA_URL + ServiceURLs to server.Options for health probes"
```

---

## Task 2: Add `Healthy()` accessor to kafkatail

**Files:**
- Modify: `services/web/internal/kafkatail/tail.go` (around line 47-90 where the goroutine loop lives)

**Why:** The probe handler needs to know whether the Kafka tail is connected. `Start` returns a stop function today; it should also return (or expose) a health accessor.

**Interfaces:**
- Consumes: nothing.
- Produces:
  - Returns from `Start` unchanged (still `(func(), error)`).
  - New package-level var `var Health atomic.Bool` in `package kafkatail`, set to `true` when a consumer is running and healthy, `false` after an error or when `Start` is called with empty brokers.

- [ ] **Step 2.1: Add `Health` atomic var**

In `services/web/internal/kafkatail/tail.go`, add to the import block `"sync/atomic"` (already imported). Add at package level (above `var topics = []string{...}`):

```go
// Health reports whether the Kafka tail consumer is currently
// connected. The probe handler reads this for the dashboard's
// Kafka chip. Starts true when a consumer is running; flips
// false on Run error and on graceful shutdown.
var Health atomic.Bool
```

- [ ] **Step 2.2: Set `Health = true` when consumer starts**

In `Start` (around line 80, just inside the `for` loop body where the consumer goroutine is launched), add immediately before the consumer is created:

```go
Health.Store(true)
```

- [ ] **Step 2.3: Set `Health = false` on error**

In the same `Start` function, inside the existing `if err := consumer.Run(...); err != nil { ... }` branch, add at the top of that branch:

```go
Health.Store(false)
```

Also set `Health.Store(false)` immediately before `return nil, nil` in the `if brokersCSV == ""` branch (around line 49).

- [ ] **Step 2.4: Set `Health = false` on shutdown**

At the top of the `closed.Store(true)` path inside the goroutine, add:

```go
Health.Store(false)
```

This is the line that runs when `Stop()` is called and the for-loop exits.

- [ ] **Step 2.5: Build + test**

```bash
cd services/web && go build ./... && go test ./...
```

Expected: green.

- [ ] **Step 2.6: Commit**

```bash
git add services/web/internal/kafkatail/tail.go
git commit -m "feat(web): expose kafkatail.Health atomic for dashboard probe"
```

---

## Task 3: `HealthAll` handler with probe logic + tests

**Files:**
- Modify: `services/web/internal/server/probe.go`
- Create: `services/web/internal/server/probe_test.go`

**Why:** This is the entire backend feature in one handler. The test file uses `httptest.NewServer` to stand up fake upstream `/healthz` endpoints and exercises every probe branch (ok / slow / timeout / 5xx / connection refused / body-shape variations).

**Interfaces:**
- Consumes: `s.opt.Urls` (the URLs plumbed through `Options`) and `s.opt.KafkaHealth()` (added in Task 4).
- Produces:
  ```go
  // In package server
  type ServiceHealth struct {
      Status    string `json:"status"`              // "ok"|"degraded"|"down"
      LatencyMS int64  `json:"latency_ms"`
      TakenAt   string `json:"taken_at"`            // RFC3339Nano
      Detail    string `json:"detail,omitempty"`    // populated when not ok
  }

  type HealthSnapshot struct {
      Order      ServiceHealth `json:"order"`
      Payment    ServiceHealth `json:"payment"`
      Inventory  ServiceHealth `json:"inventory"`
      Saga       ServiceHealth `json:"saga"`
      Kafka      ServiceHealth `json:"kafka"`       // ok|down only — no degraded
      SnapshotAt string        `json:"snapshot_at"` // RFC3339Nano
  }

  func (s *Server) HealthAll(w http.ResponseWriter, r *http.Request)
  ```

- [ ] **Step 3.1: Add types + probe function (no handler yet)**

Open `services/web/internal/server/probe.go`. Replace the existing `pingUpstreams` block (lines 16-60) with the new types + `probeOne` helper. Keep `pingUpstreams` if anything else still calls it (grep `services/web` for `pingUpstreams` — if nothing calls it, delete it; if `readyz` calls it, leave it untouched).

Add the following types + helpers. Note: this step does NOT touch the `kafkatail` package — Kafka health is wired in via the closure in Task 4. So `probe.go` imports only stdlib.

The existing `probe.go` import block has `"context"`, `"net/http"`, `"sync"`, `"time"`. After this step it must include:

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "strings"
    "sync"
    "time"
)
```

(`sync` is unused until Step 3.4 adds the `HealthAll` handler; golangci-lint may complain. If it does, add `//nolint:unused` above the import for now, or add a `var _ = sync.Mutex{}` shim — better, just write Step 3.4 in the same commit so the import is used.)

Add the following code below the imports:

```go
type ServiceHealth struct {
    Status    string `json:"status"`
    LatencyMS int64  `json:"latency_ms"`
    TakenAt   string `json:"taken_at"`
    Detail    string `json:"detail,omitempty"`
}

type HealthSnapshot struct {
    Order      ServiceHealth `json:"order"`
    Payment    ServiceHealth `json:"payment"`
    Inventory  ServiceHealth `json:"inventory"`
    Saga       ServiceHealth `json:"saga"`
    Kafka      ServiceHealth `json:"kafka"`
    SnapshotAt string        `json:"snapshot_at"`
}

// probeOne GETs u's /healthz with a 2s timeout and classifies
// the result per the rules in the dashboard spec:
//   - down      if transport error / timeout / HTTP 5xx / body says "down"
//   - degraded  if HTTP 200 AND (latency >= 1s OR body says "degraded")
//   - ok        otherwise (HTTP 200, latency < 1s, body absent or "ok")
// Always returns a non-zero ServiceHealth — the caller never
// has to nil-check.
func probeOne(parent context.Context, u string) ServiceHealth {
    start := time.Now()
    pctx, cancel := context.WithTimeout(parent, 2*time.Second)
    defer cancel()
    req, err := http.NewRequestWithContext(pctx, http.MethodGet, u+"/healthz", nil)
    if err != nil {
        return ServiceHealth{
            Status:  "down",
            TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
            Detail:  err.Error(),
        }
    }
    resp, err := http.DefaultClient.Do(req)
    elapsed := time.Since(start)
    if err != nil {
        return ServiceHealth{
            Status:    "down",
            LatencyMS: elapsed.Milliseconds(),
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
            Detail:    err.Error(),
        }
    }
    defer resp.Body.Close()
    body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
    bodyStr := strings.TrimSpace(string(body))
    bodyStatus := parseBodyStatus(bodyStr)
    switch {
    case resp.StatusCode >= 500:
        return ServiceHealth{
            Status:    "down",
            LatencyMS: elapsed.Milliseconds(),
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
            Detail:    fmt.Sprintf("upstream returned %d", resp.StatusCode),
        }
    case bodyStatus == "down":
        return ServiceHealth{
            Status:    "down",
            LatencyMS: elapsed.Milliseconds(),
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
            Detail:    bodyStr,
        }
    case resp.StatusCode >= 200 && resp.StatusCode < 300:
        if elapsed >= time.Second || bodyStatus == "degraded" {
            return ServiceHealth{
                Status:    "degraded",
                LatencyMS: elapsed.Milliseconds(),
                TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
                Detail:    detailForDegraded(elapsed, bodyStr),
            }
        }
        return ServiceHealth{
            Status:    "ok",
            LatencyMS: elapsed.Milliseconds(),
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
        }
    default:
        return ServiceHealth{
            Status:    "down",
            LatencyMS: elapsed.Milliseconds(),
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
            Detail:    fmt.Sprintf("upstream returned %d", resp.StatusCode),
        }
    }
}

// parseBodyStatus extracts a "status":"<x>" field from the body.
// Returns "" if the body is empty, not JSON, or has no status.
func parseBodyStatus(body string) string {
    if body == "" {
        return ""
    }
    var parsed struct {
        Status string `json:"status"`
    }
    if err := json.Unmarshal([]byte(body), &parsed); err != nil {
        return ""
    }
    return parsed.Status
}

func detailForDegraded(elapsed time.Duration, body string) string {
    if body != "" && parseBodyStatus(body) == "degraded" {
        return body
    }
    return fmt.Sprintf("latency %dms exceeds 1000ms threshold", elapsed.Milliseconds())
}
```

Add `sync` to the existing import block in `probe.go` (it is NOT yet imported — add `"sync"` alongside the others).

- [ ] **Step 3.2: Write the failing test**

Create `services/web/internal/server/probe_test.go`:

```go
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

// makeUpstream stands up a tiny /healthz server whose status code,
// body, and latency are all configurable.
type fakeUpstream struct {
    *httptest.Server
    calls atomic.Int32
}

func newFakeUpstream(status int, body string, delay time.Duration) *fakeUpstream {
    f := &fakeUpstream{}
    f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
        Name: "test",
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
    defer order.Close(); defer payment.Close()
    defer inventory.Close(); defer saga.Close()

    srv := server.New(server.Options{
        Name: "test",
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
    if snap.Order.Status != "ok" { t.Errorf("order=%q", snap.Order.Status) }
    if snap.Payment.Status != "down" { t.Errorf("payment=%q", snap.Payment.Status) }
    if snap.Payment.Detail == "" { t.Errorf("payment detail empty") }
    if snap.Inventory.Status != "ok" { t.Errorf("inventory=%q", snap.Inventory.Status) }
    if snap.Saga.Status != "ok" { t.Errorf("saga=%q", snap.Saga.Status) }
}

func TestHealthAll_Degraded(t *testing.T) {
    order := newFakeUpstream(200, ``, 1500*time.Millisecond) // >1s
    payment := newFakeUpstream(200, ``, 0)
    inventory := newFakeUpstream(200, `{"status":"degraded"}`, 0)
    saga := newFakeUpstream(200, ``, 0)
    defer order.Close(); defer payment.Close()
    defer inventory.Close(); defer saga.Close()

    srv := server.New(server.Options{
        Name: "test",
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
    if snap.Inventory.Status != "degraded" {
        t.Errorf("inventory=%q want degraded", snap.Inventory.Status)
    }
}

func TestHealthAll_Timeout(t *testing.T) {
    order := newFakeUpstream(200, ``, 3*time.Second) // >2s timeout
    payment := newFakeUpstream(200, ``, 0)
    inventory := newFakeUpstream(200, ``, 0)
    saga := newFakeUpstream(200, ``, 0)
    defer order.Close(); defer payment.Close()
    defer inventory.Close(); defer saga.Close()

    srv := server.New(server.Options{
        Name: "test",
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
    if snap.Payment.Status != "ok" {
        t.Errorf("payment=%q want ok", snap.Payment.Status)
    }
}

func TestHealthAll_KafkaDown(t *testing.T) {
    order := newFakeUpstream(200, ``, 0)
    payment := newFakeUpstream(200, ``, 0)
    inventory := newFakeUpstream(200, ``, 0)
    saga := newFakeUpstream(200, ``, 0)
    defer order.Close(); defer payment.Close()
    defer inventory.Close(); defer saga.Close()

    srv := server.New(server.Options{
        Name: "test",
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
}
```

- [ ] **Step 3.3: Run tests — confirm they fail (compile error)**

```bash
cd services/web && go test ./internal/server/... -run TestHealthAll -v
```

Expected: `undefined: server.ServiceHealth` / `undefined: server.HealthSnapshot` / `undefined: server.HealthAll`. The handler doesn't exist yet.

- [ ] **Step 3.4: Add `HealthAll` handler on `*Server`**

Still in `services/web/internal/server/probe.go`, add the handler at the bottom of the file:

```go
// HealthAll GET /api/health/all — probes every upstream /healthz
// in parallel (2s per probe, 1s snapshot cache) and reports the
// in-process Kafka tail state. Always returns HTTP 200; degraded
// and down are valid payload contents. Cache key is the wall
// clock — collision-free in practice for a single-process
// playground.
func (s *Server) HealthAll(w http.ResponseWriter, r *http.Request) {
    type cacheEntry struct {
        taken    time.Time
        snapshot HealthSnapshot
    }
    s.healthCacheMu.Lock()
    cached, ok := s.healthCache.(cacheEntry)
    if ok && time.Since(cached.taken) < time.Second {
        s.healthCacheMu.Unlock()
        writeJSON(w, http.StatusOK, cached.snapshot)
        return
    }
    s.healthCacheMu.Unlock()

    urls := s.opt.Urls
    var wg sync.WaitGroup
    snap := HealthSnapshot{SnapshotAt: time.Now().UTC().Format(time.RFC3339Nano)}
    for _, target := range []struct {
        name string
        url  string
        dest *ServiceHealth
    }{
        {"order", urls.Order, &snap.Order},
        {"payment", urls.Payment, &snap.Payment},
        {"inventory", urls.Inventory, &snap.Inventory},
        {"saga", urls.Saga, &snap.Saga},
    } {
        target := target
        wg.Add(1)
        go func() {
            defer wg.Done()
            if target.url == "" {
                *target.dest = ServiceHealth{
                    Status:  "down",
                    TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
                    Detail:  "upstream URL not configured",
                }
                return
            }
            *target.dest = probeOne(r.Context(), target.url)
        }()
    }
    // Kafka has no /healthz — read the closure supplied via Options.
    if s.opt.KafkaHealth != nil && s.opt.KafkaHealth() {
        snap.Kafka = ServiceHealth{
            Status:    "ok",
            LatencyMS: 0,
            TakenAt:   time.Now().UTC().Format(time.RFC3339Nano),
        }
    } else {
        snap.Kafka = ServiceHealth{
            Status:  "down",
            TakenAt: time.Now().UTC().Format(time.RFC3339Nano),
            Detail:  "Kafka tail not running (KAFKA_BROKERS unset or consumer error)",
        }
    }
    wg.Wait()

    s.healthCacheMu.Lock()
    s.healthCache = cacheEntry{taken: time.Now(), snapshot: snap}
    s.healthCacheMu.Unlock()

    writeJSON(w, http.StatusOK, snap)
}
```

You will also need to add the cache field to `Server`. Open `services/web/internal/server/server.go` and modify the `Server` struct (around line 62-67) to add:

```go
type Server struct {
    opt     Options
    srv     *http.Server
    addr    atomic.Value // string
    api     *API

    healthCacheMu sync.Mutex
    healthCache   any // cacheEntry — typed inside probe.go
}
```

`sync` is already imported in `server.go` (see line 35).

- [ ] **Step 3.5: Run tests — confirm they pass**

```bash
cd services/web && go test ./internal/server/... -run TestHealthAll -v
```

Expected: all 5 tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add services/web/internal/server/probe.go services/web/internal/server/probe_test.go services/web/internal/server/server.go
git commit -m "feat(web): /api/health/all — parallel probe of 4 upstreams + kafka tail"
```

---

## Task 4: Register `/api/health/all` route + extend Options

**Files:**
- Modify: `services/web/internal/server/server.go` (the `Start` method around line 91-117)
- Modify: `services/web/internal/server/server.go` (the `Options` struct around line 51-59 — add `KafkaHealth func() bool`)
- Modify: `services/web/internal/web/main.go` (the `server.New(server.Options{...})` block around line 132-140 — populate `KafkaHealth`)

**Why:** The probe handler exists but is unreachable. Wire it into the router and populate its config from `Run`.

**Interfaces:**
- `server.Options` gains `KafkaHealth func() bool` (a closure so `internal/server` does not import `internal/kafkatail`).

- [ ] **Step 4.1: Extend `Options` with `KafkaHealth`**

Open `services/web/internal/server/server.go`. In the `Options` struct (around line 51-59), add a new field at the end:

```go
KafkaHealth func() bool // reports whether the Kafka tail is connected
```

- [ ] **Step 4.2: Register the route in `Start`**

In the same file, in `Start` (around line 117 where the API routes are registered), add:

```go
r.Get("/api/health/all", s.HealthAll)
```

Place it next to the other `/api/*` routes (before the SSE registration).

- [ ] **Step 4.3: Pass `KafkaHealth` from `Run`**

Open `services/web/internal/web/main.go`. In the `server.New(server.Options{...})` block (around line 132-140), add a new entry:

```go
KafkaHealth: kafkatail.Health.Load,
```

- [ ] **Step 4.4: Build + test**

```bash
cd services/web && go build ./... && go test ./...
```

Expected: green. The handler in `probe.go` already uses `s.opt.KafkaHealth` (set up in Task 3 Step 3.4), so no further `probe.go` changes are needed.

- [ ] **Step 4.5: Commit**

```bash
git add services/web/internal/server/server.go services/web/internal/web/main.go
git commit -m "feat(web): register /api/health/all route + wire KafkaHealth into Options"
```

---

# STAGE 2 — Frontend types + API client

## Task 5: Add `ServiceHealth` + `HealthSnapshot` TS types

**Files:**
- Modify: `services/web/frontend/src/lib/types.ts`

**Why:** The dashboard's `getHealthAll()` returns the BFF's `HealthSnapshot` JSON. TypeScript needs matching types so the dashboard code compiles.

- [ ] **Step 5.1: Append the types to `types.ts`**

Open `services/web/frontend/src/lib/types.ts`. At the bottom of the file (after the `SseEvent` interface), append:

```ts
// Health snapshot from GET /api/health/all. Wire format matches
// services/web/internal/server/probe.go ServiceHealth +
// HealthSnapshot. Keep these two definitions in sync.

export type ServiceStatus = 'ok' | 'degraded' | 'down';

export interface ServiceHealth {
  status: ServiceStatus;
  latency_ms: number;
  taken_at: string;
  detail?: string;
}

export interface HealthSnapshot {
  order: ServiceHealth;
  payment: ServiceHealth;
  inventory: ServiceHealth;
  saga: ServiceHealth;
  /** Kafka tail has only ok|down (no degraded middle ground). */
  kafka: { status: 'ok' | 'down'; latency_ms: number; taken_at: string; detail?: string };
  snapshot_at: string;
}
```

- [ ] **Step 5.2: Verify TypeScript compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: no errors. (If `tsc` reports unused / missing — fix before committing. The types here are not yet used, but TS will not complain about unused exports.)

- [ ] **Step 5.3: Commit**

```bash
git add services/web/frontend/src/lib/types.ts
git commit -m "feat(web): add HealthSnapshot TS types matching /api/health/all wire format"
```

---

## Task 6: Add `getHealthAll()` API client

**Files:**
- Modify: `services/web/frontend/src/lib/api.ts`

- [ ] **Step 6.1: Append `getHealthAll` to `api.ts`**

Open `services/web/frontend/src/lib/api.ts`. Add `HealthSnapshot` to the import list at the top:

```ts
import type {
  OrderListResponse,
  Order,
  OrderState,
  PaymentWebhook,
  StockItem,
  SubmitOrderRequest,
  HealthSnapshot,
} from './types';
```

At the bottom of the file (after `fireWebhook`), append:

```ts
export async function getHealthAll(): Promise<HealthSnapshot> {
  const res = await fetch('/api/health/all', {
    headers: { Accept: 'application/json' }
  });
  return jsonOrThrow<HealthSnapshot>(res);
}
```

- [ ] **Step 6.2: Verify TS compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 6.3: Commit**

```bash
git add services/web/frontend/src/lib/api.ts
git commit -m "feat(web): add getHealthAll() API client"
```

---

# STAGE 3 — Frontend dashboard

## Task 7: `$lib/dashboard.ts` derivation helpers

**Files:**
- Create: `services/web/frontend/src/lib/dashboard.ts`

**Why:** Pure-function helpers — easy to read, no Svelte runes, no DOM. Lives in `$lib` so multiple components could reuse them later.

- [ ] **Step 7.1: Create the file**

Create `services/web/frontend/src/lib/dashboard.ts` with the following content:

```ts
// Dashboard derivation helpers. Pure functions — no DOM, no
// Svelte runes, no fetch. The dashboard page imports these and
// feeds them reactive state.

import type { HealthSnapshot, Order, ServiceStatus } from './types';

export interface KpiSummary {
  /** Number of orders created today (browser-local time). */
  ordersToday: number;
  /** Confirmed / (confirmed + cancelled + failed) * 100. Null when
   *  no terminal orders exist in the window. */
  successRatePct: number | null;
  /** Orders currently in pending or reserved. */
  inFlight: number;
  /** Mean (completed_at − created_at) over completed orders in the
   *  window. Null when no completed orders exist. */
  avgCompletionMs: number | null;
}

const TERMINAL_STATES = new Set(['confirmed', 'cancelled', 'failed']);

function startOfToday(): Date {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d;
}

export function kpiFromOrders(orders: Order[]): KpiSummary {
  const startToday = startOfToday();
  let ordersToday = 0;
  let inFlight = 0;
  let confirmed = 0;
  let cancelled = 0;
  let failed = 0;
  let completionSum = 0;
  let completionCount = 0;
  for (const o of orders) {
    if (new Date(o.created_at) >= startToday) {
      ordersToday++;
      if (o.state === 'pending' || o.state === 'reserved') inFlight++;
      if (TERMINAL_STATES.has(o.state)) {
        if (o.state === 'confirmed') confirmed++;
        else if (o.state === 'cancelled') cancelled++;
        else failed++;
        if (o.completed_at) {
          const ms = new Date(o.completed_at).getTime() - new Date(o.created_at).getTime();
          if (Number.isFinite(ms) && ms >= 0) {
            completionSum += ms;
            completionCount++;
          }
        }
      }
    }
  }
  const terminals = confirmed + cancelled + failed;
  const successRatePct = terminals === 0 ? null : (confirmed / terminals) * 100;
  const avgCompletionMs = completionCount === 0 ? null : completionSum / completionCount;
  return { ordersToday, successRatePct, inFlight, avgCompletionMs };
}

export function hasDown(snap: HealthSnapshot | null): boolean {
  if (!snap) return false;
  return (
    snap.order.status === 'down' ||
    snap.payment.status === 'down' ||
    snap.inventory.status === 'down' ||
    snap.saga.status === 'down' ||
    snap.kafka.status === 'down'
  );
}

export function downServiceNames(snap: HealthSnapshot): string[] {
  const names: string[] = [];
  if (snap.order.status === 'down') names.push('Order');
  if (snap.payment.status === 'down') names.push('Payment');
  if (snap.inventory.status === 'down') names.push('Inventory');
  if (snap.saga.status === 'down') names.push('Saga');
  if (snap.kafka.status === 'down') names.push('Kafka tail');
  return names;
}

export function statusClass(status: ServiceStatus | 'ok' | 'down'): string {
  return `chip-${status}`;
}

/** Formats an ISO timestamp for display: "14:32:07" or "14:32:07.123". */
export function fmtClock(iso: string): string {
  return iso.slice(11, 19);
}
```

- [ ] **Step 7.2: Verify TS compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 7.3: Commit**

```bash
git add services/web/frontend/src/lib/dashboard.ts
git commit -m "feat(web): dashboard derivation helpers (KPI + health checks)"
```

---

## Task 8: Dashboard page (`/dashboard`)

**Files:**
- Create: `services/web/frontend/src/routes/dashboard/+page.svelte`

**Why:** This is the centerpiece of the spec. KPI tiles, health chips, recent-orders table, empty-state Welcome.

- [ ] **Step 8.1: Create the file**

Create `services/web/frontend/src/routes/dashboard/+page.svelte` with the following content (the file is self-contained; it has no test in this spec):

```svelte
<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { ApiError, getHealthAll, listOrders } from '$lib/api';
  import type { HealthSnapshot, Order } from '$lib/types';
  import {
    downServiceNames,
    fmtClock,
    hasDown,
    kpiFromOrders
  } from '$lib/dashboard';

  let health = $state<HealthSnapshot | null>(null);
  let orders = $state<Order[]>([]);
  let healthErr = $state<string | null>(null);
  let ordersErr = $state<string | null>(null);

  let healthTimer: ReturnType<typeof setInterval> | null = null;
  let ordersTimer: ReturnType<typeof setInterval> | null = null;

  let kpis = $derived(kpiFromOrders(orders));
  let degradedNames = $derived(health ? downServiceNames(health).join(', ') : '');
  let showBanner = $derived(hasDown(health));
  let isEmpty = $derived(orders.length === 0 && !ordersErr);

  async function refreshHealth(): Promise<void> {
    try {
      health = await getHealthAll();
      healthErr = null;
    } catch (e) {
      if (e instanceof ApiError) healthErr = e.message;
      else healthErr = String(e);
      health = null;
    }
  }

  async function refreshOrders(): Promise<void> {
    try {
      orders = await listOrders({});
      ordersErr = null;
    } catch (e) {
      if (e instanceof ApiError) ordersErr = e.message;
      else ordersErr = String(e);
    }
  }

  function startTimers(): void {
    stopTimers();
    refreshHealth();
    refreshOrders();
    healthTimer = setInterval(refreshHealth, 5_000);
    ordersTimer = setInterval(refreshOrders, 2_000);
  }

  function stopTimers(): void {
    if (healthTimer) { clearInterval(healthTimer); healthTimer = null; }
    if (ordersTimer) { clearInterval(ordersTimer); ordersTimer = null; }
  }

  function onVisibility(): void {
    if (document.visibilityState === 'visible') startTimers();
    else stopTimers();
  }

  function fmtTime(iso: string): string {
    return iso.slice(0, 16).replace('T', ' ');
  }

  function fmtSkuLine(items: Order['items']): string {
    return items.map((it) => `${it.sku}×${it.quantity}`).join(' ');
  }

  onMount(() => {
    startTimers();
    document.addEventListener('visibilitychange', onVisibility);
  });

  onDestroy(() => {
    stopTimers();
    document.removeEventListener('visibilitychange', onVisibility);
  });
</script>

<svelte:head>
  <title>Dashboard — OrderFlow</title>
</svelte:head>

<section>
  {#if healthErr}
    <div class="banner banner-fatal">
      Backend unreachable — {healthErr}
      <button onclick={refreshHealth}>retry</button>
    </div>
  {:else if showBanner}
    <div class="banner banner-down">
      {downServiceNames(health!).length} service(s) unreachable: {degradedNames}
    </div>
  {/if}

  <div class="row-between">
    <h1>Dashboard</h1>
    <a class="btn" href="/orders/new">+ New order</a>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="kpi-label">Orders today</div>
      <div class="kpi-value">{kpis.ordersToday}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Success rate</div>
      <div class="kpi-value">
        {kpis.successRatePct === null ? '—' : `${kpis.successRatePct.toFixed(1)}%`}
      </div>
    </div>
    <div class="kpi">
      <div class="kpi-label">In-flight</div>
      <div class="kpi-value">{kpis.inFlight}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Avg completion</div>
      <div class="kpi-value">
        {kpis.avgCompletionMs === null
          ? '—'
          : kpis.avgCompletionMs < 1000
            ? `${Math.round(kpis.avgCompletionMs)} ms`
            : `${(kpis.avgCompletionMs / 1000).toFixed(2)} s`}
      </div>
    </div>
  </div>

  <div class="grid">
    <section class="panel">
      <h2>Health</h2>
      {#if health}
        <ul class="chips">
          {#each [
            { name: 'Order',     h: health.order },
            { name: 'Payment',   h: health.payment },
            { name: 'Inventory', h: health.inventory },
            { name: 'Saga',      h: health.saga },
            { name: 'Kafka tail', h: health.kafka }
          ] as item}
            <li>
              <span class="chip chip-{item.h.status}" title={`latency ${item.h.latency_ms}ms · taken ${fmtClock(item.h.taken_at)}${item.h.detail ? ' · ' + item.h.detail : ''}`}>
                {item.name}
                <span class="chip-status">{item.h.status}</span>
              </span>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">Awaiting first probe…</p>
      {/if}
    </section>

    <section class="panel">
      <h2>Recent orders</h2>
      {#if ordersErr}
        <div class="banner banner-soft">{ordersErr} (retrying)</div>
      {:else if orders.length === 0}
        <p class="muted">No orders yet.</p>
      {:else}
        <table>
          <thead>
            <tr>
              <th>ID</th><th>State</th><th>Items</th><th>Created</th><th></th>
            </tr>
          </thead>
          <tbody>
            {#each orders.slice(0, 10) as o (o.id)}
              <tr>
                <td class="mono">{o.id.slice(0, 8)}…</td>
                <td><span class="badge state-{o.state}">{o.state}</span></td>
                <td class="mono small">{fmtSkuLine(o.items)}</td>
                <td class="muted small">{fmtTime(o.created_at)}</td>
                <td><a href={`/orders/${o.id}`}>view →</a></td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </section>
  </div>

  {#if isEmpty && !healthErr}
    <section class="welcome">
      <h2>Welcome to OrderFlow</h2>
      <p>
        This is the playground for the orderflow distributed order
        processing platform. Create your first order to see the
        saga run in real time.
      </p>
      <div class="welcome-actions">
        <a class="btn" href="/orders/new">+ Create order</a>
        <button class="btn-secondary" disabled title="Coming soon — Spec #2">
          Seed demo data
        </button>
      </div>
    </section>
  {/if}
</section>

<style>
  .row-between { display: flex; align-items: center; justify-content: space-between; margin-bottom: var(--gap-4); }
  .row-between h1 { margin: 0; font-size: var(--fs-xl); }

  .banner {
    padding: var(--gap-2) var(--gap-4);
    border-radius: var(--radius);
    margin-bottom: var(--gap-4);
    display: flex; align-items: center; gap: var(--gap-3);
  }
  .banner-down { background: var(--bad-soft); color: var(--bad); border: 1px solid var(--bad); }
  .banner-fatal { background: var(--bad-soft); color: var(--bad); border: 1px solid var(--bad); justify-content: space-between; }
  .banner-soft { background: var(--panel-2); color: var(--fg-muted); border: 1px solid var(--border); font-size: var(--fs-sm); }

  .kpis {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: var(--gap-3); margin-bottom: var(--gap-4);
  }
  @media (max-width: 720px) {
    .kpis { grid-template-columns: repeat(2, 1fr); }
  }
  .kpi {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--gap-3) var(--gap-4);
  }
  .kpi-label { color: var(--fg-muted); font-size: var(--fs-sm); margin-bottom: var(--gap-1); }
  .kpi-value { font-size: var(--fs-2xl); font-weight: 600; font-family: var(--font-mono); }

  .grid {
    display: grid; grid-template-columns: 1fr 2fr;
    gap: var(--gap-4); margin-bottom: var(--gap-4);
  }
  @media (max-width: 960px) { .grid { grid-template-columns: 1fr; } }

  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--gap-4);
  }
  .panel h2 { margin: 0 0 var(--gap-3); font-size: var(--fs-md); color: var(--fg-muted); text-transform: uppercase; letter-spacing: 0.05em; }

  .chips { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: var(--gap-2); }
  .chip {
    display: flex; align-items: center; justify-content: space-between;
    padding: var(--gap-2) var(--gap-3);
    border-radius: var(--radius);
    border: 1px solid var(--border);
    background: var(--panel-2);
    cursor: help;
  }
  .chip-status { font-family: var(--font-mono); font-size: var(--fs-xs); }
  .chip-ok      { border-color: var(--good); }
  .chip-ok .chip-status      { color: var(--good); }
  .chip-degraded { border-color: var(--warn); }
  .chip-degraded .chip-status { color: var(--warn); }
  .chip-down    { border-color: var(--bad); }
  .chip-down .chip-status    { color: var(--bad); }

  table { width: 100%; border-collapse: collapse; }
  th, td { padding: var(--gap-2) var(--gap-3); text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--fg-muted); font-weight: 600; font-size: var(--fs-sm); }
  .small { font-size: var(--fs-sm); }

  .badge { padding: 2px var(--gap-2); border-radius: var(--radius-pill); font-size: var(--fs-xs); font-weight: 600; }
  .badge.state-pending { background: rgba(210,153,34,0.15); color: var(--warn); }
  .badge.state-reserved { background: rgba(68,147,248,0.15); color: var(--accent); }
  .badge.state-confirmed { background: rgba(86,211,100,0.15); color: var(--good); }
  .badge.state-cancelled,
  .badge.state-failed { background: rgba(248,81,73,0.15); color: var(--bad); }

  .btn {
    display: inline-block; padding: var(--gap-2) var(--gap-3);
    background: var(--accent); color: white;
    border: 0; border-radius: var(--radius); font-weight: 600;
  }
  .btn:hover { text-decoration: none; opacity: 0.9; }
  .btn-secondary {
    padding: var(--gap-2) var(--gap-3);
    background: var(--panel-2); color: var(--fg-muted);
    border: 1px solid var(--border); border-radius: var(--radius);
    cursor: not-allowed; font-weight: 600;
  }

  .welcome {
    background: var(--panel);
    border: 1px dashed var(--border-strong);
    border-radius: var(--radius-lg);
    padding: var(--gap-5);
    text-align: center;
  }
  .welcome h2 { margin: 0 0 var(--gap-3); font-size: var(--fs-lg); }
  .welcome p { margin: 0 auto var(--gap-4); max-width: 480px; color: var(--fg-muted); }
  .welcome-actions { display: flex; gap: var(--gap-3); justify-content: center; }
</style>
```

- [ ] **Step 8.2: Verify TS compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 8.3: Commit**

```bash
git add services/web/frontend/src/routes/dashboard/+page.svelte
git commit -m "feat(web): dashboard route with KPIs + health chips + recent orders"
```

---

## Task 9: Change root redirect to `/dashboard`

**Files:**
- Modify: `services/web/frontend/src/routes/+page.svelte`

- [ ] **Step 9.1: Change `goto('/orders')` to `goto('/dashboard')`**

Open `services/web/frontend/src/routes/+page.svelte`. Replace the entire `<script>` block content with:

```svelte
<script lang="ts">
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';

  // Root route — redirect to /dashboard so the SPA lands on
  // the at-a-glance summary rather than the raw orders list.
  // Server-side fallback in the BFF returns index.html for any
  // non-API GET, but doing it client-side avoids a round trip
  // and lets the browser's back-button return to the list.
  onMount(() => {
    goto('/dashboard', { replaceState: true });
  });
</script>

<p class="muted">Redirecting to dashboard…</p>
```

- [ ] **Step 9.2: Verify TS compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 9.3: Commit**

```bash
git add services/web/frontend/src/routes/+page.svelte
git commit -m "feat(web): redirect / to /dashboard"
```

---

## Task 10: Add Dashboard to top-nav

**Files:**
- Modify: `services/web/frontend/src/routes/+layout.svelte`

- [ ] **Step 10.1: Extend the nav with Dashboard link**

Open `services/web/frontend/src/routes/+layout.svelte`. In the `<nav>` block (currently lines 21-25), add a Dashboard link as the first item:

```svelte
<nav>
  <a href="/" class:active={$page.url.pathname === '/' || $page.url.pathname.startsWith('/dashboard')}>Dashboard</a>
  <a href="/orders" class:active={!$page.url.pathname.startsWith('/dashboard') && ($page.url.pathname === '/' || $page.url.pathname.startsWith('/orders'))}>Orders</a>
  <a href="/inventory" class:active={$page.url.pathname.startsWith('/inventory')}>Inventory</a>
  <a href="/payments/sim" class:active={$page.url.pathname.startsWith('/payments')}>Payments sim</a>
</nav>
```

Also update the active-state check on the existing Orders link — the new Dashboard link uses the root URL `/`, so the Orders link must NOT be active when the user is on `/` or `/dashboard`. The expression above already handles that with `!$page.url.pathname.startsWith('/dashboard')`.

- [ ] **Step 10.2: Verify TS compiles**

```bash
cd services/web/frontend && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 10.3: Commit**

```bash
git add services/web/frontend/src/routes/+layout.svelte
git commit -m "feat(web): add Dashboard to top-nav with active-state"
```

---

# STAGE 4 — Build, smoke, verify

## Task 11: Build the SPA + Go binary + run all checks

**Files:** (none — verification only)

**Why:** Frontend changes don't take effect until the SvelteKit bundle is rebuilt and the Go binary picks up the embed. This task produces the artifact operators run.

- [ ] **Step 11.1: Build the SvelteKit bundle**

```bash
cd C:\Users\t0p_m\projects\orderflow\services\web\frontend
npm run build
```

Expected: `frontend/build/` and `frontend/dist/` populated with the new route manifest. Confirm with:

```bash
ls frontend/.svelte-kit/types/src/routes/dashboard/
```

Should show `$types.d.ts` and similar files. If not, the build failed — inspect stdout.

- [ ] **Step 11.2: Build the Go binary**

```bash
cd C:\Users\t0p_m\projects\orderflow
make build
```

Expected: `bin/web` (and `bin/order`, `bin/payment`, `bin/inventory`, `bin/saga`) produced with version-injected LDFLAGS.

- [ ] **Step 11.3: Run all Go tests**

```bash
cd C:\Users\t0p_m\projects\orderflow
make test
```

Expected: green. The new `TestHealthAll_*` suite must pass alongside the existing tests.

- [ ] **Step 11.4: Run linter**

```bash
cd C:\Users\t0p_m\projects\orderflow
make lint
```

Expected: clean. If golangci-lint complains about `import shadow` in the `HealthAll` handler's `for` loop, rename the loop var to `target` (which it already is) and ensure no underscore prefixes are needed.

- [ ] **Step 11.5: Run `go vet`**

```bash
cd C:\Users\t0p_m\projects\orderflow
go vet ./services/web/...
```

Expected: clean.

- [ ] **Step 11.6: Manual smoke (the 8-point checklist)**

```bash
# In one terminal — start the full stack
cd C:\Users\t0p_m\projects\orderflow
bash scripts/run.sh
```

Wait for all five binaries to be healthy.

```bash
# In a second terminal
cd C:\Users\t0p_m\projects\orderflow
curl -s http://localhost:8085/api/health/all | jq
```

Walk through:
1. `curl /` → SPA returns `index.html`.
2. In browser, open `http://localhost:8085/` → redirects to `/dashboard`.
3. On a fresh DB: all 5 chips green (or grey "Awaiting first probe" then green); 4 KPI tiles show `0` / `—`.
4. Create an order via `/orders/new`; return to `/dashboard`; KPI "Orders today" ticks to 1; recent-orders row appears within 2s.
5. Force-fail the payment via `/payments/sim`; revisit `/dashboard`; "Success rate" eventually shows `<100.0%` once a `cancelled` order appears.
6. Stop the Order service: `docker compose stop order` (or kill the terminal running it). Within 5s the Order chip turns red; banner appears with `Order` in the names list.
7. Hide the tab; wait 30s; show the tab again. Open browser devtools console — no errors. KPI numbers refreshed (timers resumed).
8. Stop all services. Reload `/dashboard`. "Backend unreachable" banner with retry button appears. KPI tiles grey. Recent-orders section hidden.

- [ ] **Step 11.7: Restart services for next developer**

```bash
bash scripts/run.sh
```

- [ ] **Step 11.8: Commit any build artifact changes**

The SvelteKit build updates `frontend/dist/index.html` and several other files. These are normally git-ignored, but the project's current `.gitignore` keeps them tracked. Run:

```bash
cd C:\Users\t0p_m\projects\orderflow
git status
```

If `frontend/dist/`, `frontend/build/`, or `.svelte-kit/` show modifications, add them:

```bash
git add services/web/frontend/dist services/web/frontend/build services/web/frontend/.svelte-kit
git commit -m "build(web): refresh SPA bundle with dashboard route"
```

If `.gitignore` already excludes these paths, the commit is a no-op — skip it.

- [ ] **Step 11.9: Verify `git diff --stat origin/main`**

```bash
git fetch origin
git diff --stat origin/main
```

Expected: only files inside `services/web/**` are touched. No domain-service, OpenAPI, or infra files modified.

---

## Definition of Done

- `make build`, `make test`, `make lint` all green.
- Visiting `/` lands on `/dashboard`.
- KPI tiles show real numbers within 5s of `make run-web` + `bash scripts/run.sh`.
- Health chips reflect upstream state within 5s of a service start/stop.
- Empty-state Welcome card appears on a fresh DB; the disabled "Seed demo data" slot is visible.
- Degraded banner appears within 5s of any service becoming unreachable.
- Visibility-aware polling stops and resumes without console errors.
- Only `services/web/**` files are modified (per `git diff --stat origin/main`).
- All 5 commits follow the existing project's commit-message style (`feat(web):` prefix).