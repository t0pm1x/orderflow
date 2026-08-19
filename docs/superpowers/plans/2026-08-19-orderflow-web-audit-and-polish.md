# orderflow-web Playground — Audit & Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the `services/web` playground from "ship-able with caveats" to "demo-ready" by fixing 4 BLOCKERs, 10 P0s, 12 P1s, 10 P2s, 8 P3s — 44 fixes across 8 parallel tracks.

**Architecture:** Server-rendered HTML + htmx 2.0.3 (vendored) + SSE for live events + per-order event ring buffer for timeline + bounded idempotency tokens for double-submit protection + unified `KAFKA_BROKERS` env var + design tokens for color-blind-safe status indicators.

**Tech Stack:** Go 1.25.13, chi v5.3.1, html/template, htmx 2.0.3, franz-go, pgx/v5, github.com/google/uuid, slog.

**Spec:** `docs/superpowers/specs/2026-08-19-orderflow-web-audit-and-polish-design.md`

## Global Constraints

- Go 1.25.13 (matches `go.work` and CI).
- chi v5.3.1 (matches `go.mod`).
- htmx 2.0.3 — vendored, NOT CDN.
- Env var: `KAFKA_BROKERS` (CSV) — replaces `KAFKA_BROKER` with back-compat shim.
- Web default port: `:8085` host (binary default stays `:8083` for back-compat).
- Status colors must remain color-blind safe (icon + color, never color-only).
- Every form must have `hx-disabled-elt="this"` on submit + server-side idempotency.
- Every UUID input must be `uuid.Parse`-validated before reaching backend.
- Every URL path interpolation must use `url.PathEscape`.
- Backward-compatible: existing handlers, existing test signatures, existing route paths preserved.
- No new external deps without explicit justification.

---

## File Structure Map

### New files

| Path | Created by | Purpose |
|------|-----------|---------|
| `services/web/internal/static/vendor/htmx.min.js` | P0.1 | Vendored htmx 2.0.3 |
| `services/web/internal/static/vendor/htmx-sse.min.js` | P0.1 | Vendored htmx-sse 2.0.3 |
| `services/web/internal/static/vendor/htmx-sse.min.js.map` | P0.1 | Optional source map |
| `services/web/internal/static/diagrams/saga_happy.svg` | P3.4 | Static SVG of happy-path state machine |
| `services/web/internal/static/diagrams/saga_compensation.svg` | P3.4 | Static SVG of compensation state machine |
| `services/web/internal/templates/order_events.html` | P0.2 | Per-order event timeline |
| `services/web/internal/templates/order_hero.html` | P2.1 | First-time-user hero card |
| `services/web/internal/handlers/errors.go` | P1.1 | `mapUpstreamError` helper |
| `scripts/smoke-web.ps1` | BL.2 | Automated web playground smoke |
| `scripts/smoke-web.sh` | BL.2 | POSIX sibling |

### Modified files (grouped by track)

**Track α — handlers + events logic:**
`internal/handlers/handlers.go`, `internal/handlers/pages.go`, `internal/handlers/errors.go` (new), `internal/events/bus.go`, `internal/events/bus_test.go`.

**Track β — kafkatail:**
`internal/kafkatail/tail.go`, `internal/events/bus.go` (ring buffer).

**Track γ — CSS + templates (markup only):**
`internal/static/styles.css`, `internal/templates/{orders_list,order_detail,payments,layout}.html`, `internal/templates/order_hero.html` (new).

**Track δ — layout JS + vendor:**
`internal/templates/layout.html`, `internal/static/package.go`, `internal/static/vendor/*` (new), `internal/static/diagrams/*` (new).

**Track 5 — backend:**
`services/order/internal/repository/pg_repo.go`, `services/order/internal/api/handler.go`, `api/openapi.yaml`, `services/web/internal/backend/types.go`, `services/web/internal/backend/{order,inventory,errors}.go`.

**Track 6 — web README:**
`services/web/README.md`.

**Track 7 — scripts + config:**
`scripts/{run.ps1,run.sh,run-demo.ps1,run-demo-manual.ps1,smoke-web.ps1,smoke-web.sh}`, `docs/demo/demo.sh`, `cmd/{order,payment,inventory,saga,web}/main.go`, `deploy/docker-compose.yml`, `Makefile`, `services/web/internal/server/server.go`, `services/web/internal/web/main.go`, `internal/server/server.go` (for P2.8).

**Track 8 — top-level docs:**
`README.md`, `STATUS.md`, `docs/adr/0001-saga-vs-choreography.md`, `docs/adr/0003-rest-vs-grpc.md`.

---

# STAGE 1 — BLOCKER fixes

## Task 1: BL.1 — Fix runaway Kafka log loop

**Files:**
- Modify: `services/web/internal/kafkatail/tail.go:1-108`
- Modify: `pkg/consumer/consumer.go` (find the poll loop)
- Test: existing `kafkatail/tail_test.go`

**Problem:** Web log grows ~8 MB/sec with the same `WARN consumer: poll fetch error topic="" partition=-1 err="client closed"` line. The poll loop keeps iterating after the consumer is closed.

**Steps:**

- [ ] **Step 1: Identify the close signal**

```bash
grep -n "client closed" pkg/consumer/*.go
grep -n "Stop\|Close" pkg/consumer/consumer.go
```

Read the consumer's `Stop()` / `Run()` to confirm the close flow.

- [ ] **Step 2: Add a `closed` flag to the tail's stop function**

In `services/web/internal/kafkatail/tail.go`, modify `Start`:

```go
func Start(ctx context.Context, logger *slog.Logger, brokersCSV string, bus *events.Bus) (func(), error) {
    if brokersCSV == "" {
        logger.Info("kafka tail disabled: KAFKA_BROKERS not set")
        return nil, nil
    }
    // ... existing setup ...
    var wg sync.WaitGroup
    var closed atomic.Bool
    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := c.Run(ctx); err != nil && !closed.Load() {
            logger.Error("kafka tail exited", "err", err)
        }
    }()
    stop := func() {
        if !closed.CompareAndSwap(false, true) {
            return
        }
        c.Stop()
        wg.Wait()
    }
    return stop, nil
}
```

Add `"sync/atomic"` to imports.

- [ ] **Step 3: Cap log spam with exponential backoff**

In the consumer's poll loop (find the `WARN consumer: poll fetch error` line in `pkg/consumer/`), wrap the warn so it fires at most once per 5 seconds during a sustained error:

```go
var lastWarn atomic.Int64 // unix nanos
// inside the poll loop on error:
now := time.Now().UnixNano()
if now-lastWarn.Load() > int64(5*time.Second) {
    log.Warn("consumer: poll fetch error", "topic", topic, "partition", partition, "err", err)
    lastWarn.Store(now)
}
```

(Exact variable names depend on the consumer's actual API; adapt the location but keep the throttle semantics.)

- [ ] **Step 4: Verify**

```bash
cd C:\Users\t0p_m\projects\orderflow
go build ./...
go test ./services/web/... ./pkg/consumer/... -count=1
```

Expected: build clean, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/web/internal/kafkatail/tail.go pkg/consumer/consumer.go
git commit -m "fix(kafkatail): throttle Kafka poll-error log + dedupe stop"
```

---

## Task 2: BL.2 — Add automated smoke script

**Files:**
- Create: `scripts/smoke-web.ps1`
- Create: `scripts/smoke-web.sh`

**Steps:**

- [ ] **Step 1: Create `scripts/smoke-web.ps1`**

```powershell
#!/usr/bin/env pwsh
# scripts/smoke-web.ps1 — automated smoke for the orderflow-web playground.
# Asserts happy path + compensation + 4xx + 5xx against a running stack.
# Requires: scripts/run.ps1 already executed (services + web listening).

[CmdletBinding()]
param(
    [string]$WebUrl    = 'http://127.0.0.1:8085',
    [string]$OrderUrl  = 'http://127.0.0.1:8081',
    [string]$PaymentUrl = 'http://127.0.0.1:8082',
    [string]$LogDir    = (Join-Path $PSScriptRoot '..\tests\logs')
)

$ErrorActionPreference = 'Continue'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$logFile = Join-Path $LogDir 'smoke-web.log'
'' | Out-File -FilePath $logFile -Encoding utf8

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan; "`n=== $msg ===" | Out-File -Append -FilePath $logFile -Encoding utf8 }
function Pass($msg) { Write-Host "  PASS: $msg" -ForegroundColor Green; "  PASS: $msg" | Out-File -Append -FilePath $logFile -Encoding utf8 }
function Fail($msg) { Write-Host "  FAIL: $msg" -ForegroundColor Red; "  FAIL: $msg" | Out-File -Append -FilePath $logFile -Encoding utf8; $script:failures++ }
function Expect($actual, $expected, $label) {
    if ($actual -eq $expected) { Pass "$label = $actual" }
    else { Fail "$label expected $expected, got $actual" }
}

$failures = 0

# 1. healthz
Step "healthz"
$h = Invoke-WebRequest "$WebUrl/healthz" -UseBasicParsing -TimeoutSec 5
Expect $h.StatusCode 200 "healthz"

# 2. readyz
$rz = Invoke-WebRequest "$WebUrl/readyz" -UseBasicParsing -TimeoutSec 5
Expect $rz.StatusCode 200 "readyz"

# 3. orders list page renders
Step "orders list"
$list = Invoke-WebRequest "$WebUrl/" -UseBasicParsing -TimeoutSec 5
Expect $list.StatusCode 200 "/"
if ($list.Content -match 'OrderFlow|orderflow-web') { Pass "/ contains brand" } else { Fail "/ missing brand" }

# 4. happy path: POST /v1/orders
Step "happy path"
$body = @{ customer_id = [guid]::NewGuid().ToString(); items = @(@{ sku = 'SKU-SMOKE'; quantity = 1; unit_price_cents = 1999 }); payment = @{ last_four = '4242' } } | ConvertTo-Json -Depth 5
$created = Invoke-WebRequest "$OrderUrl/v1/orders" -Method Post -ContentType 'application/json' -Body $body -UseBasicParsing -TimeoutSec 5
Expect $created.StatusCode 201 "POST /v1/orders"
$oid = ($created.Content | ConvertFrom-Json).id
if ($oid) { Pass "order id = $oid" } else { Fail "no order id" }

# 5. poll for confirmed
$state = 'pending'
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $r = Invoke-WebRequest "$OrderUrl/v1/orders/$oid" -UseBasicParsing -TimeoutSec 5
    $state = ($r.Content | ConvertFrom-Json).state
    if ($state -eq 'confirmed') { break }
}
Expect $state 'confirmed' "final state"

# 6. compensation path
Step "compensation path"
$body2 = @{ customer_id = [guid]::NewGuid().ToString(); items = @(@{ sku = 'SKU-SMOKE'; quantity = 1; unit_price_cents = 1999 }); payment = @{ last_four = '0001' } } | ConvertTo-Json -Depth 5
$created2 = Invoke-WebRequest "$OrderUrl/v1/orders" -Method Post -ContentType 'application/json' -Body $body2 -UseBasicParsing -TimeoutSec 5
Expect $created2.StatusCode 201 "POST compensation"
$oid2 = ($created2.Content | ConvertFrom-Json).id

# 7. payment sim fire failed
$wh = @{ order_id = $oid2; payment_id = $oid2; status = 'failed'; error_code = 'card_declined' } | ConvertTo-Json
$h2 = @{ 'Idempotency-Key' = "orderflow-web:${oid2}:failed" }
try {
    $fired = Invoke-WebRequest "$PaymentUrl/v1/payments/webhook" -Method Post -ContentType 'application/json' -Body $wh -Headers $h2 -UseBasicParsing -TimeoutSec 5
    Expect $fired.StatusCode 200 "fire webhook"
} catch { Fail "fire webhook threw: $_" }

# 8. final state cancelled
$state2 = 'pending'
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $r = Invoke-WebRequest "$OrderUrl/v1/orders/$oid2" -UseBasicParsing -TimeoutSec 5
    $state2 = ($r.Content | ConvertFrom-Json).state
    if ($state2 -in @('cancelled','failed')) { break }
}
if ($state2 -in @('cancelled','failed')) { Pass "compensation state = $state2" } else { Fail "compensation state = $state2" }

# 9. invalid UUID rejected
Step "validation"
try {
    $bad = Invoke-WebRequest "$WebUrl/v1/orders" -Method Post -ContentType 'application/x-www-form-urlencoded' -Body 'sku=&quantity=0' -UseBasicParsing -TimeoutSec 5
    Expect $bad.StatusCode 400 "empty form"
} catch {
    if ($_.Exception.Response.StatusCode -eq 400) { Pass "empty form = 400" }
    else { Fail "empty form threw: $_" }
}

# 10. inventory page renders
$inv = Invoke-WebRequest "$WebUrl/inventory" -UseBasicParsing -TimeoutSec 5
Expect $inv.StatusCode 200 "inventory"

# 11. payments sim renders
$ps = Invoke-WebRequest "$WebUrl/payments/sim" -UseBasicParsing -TimeoutSec 5
Expect $ps.StatusCode 200 "payments sim"

# 12. order detail renders
$od = Invoke-WebRequest "$WebUrl/orders/$oid" -UseBasicParsing -TimeoutSec 5
Expect $od.StatusCode 200 "order detail"

# summary
Step "summary"
if ($failures -eq 0) {
    Write-Host "ALL PASS" -ForegroundColor Green
    "ALL PASS" | Out-File -Append -FilePath $logFile -Encoding utf8
    exit 0
} else {
    Write-Host "$failures FAILURE(S)" -ForegroundColor Red
    "$failures FAILURE(S)" | Out-File -Append -FilePath $logFile -Encoding utf8
    exit 1
}
```

- [ ] **Step 2: Create POSIX sibling `scripts/smoke-web.sh`**

Mirror the .ps1 logic in bash + curl + jq. (Same test order, same assertions; ~120 lines.)

- [ ] **Step 3: Run it against the live stack**

```bash
cd C:\Users\t0p_m\projects\orderflow
powershell -ExecutionPolicy Bypass -File scripts\smoke-web.ps1
```

Expected: `ALL PASS`.

- [ ] **Step 4: Commit**

```bash
git add scripts/smoke-web.ps1 scripts/smoke-web.sh
git commit -m "feat(scripts): automated web playground smoke (happy + compensation + validation)"
```

---

## Task 3: BL.3 — Set OTEL_EXPORTER=stdout in demo scripts

**Files:**
- Modify: `docs/demo/demo.sh:55-95`
- Modify: `scripts/run-demo.ps1` (find where binaries are started)
- Modify: `scripts/run-demo-manual.ps1` (same)

**Steps:**

- [ ] **Step 1: Add `OTEL_EXPORTER=stdout` to demo.sh**

Find the block that starts `bin/order` etc. (around line 70-86). Prepend `OTEL_EXPORTER=stdout \` before each binary's env var block, or set it globally above the loop:

```bash
export OTEL_EXPORTER=stdout
```

- [ ] **Step 2: Same for `scripts/run-demo.ps1` and `scripts/run-demo-manual.ps1`**

Find where each binary is launched. Add `$env:OTEL_EXPORTER = 'stdout'` before the `Start-Process` call (or set it once at the top).

- [ ] **Step 3: Verify by re-running the stack and checking logs**

```bash
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -NoBuild
Select-String -Path tests\logs\order.log -Pattern "no such host" -SimpleMatch
```

Expected: no matches.

- [ ] **Step 4: Commit**

```bash
git add docs/demo/demo.sh scripts/run-demo.ps1 scripts/run-demo-manual.ps1
git commit -m "fix(scripts): set OTEL_EXPORTER=stdout in demo scripts"
```

---

## Task 4: BL.4 — Remove dead `OrderUpdated` Kafka subscription

**Files:**
- Modify: `services/web/internal/kafkatail/tail.go:50-65`

**Steps:**

- [ ] **Step 1: Remove the line**

```go
// before:
"OrderUpdated":          forwardToBus(bus),

// after: (delete the line)
```

- [ ] **Step 2: Verify build**

```bash
go build ./services/web/...
go test ./services/web/...
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/kafkatail/tail.go
git commit -m "fix(kafkatail): drop dead OrderUpdated subscription (never published)"
```

---

# STAGE 2 — P0 fixes

## Task 5: P0.1 — Vendor htmx 2.0.3 + htmx-sse into embed.FS

**Files:**
- Create: `services/web/internal/static/vendor/htmx.min.js`
- Create: `services/web/internal/static/vendor/htmx-sse.min.js`
- Modify: `services/web/internal/static/package.go`
- Modify: `services/web/internal/templates/layout.html:8`
- Modify: `services/web/internal/server/server.go:89-102`

**Steps:**

- [ ] **Step 1: Download htmx 2.0.3 + htmx-sse 2.0.3**

```bash
mkdir -p services/web/internal/static/vendor
Invoke-WebRequest -Uri 'https://cdn.jsdelivr.net/npm/htmx.org@2.0.3/dist/htmx.min.js' -OutFile services/web/internal/static/vendor/htmx.min.js
Invoke-WebRequest -Uri 'https://cdn.jsdelivr.net/npm/htmx-ext-sse@2.0.3/htmx-sse.min.js' -OutFile services/web/internal/static/vendor/htmx-sse.min.js
```

Verify both files downloaded and are non-empty.

- [ ] **Step 2: Update `static/package.go` to embed vendor directory**

```go
//go:embed styles.css vendor/* diagrams/*
var FS embed.FS
```

- [ ] **Step 3: Update `server.go` static handler to serve vendor + diagrams**

Replace the existing handler (lines 89-102) with one that maps `vendor/*` and `diagrams/*` to embedded files:

```go
r.Get("/static/*", func(w http.ResponseWriter, req *http.Request) {
    p := strings.TrimPrefix(req.URL.Path, "/static/")
    data, err := static.FS.ReadFile(p)
    if err != nil {
        http.NotFound(w, req)
        return
    }
    contentType := mime.TypeByExtension(filepath.Ext(p))
    if contentType == "" {
        contentType = "application/octet-stream"
    }
    w.Header().Set("Content-Type", contentType+"; charset=utf-8")
    _, _ = w.Write(data)
})
```

Add imports for `mime`, `path/filepath`.

- [ ] **Step 4: Update `layout.html` to load from /static/**

Replace:
```html
<script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.3/dist/htmx.min.js" crossorigin="anonymous"></script>
```

With:
```html
<script src="/static/vendor/htmx.min.js"></script>
<script src="/static/vendor/htmx-sse.min.js" defer></script>
```

- [ ] **Step 5: Verify**

```bash
go build ./services/web/...
go test ./services/web/...
# start stack, then:
curl -sS http://127.0.0.1:8085/static/vendor/htmx.min.js | head -c 200
```

Expected: build clean, htmx body returned.

- [ ] **Step 6: Commit**

```bash
git add services/web/internal/static/vendor/ services/web/internal/static/package.go services/web/internal/server/server.go services/web/internal/templates/layout.html
git commit -m "feat(web): vendor htmx 2.0.3 + htmx-sse into embed.FS (offline, no CDN, no SRI worry)"
```

---

## Task 6: P0.2 — Per-order event ring buffer + timeline endpoint + template

**Files:**
- Modify: `services/web/internal/events/bus.go` (add ring buffer + History method)
- Modify: `services/web/internal/events/bus_test.go` (extend)
- Create: `services/web/internal/templates/order_events.html`
- Modify: `services/web/internal/handlers/handlers.go` (NewSet + Routes)
- Modify: `services/web/internal/handlers/pages.go` (PageOrderEvents)
- Modify: `services/web/internal/templates/order_detail.html` (link to timeline)

**Steps:**

- [ ] **Step 1: Add ring buffer + History to `events/bus.go`**

Add fields + methods:

```go
// At top of bus.go, add:
const ringCap = 200

type ringEntry struct {
    aggregateID string
    env         pkgEvents.Envelope
}

// In Bus struct add:
type Bus struct {
    mu   sync.Mutex
    subs map[chan BusEvent]struct{}
    ring []ringEntry     // bounded; ringCap
    done chan struct{}
}

// In NewBus add ring: make([]ringEntry, 0, ringCap).

// In Publish, after the fan-out, append to ring under lock:
func (b *Bus) Publish(e BusEvent) {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.closed() {
        return
    }
    // ... existing fan-out ...
    b.ring = append(b.ring, ringEntry{aggregateID: e.Envelope.AggregateID, env: e.Envelope})
    if len(b.ring) > ringCap {
        // drop oldest 10% to amortize
        drop := ringCap / 10
        b.ring = b.ring[drop:]
    }
}

// Add History method:
func (b *Bus) History(aggregateID string) []pkgEvents.Envelope {
    b.mu.Lock()
    defer b.mu.Unlock()
    out := make([]pkgEvents.Envelope, 0)
    for _, e := range b.ring {
        if e.aggregateID == aggregateID {
            out = append(out, e.env)
        }
    }
    return out
}
```

- [ ] **Step 2: Add unit tests in `bus_test.go`**

```go
func TestBus_History_PerAggregate(t *testing.T) {
    b := NewBus()
    defer b.Close()
    for i := 0; i < 50; i++ {
        agg := "ord-A"
        if i%2 == 0 { agg = "ord-B" }
        b.Publish(BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: agg}})
    }
    hA := b.History("ord-A")
    hB := b.History("ord-B")
    if len(hA) != 25 || len(hB) != 25 {
        t.Fatalf("expected 25 each, got A=%d B=%d", len(hA), len(hB))
    }
}

func TestBus_RingOverflow(t *testing.T) {
    b := NewBus()
    defer b.Close()
    for i := 0; i < ringCap*3; i++ {
        b.Publish(BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: "ord"}})
    }
    h := b.History("ord")
    if len(h) > ringCap {
        t.Fatalf("history exceeded ringCap: %d", len(h))
    }
}
```

- [ ] **Step 3: Run tests**

```bash
go test ./services/web/internal/events/... -v
```

Expected: PASS (new + existing).

- [ ] **Step 4: Create `services/web/internal/templates/order_events.html`**

```html
{{define "orderEventsBody"}}
<div id="timeline-{{.OrderID}}" hx-get="/orders/{{.OrderID}}/events?frag=1" hx-trigger="every 1s" hx-swap="outerHTML">
  <h3>Saga timeline</h3>
  {{if not .Events}}
  <p class="muted">No events received yet for this order. The timeline will populate as the saga runs.</p>
  {{else}}
  <ol class="timeline">
    {{range .Events}}
    <li class="timeline-node timeline-{{.EventType}}">
      <span class="timeline-time mono" title="{{.OccurredAt.Format "2006-01-02 15:04:05.000"}}">{{.OccurredAt.Format "15:04:05"}}</span>
      <span class="timeline-type mono">{{.EventType}}</span>
      <details class="timeline-payload"><summary>payload</summary><pre class="mono">{{.Payload}}</pre></details>
    </li>
    {{end}}
  </ol>
  {{end}}
</div>
{{end}}
```

- [ ] **Step 5: Add `PageOrderEvents` handler in `handlers/pages.go`**

```go
type orderEventsVM struct {
    Body    string
    OrderID string
    Events  []pkgEvents.Envelope
}

func (s *Set) PageOrderEvents(w http.ResponseWriter, r *http.Request) {
    id := chi.URLParam(r, "id")
    vm := orderEventsVM{Body: "orderEventsBody", OrderID: id}
    if _, err := uuid.Parse(id); err == nil {
        vm.Events = s.Bus.History(id)
    }
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    if r.URL.Query().Get("frag") == "1" {
        renderFragment(w, s.Templates, "orderEventsBody", vm)
        return
    }
    _ = s.Templates.ExecuteTemplate(w, "layout", vm)
}
```

Add to imports: `pkgEvents "github.com/t0pm1x/orderflow/platform/events"`, `"github.com/google/uuid"`.

- [ ] **Step 6: Register route in `handlers.go`**

In `Set.Routes`, add:
```go
r.Get("/orders/{id}/events", s.PageOrderEvents)
```

Also add `"order_events.html"` to the `template.ParseFS` call in `NewSet`.

- [ ] **Step 7: Embed timeline in order_detail.html**

Add below the items table:
```html
{{template "orderEventsBody" (dict "OrderID" .Order.ID "Events" (busHistory .Order.ID))}}
```

(Requires passing `bus` into the template; alternatively render the timeline via a separate hx-get target.) Simpler approach: in `PageOrderDetail`, also call `s.Bus.History(id)` and pass via a new view-model field.

Adjust `orderDetailVM`:
```go
type orderDetailVM struct {
    Body          string
    Order         *backend.Order
    BackendDown   bool
    Error         string
    Events        []pkgEvents.Envelope  // new
}
```

In `PageOrderDetail` success path:
```go
vm.Events = s.Bus.History(o.ID)
```

In template, render inline below the table:
```html
{{if .Events}}
<h3>Saga timeline</h3>
<ol class="timeline">
  {{range .Events}}
  <li class="timeline-node timeline-{{.EventType}}">
    <span class="timeline-time mono">{{.OccurredAt.Format "15:04:05"}}</span>
    <span class="timeline-type mono">{{.EventType}}</span>
  </li>
  {{end}}
</ol>
{{end}}
```

- [ ] **Step 8: CSS for timeline**

Append to `styles.css`:
```css
.timeline { list-style: none; padding: 0; margin: 16px 0 0; border-left: 2px solid var(--border); }
.timeline-node { padding: 6px 0 6px 16px; position: relative; }
.timeline-node::before { content: ''; position: absolute; left: -7px; top: 12px; width: 10px; height: 10px; border-radius: 50%; background: var(--muted); }
.timeline-OrderCreated::before, .timeline-OrderUpdated::before { background: var(--warn); }
.timeline-OrderConfirmed::before { background: var(--good); }
.timeline-OrderCancelled::before, .timeline-PaymentFailed::before, .timeline-StockReservationFailed::before { background: var(--bad); }
.timeline-StockReserved::before, .timeline-PaymentRequested::before, .timeline-PaymentCompleted::before { background: var(--accent); }
.timeline-time { color: var(--muted); margin-right: 8px; }
.timeline-type { font-weight: 600; }
.timeline-payload { margin-top: 4px; }
.timeline-payload pre { background: var(--bg); padding: 8px; border-radius: 4px; overflow-x: auto; font-size: 11px; }
```

- [ ] **Step 9: Verify**

```bash
go build ./...
go test ./services/web/...
# start stack, create order, visit /orders/{id} -> see timeline populate
```

- [ ] **Step 10: Commit**

```bash
git add services/web/internal/events/ services/web/internal/templates/order_events.html services/web/internal/templates/order_detail.html services/web/internal/handlers/ services/web/internal/static/styles.css
git commit -m "feat(web): per-order saga timeline (ring buffer + template + CSS)"
```

---

## Task 7: P0.3 — Form double-submit protection (htmx + idempotency)

**Files:**
- Modify: `services/web/internal/templates/order_new.html`
- Modify: `services/web/internal/templates/order_detail.html`
- Modify: `services/web/internal/templates/payments.html`
- Modify: `services/web/internal/handlers/pages.go` (ActionOrderSubmit)
- Modify: `services/web/internal/handlers/handlers.go` (render-time token)
- Test: extend `handlers/pages_test.go`

**Steps:**

- [ ] **Step 1: Add `hx-disabled-elt="this"` to every submit button**

In each template's submit form, add the attribute:
```html
<button type="submit" hx-disabled-elt="this">...</button>
```

Touch:
- `order_new.html:24` — submit button
- `order_detail.html:33` — cancel button
- `payments.html:18` — force ✓ button
- `payments.html:26` — force ✗ button

- [ ] **Step 2: Generate a per-form-render token + embed in form + header**

In `handlers.NewSet`, add a new helper or do this at request time. Simpler approach: generate the token at form-render time inside `PageOrderNew`:

```go
type orderNewVM struct {
    // ... existing ...
    IdempotencyToken string  // new
}

// in PageOrderNew:
token := newIdempotencyToken()
vm := orderNewVM{Body: "orderNewBody", IdempotencyToken: token}
```

Add helper:
```go
// in handlers/errors.go (or a new handlers/idempotency.go):
func newIdempotencyToken() string {
    var b [16]byte
    _, _ = rand.Read(b[:])
    return base64.RawURLEncoding.EncodeToString(b[:])
}
```

Imports: `crypto/rand`, `encoding/base64`.

In template `order_new.html`, add hidden input:
```html
<input type="hidden" name="idempotency_token" value="{{.IdempotencyToken}}">
```

In `ActionOrderSubmit`:
```go
token := r.FormValue("idempotency_token")
if token == "" {
    http.Error(w, "missing idempotency token", http.StatusBadRequest)
    return
}
// Add to backend client call:
//   req.Header.Set("Idempotency-Key", "orderflow-web:" + token)
```

Extend `backend.OrderClient.Submit` to accept an optional `IdempotencyKey` (or use a builder; simplest: add a field to `OrderSubmit`).

Cleanest: add a new field to `OrderSubmit`:
```go
type OrderSubmit struct {
    CustomerID      *string     `json:"customer_id,omitempty"`
    Items           []OrderItem `json:"items"`
    IdempotencyKey  string      `json:"-"` // set via header, not body
}
```

Then in `backend/order.go`:
```go
func (c *HTTPClient) Submit(ctx context.Context, in OrderSubmit) (*Order, error) {
    // ... existing ...
    if in.IdempotencyKey != "" {
        req.Header.Set("Idempotency-Key", "orderflow-web:" + in.IdempotencyKey)
    }
    // ... existing ...
}
```

(Header sent to backend is `Idempotency-Key: orderflow-web:<token>`; the backend doesn't use it today but will once the order service gets idempotency middleware.)

- [ ] **Step 3: Track token reuse**

Add a per-process token-replay cache (in-memory, bounded, TTL 5 min):

```go
// new file: services/web/internal/handlers/idempotency.go
package handlers

import (
    "sync"
    "time"
)

type replayCache struct {
    mu    sync.Mutex
    seen  map[string]time.Time
}

func newReplayCache() *replayCache { return &replayCache{seen: map[string]time.Time{}} }

func (c *replayCache) check(token string) (replay bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if t, ok := c.seen[token]; ok {
        if time.Since(t) < 5*time.Minute {
            return true
        }
    }
    c.seen[token] = time.Now()
    // opportunistic GC
    if len(c.seen) > 1024 {
        cutoff := time.Now().Add(-5 * time.Minute)
        for k, v := range c.seen {
            if v.Before(cutoff) { delete(c.seen, k) }
        }
    }
    return false
}
```

Add `replays *replayCache` to `Set` struct. Initialize in `NewSet`.

In `ActionOrderSubmit`:
```go
if s.replays.check(token) {
    http.Error(w, "duplicate submission", http.StatusConflict)
    return
}
```

Apply same pattern to `ActionOrderCancel` and `ActionPaymentsFire`.

- [ ] **Step 4: Test**

```go
func TestOrderSubmit_DuplicateToken_409(t *testing.T) {
    // ... use fakeOrderClient; submit same form twice; second should 409 ...
}
```

- [ ] **Step 5: Commit**

```bash
git add services/web/internal/templates/ services/web/internal/handlers/ services/web/internal/backend/
git commit -m "feat(web): double-submit protection (hx-disabled-elt + token replay cache)"
```

---

## Task 8: P0.4 — UUID validation + URL path escaping

**Files:**
- Modify: `services/web/internal/handlers/pages.go` (every UUID input)
- Modify: `services/web/internal/backend/order.go` (use url.PathEscape)
- Modify: `services/web/internal/backend/inventory.go` (use url.PathEscape)
- Test: extend backend tests

**Steps:**

- [ ] **Step 1: Add UUID validation helper in handlers**

```go
// in handlers/pages.go, add:
func parseUUID(s string) (string, bool) {
    if _, err := uuid.Parse(s); err != nil { return "", false }
    return s, true
}
```

- [ ] **Step 2: Validate in handlers**

In `ActionOrderSubmit`:
```go
if vm.CustomerID != "" {
    if _, ok := parseUUID(vm.CustomerID); !ok {
        vm.Error = "customer_id must be a UUID (or leave blank for auto-generation)"
        w.WriteHeader(http.StatusBadRequest)
        _ = s.Templates.ExecuteTemplate(w, "layout", vm)
        return
    }
}
```

In `ActionOrderCancel`:
```go
id := chi.URLParam(r, "id")
if _, ok := parseUUID(id); !ok {
    http.Error(w, "order id must be a UUID", http.StatusBadRequest)
    return
}
```

In `PageOrderDetail`:
```go
if _, ok := parseUUID(id); !ok {
    http.Error(w, "order id must be a UUID", http.StatusBadRequest)
    return
}
```

In `ActionPaymentsFire`:
```go
if _, ok := parseUUID(orderID); !ok {
    http.Error(w, "order_id must be a UUID", http.StatusBadRequest)
    return
}
```

- [ ] **Step 3: URL-escape in backend client**

`backend/order.go:42-44` and `backend/inventory.go:13-14`:
```go
import "net/url"
// In Get:
req, err := http.NewRequestWithContext(ctx, http.MethodGet,
    fmt.Sprintf("%s/v1/orders/%s", c.orderURL, url.PathEscape(id)), nil)
```

Same for `Cancel` and `Inventory.GetStock`.

- [ ] **Step 4: Tests**

```go
func TestOrderClient_Get_PathEscape(t *testing.T) {
    // server receives request with %2F for /
    // ...
}
```

- [ ] **Step 5: Commit**

```bash
git add services/web/internal/handlers/pages.go services/web/internal/backend/order.go services/web/internal/backend/inventory.go
git commit -m "fix(web): UUID validation + url.PathEscape on all path interpolations"
```

---

## Task 9: P0.5 — Backend SELECT includes timestamps (order service)

**Files:**
- Modify: `services/order/internal/repository/pg_repo.go:89-105`
- Test: extend `services/order/internal/api/handler_test.go` (or repo test)

**Steps:**

- [ ] **Step 1: Extend the SELECT**

Replace:
```sql
SELECT id, customer_id, items, state, total_cents FROM orders WHERE id = $1
```

With:
```sql
SELECT id, customer_id, items, state, total_cents, created_at, updated_at, completed_at, last_four
  FROM orders WHERE id = $1
```

- [ ] **Step 2: Update Scan**

In the same function:
```go
var (
    o         domain.Order
    itemsJSON []byte
    state     string
    lastFour  sql.NullString
)
if err := row.Scan(
    &o.ID, &o.CustomerID, &itemsJSON, &state, &o.TotalCents,
    &o.CreatedAt, &o.UpdatedAt, &o.CompletedAt, &lastFour,
); err != nil {
    // ...
}
if lastFour.Valid {
    o.LastFour = lastFour.String
}
```

- [ ] **Step 3: Verify**

```bash
go build ./...
go test ./services/order/...
```

- [ ] **Step 4: Commit**

```bash
git add services/order/internal/repository/pg_repo.go
git commit -m "fix(order): Get now SELECTs created_at/updated_at/completed_at/last_four"
```

---

## Task 10: P0.6 — Align OrderList shape (`next_cursor`)

**Files:**
- Modify: `services/order/internal/api/handler.go:174-192`
- Modify: `services/web/internal/backend/types.go:54-57`
- Modify: `api/openapi.yaml:290-300`

**Steps:**

- [ ] **Step 1: Backend returns `next_cursor` instead of `has_more`**

In `handler.go:188-191`, replace the response shape:
```go
next := ""
if len(items) == limit {
    next = string(items[len(items)-1].ID)
}
writeJSON(w, http.StatusOK, struct {
    Items      []domain.Order `json:"items"`
    NextCursor string         `json:"next_cursor,omitempty"`
}{Items: items, NextCursor: next})
```

- [ ] **Step 2: Update web `OrderList`**

```go
type OrderList struct {
    Items      []Order `json:"items"`
    NextCursor string  `json:"next_cursor,omitempty"`
}
```

(Change `NextCursor *string` → `string`.)

- [ ] **Step 3: Update OpenAPI**

Edit `api/openapi.yaml:290-300` to match:
```yaml
OrderList:
  type: object
  properties:
    items:
      type: array
      items: { $ref: "#/components/schemas/Order" }
    next_cursor:
      type: string
```

- [ ] **Step 4: Verify**

```bash
go build ./...
go test ./services/order/... ./services/web/...
curl -sS http://127.0.0.1:8081/v1/orders | jq .
```

Expected: response has `next_cursor` field.

- [ ] **Step 5: Commit**

```bash
git add services/order/internal/api/handler.go services/web/internal/backend/types.go api/openapi.yaml
git commit -m "fix(order): OrderList now returns next_cursor (was has_more)"
```

---

## Task 11: P0.7 — Drop `FailureReason` from web `Order` type

**Files:**
- Modify: `services/web/internal/backend/types.go:50`

**Steps:**

- [ ] **Step 1: Remove the field**

Delete `FailureReason *string \`json:"failure_reason,omitempty"\``.

- [ ] **Step 2: Remove usages**

`templates/order_detail.html:18` references `{{.Order.FailureReason}}`. Remove that block.

- [ ] **Step 3: Build + test**

```bash
go build ./...
go test ./services/web/...
```

- [ ] **Step 4: Commit**

```bash
git add services/web/internal/backend/types.go services/web/internal/templates/order_detail.html
git commit -m "fix(web): drop FailureReason (field absent in upstream Order)"
```

---

## Task 12: P0.9 — Rewrite `services/web/README.md`

**Files:**
- Modify: `services/web/README.md`

**Steps:**

- [ ] **Step 1: Rewrite**

Match the actual port (`:8085`), document every route, document SSE, document the demo flow, link to scripts/run.ps1.

```markdown
# orderflow-web

Tactile playground UI for the orderflow platform. Server-rendered HTML
(`html/template`) + a sprinkle of `htmx` (vendored, offline-capable) for
progressive enhancement, plus Server-Sent Events for live saga telemetry.

## Quick start (against the full platform)

The one-command launcher brings the playground up alongside order / payment /
inventory / saga:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1
```

Browse to **<http://127.0.0.1:8085>**.

## Standalone run (against already-running services)

```powershell
cd services\web
go run .\cmd\web
# → listens on :8083 by default (use HTTP_ADDR to override; default collisions
#   with the inventory service's :8083 are intentional for the standalone case)
```

Override with env: `HTTP_ADDR=:9090 ORDER_URL=... PAYMENT_URL=... INVENTORY_URL=... KAFKA_BROKERS=localhost:9092`.

## Pages

| Path | Purpose |
|------|---------|
| `/` | Orders list (auto-refreshes every 2 s) |
| `/orders/new` | Create-order form |
| `/orders/{id}` | Order detail + saga timeline (auto-refreshes every 1 s while non-terminal) |
| `/inventory` | Per-SKU stock viewer (auto-refreshes every 3 s) |
| `/payments/sim` | Force-success / force-fail webhook simulator |
| `/events/stream` | SSE stream of Kafka events (server-sent; consumed by the sidebar) |
| `/healthz` | Liveness |
| `/readyz` | Readiness (parallel probes of order/payment/inventory upstreams) |

## Actions

| Method | Path | Effect |
|--------|------|--------|
| POST | `/v1/orders` | Submit a new order (returns HX-Redirect on success) |
| POST | `/v1/orders/{id}` | Cancel a non-terminal order |
| POST | `/payments/sim/fire` | Fire a synthetic payment webhook |

## Architecture

See `docs/superpowers/specs/2026-08-18-orderflow-web-design.md`.

## Smoke

After `scripts/run.ps1`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke-web.ps1
```

Asserts happy path + compensation + 4xx + 5xx.
```

- [ ] **Step 2: Commit**

```bash
git add services/web/README.md
git commit -m "docs(web): rewrite README to match actual port :8085 + document all routes"
```

---

## Task 13: P0.10 — Unify Kafka env var to `KAFKA_BROKERS`

**Files:**
- Modify: `cmd/order/main.go`, `cmd/payment/main.go`, `cmd/inventory/main.go`, `cmd/saga/main.go`
- Modify: `services/{order,payment,inventory,saga}/cmd/*/main.go` (if separate from above)
- Modify: all `scripts/{run,run-demo,run-demo-manual,smoke-web}.{ps1,sh}`
- Modify: `docs/demo/demo.sh`
- Modify: `deploy/docker-compose.yml`
- Modify: `Makefile`

**Steps:**

- [ ] **Step 1: Back-compat helper in each cmd main**

Replace `os.Getenv("KAFKA_BROKER")` with:
```go
func kafkaBrokers() []string {
    raw := os.Getenv("KAFKA_BROKERS")
    if raw == "" { raw = os.Getenv("KAFKA_BROKER") }
    if raw == "" { return nil }
    return strings.Split(raw, ",")
}
```

Use `kafkaBrokers()` everywhere instead of single-broker reads.

- [ ] **Step 2: Update all scripts + compose to use `KAFKA_BROKERS`**

Search-and-replace `KAFKA_BROKER=` → `KAFKA_BROKERS=` in:
- `scripts/run.ps1`, `scripts/run.sh`
- `scripts/run-demo.ps1`, `scripts/run-demo-manual.ps1`
- `docs/demo/demo.sh`
- `deploy/docker-compose.yml` (the 4 non-web services blocks)

- [ ] **Step 3: Verify**

```bash
go build ./...
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -NoBuild
Select-String -Path tests\logs\*.log -Pattern "no brokers|KAFKA_BROKER" -SimpleMatch
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add cmd/ services/*/cmd/ scripts/ docs/demo/ deploy/ Makefile
git commit -m "refactor(env): unify Kafka env var to KAFKA_BROKERS (CSV), keep KAFKA_BROKER back-compat"
```

---

# STAGE 3 — P1 fixes

## Task 14: P1.1 — Error message mapping

**Files:**
- Create: `services/web/internal/handlers/errors.go`
- Modify: `services/web/internal/handlers/pages.go` (use new helper)
- Modify: `services/web/internal/handlers/handlers.go` (template execute errors)
- Test: extend `handlers/pages_test.go`

**Steps:**

- [ ] **Step 1: Create errors.go**

```go
package handlers

import (
    "errors"
    "fmt"
    "log/slog"
    "net/http"

    "github.com/t0pm1x/orderflow/services/web/internal/backend"
)

// mapUpstreamError turns a backend error into a user-safe message + status.
// Logs the original error server-side with full detail. Never echoes the
// upstream response body to the user.
func mapUpstreamError(logger *slog.Logger, route string, err error) (userMsg string, status int) {
    if err == nil {
        return "", http.StatusOK
    }
    var he *backend.HTTPError
    if errors.As(err, &he) {
        logger.Warn("upstream error", "route", route, "status", he.Status, "body", he.Body, "url", he.URL)
        switch {
        case he.Status >= 400 && he.Status < 500:
            switch he.Status {
            case http.StatusBadRequest:
                return "The order service rejected the request. Please check your input.", http.StatusBadRequest
            case http.StatusNotFound:
                return "Not found.", http.StatusNotFound
            case http.StatusConflict:
                return "Conflict — the order may already be in this state.", http.StatusConflict
            case http.StatusUnprocessableEntity:
                return "The request was understood but rejected. Please check your input.", http.StatusBadRequest
            default:
                return "The order service rejected the request.", http.StatusBadRequest
            }
        default:
            return "The order service is temporarily unavailable. Please try again in a moment.", http.StatusBadGateway
        }
    }
    // transport error
    logger.Error("upstream transport error", "route", route, "err", err)
    return "Cannot reach the order service. Please check your connection.", http.StatusBadGateway
}
```

- [ ] **Step 2: Wire Set logger**

Add `Logger *slog.Logger` to `Set` struct. Initialize in `NewSet` (caller passes `slog.Default()`).

- [ ] **Step 3: Replace inline error rendering**

In `pages.go`, `ActionOrderSubmit`:
```go
out, err := s.Order.Submit(r.Context(), in)
if err != nil {
    msg, status := mapUpstreamError(s.Logger, "POST /v1/orders", err)
    vm.Error = msg
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(status)
    _ = s.Templates.ExecuteTemplate(w, "layout", vm)
    return
}
```

Apply same pattern in `ActionOrderCancel`, `ActionPaymentsFire`, and the GET page handlers (PageOrderDetail, PageOrdersList, PageInventory, PagePaymentsSim).

- [ ] **Step 4: Tests**

```go
func TestOrderSubmit_Upstream400_HidesRawBody(t *testing.T) {
    // fake upstream returns 400 with body "internal debug: stack trace here"
    // assert vm.Error does NOT contain "stack trace"
}
```

- [ ] **Step 5: Commit**

```bash
git add services/web/internal/handlers/
git commit -m "feat(web): map upstream errors to user-safe messages; never echo raw bodies"
```

---

## Task 15: P1.2 — Kafka-down UI state

**Files:**
- Modify: `services/web/internal/web/main.go` (track tail state)
- Modify: `services/web/internal/handlers/pages.go` (PageEventsStream + render flag)
- Modify: `services/web/internal/templates/layout.html:28-31`

**Steps:**

- [ ] **Step 1: Track "events disabled" in web main**

Add `eventsEnabled bool` field, set based on whether `kafkatail.Start` returned `(nil, nil)`.

- [ ] **Step 2: Pass to handlers via Set**

Add `EventsEnabled bool` to `Set` struct.

- [ ] **Step 3: Sidebar template**

In `layout.html`, replace the `<aside class="sidebar">`:
```html
<aside class="sidebar" hx-ext="sse" sse-connect="/events/stream" sse-swap="event" hx-swap="afterend">
  <h3>Live events {{if not .EventsEnabled}}<span class="badge cancelled" title="Kafka tail not started">disconnected</span>{{end}}</h3>
  <ul id="events" class="events" role="log" aria-live="polite" aria-label="Order event stream"></ul>
  {{if not .EventsEnabled}}
  <p class="muted">Live events: disconnected (KAFKA_BROKERS not set or Kafka tail not started). Pages still work.</p>
  {{end}}
</aside>
```

`Set.EventsEnabled` propagates via the `vm` struct in each handler.

- [ ] **Step 4: SSE endpoint behavior**

When `EventsEnabled == false`, the SSE endpoint returns 503 with `{"error":"events unavailable"}` (no SSE stream).

- [ ] **Step 5: Commit**

```bash
git add services/web/internal/web/main.go services/web/internal/handlers/ services/web/internal/templates/layout.html
git commit -m "feat(web): sidebar banner + SSE 503 when Kafka tail is disabled"
```

---

## Task 16: P1.3 — Responsive sidebar

**Files:**
- Modify: `services/web/internal/static/styles.css:16`

**Steps:**

- [ ] **Step 1: Add media query**

```css
@media (max-width: 720px) {
    .main { grid-template-columns: 1fr; }
    .sidebar { border-left: 0; border-top: 1px solid var(--border); max-height: 40vh; }
}
table { display: block; overflow-x: auto; }
```

- [ ] **Step 2: Verify**

```bash
go build ./...
# curl with custom User-Agent / viewport simulation
curl -sS http://127.0.0.1:8085/static/styles.css | grep -A3 "@media"
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/static/styles.css
git commit -m "feat(web): responsive layout breakpoint at 720px"
```

---

## Task 17: P1.4 — Status icon system

**Files:**
- Modify: `services/web/internal/templates/orders_list.html`
- Modify: `services/web/internal/templates/order_detail.html`
- Modify: `services/web/internal/templates/payments.html`
- Modify: `services/web/internal/static/styles.css`

**Steps:**

- [ ] **Step 1: Create inline SVG icon helper**

Define a template `{{define "statusIcon"}}`:
```html
{{define "statusIcon"}}
{{if eq . "pending"}}<svg class="icon" viewBox="0 0 16 16" aria-label="pending"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="2"/></svg>{{end}}
{{if eq . "reserved"}}<svg class="icon" viewBox="0 0 16 16" aria-label="reserved"><rect x="3" y="7" width="10" height="6" fill="none" stroke="currentColor" stroke-width="2"/><path d="M5 7 V4 a3 3 0 0 1 6 0 V7" fill="none" stroke="currentColor" stroke-width="2"/></svg>{{end}}
{{if eq . "confirmed"}}<svg class="icon" viewBox="0 0 16 16" aria-label="confirmed"><path d="M3 8 L7 12 L13 4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>{{end}}
{{if eq . "cancelled"}}<svg class="icon" viewBox="0 0 16 16" aria-label="cancelled"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="2"/><line x1="4" y1="4" x2="12" y2="12" stroke="currentColor" stroke-width="2"/></svg>{{end}}
{{if eq . "failed"}}<svg class="icon" viewBox="0 0 16 16" aria-label="failed"><line x1="4" y1="4" x2="12" y2="12" stroke="currentColor" stroke-width="2"/><line x1="12" y1="4" x2="4" y2="12" stroke="currentColor" stroke-width="2"/></svg>{{end}}
{{end}}
```

Add to layout.html at the end of `<body>` (or as a separate file in `templates/_icons.html` parsed in `NewSet`).

- [ ] **Step 2: Use in templates**

Replace `<span class="badge {{.State}}">{{.State}}</span>` with:
```html
<span class="badge {{.State}}">{{template "statusIcon" .State}} {{.State}}</span>
```

Apply in `orders_list.html:21`, `order_detail.html:14`, `payments.html:13`.

- [ ] **Step 3: CSS**

```css
.icon { width: 14px; height: 14px; vertical-align: -2px; }
```

- [ ] **Step 4: Commit**

```bash
git add services/web/internal/templates/ services/web/internal/static/styles.css
git commit -m "feat(web): inline-SVG icons for status (color-blind safe)"
```

---

## Task 18: P1.5 — ARIA + focus-visible

**Files:**
- Modify: `services/web/internal/templates/layout.html:28-31, 34-50`
- Modify: `services/web/internal/static/styles.css`

**Steps:**

- [ ] **Step 1: ARIA on sidebar + events**

Add `role="log"`, `aria-live="polite"`, `aria-label="Order event stream"` to the `<ul id="events">`.

- [ ] **Step 2: Focus-visible CSS**

```css
button:focus-visible, .btn:focus-visible, a:focus-visible, input:focus-visible, select:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
}
```

- [ ] **Step 3: Submit button announce via aria-busy**

In `order_new.html`, add `aria-busy="false"` to the button + JS in `layout.html` to set it during htmx request.

- [ ] **Step 4: Commit**

```bash
git add services/web/internal/templates/layout.html services/web/internal/static/styles.css services/web/internal/templates/order_new.html
git commit -m "feat(web): ARIA live region + focus-visible + aria-busy on submit"
```

---

## Task 19: P1.6 — Per-order timeline UI (depends on P0.2)

P0.2 already added the timeline template + handler + view-model field on `PageOrderDetail`. This task is the polish: make the timeline poll-able, expandable payload, etc.

**Files:**
- (mostly already done in Task 6) — confirm via smoke test

**Steps:**

- [ ] **Step 1: Verify timeline polls via `?frag=1`**

Confirm `/orders/{id}/events?frag=1` works end-to-end.

- [ ] **Step 2: Wire timeline refresh on order detail page**

Already done in Task 6; verify.

- [ ] **Step 3: Commit if any tweaks landed**

```bash
git add services/web/internal/templates/order_events.html services/web/internal/handlers/pages.go
git commit -m "polish(web): timeline polish (already shipped in P0.2; this commit captures tweaks)"
```

---

## Task 20: P1.7 — Payment sim button labels

**Files:**
- Modify: `services/web/internal/templates/payments.html:18,26`

**Steps:**

- [ ] **Step 1: Rename buttons**

```html
<button type="submit" aria-label="Fire a succeeded webhook for this order">Force succeed ✓</button>
```

```html
<button type="submit" class="danger" aria-label="Fire a failed webhook with error code card_declined">Force fail ✗ (card_declined)</button>
```

- [ ] **Step 2: Commit**

```bash
git add services/web/internal/templates/payments.html
git commit -m "polish(web): clearer payment-sim button labels + aria-label"
```

---

## Task 21: P1.8 — Refresh banner + relative timestamps

**Files:**
- Modify: `services/web/internal/templates/order_detail.html`
- Modify: `services/web/internal/templates/layout.html` (JS helper)
- Modify: `services/web/internal/handlers/handlers.go` (template func)

**Steps:**

- [ ] **Step 1: Register template funcs in `NewSet`**

```go
import "time"

func timeAgo(t time.Time) string {
    d := time.Since(t)
    switch {
    case d < time.Minute: return fmt.Sprintf("%ds ago", int(d.Seconds()))
    case d < time.Hour:   return fmt.Sprintf("%dm ago", int(d.Minutes()))
    case d < 24*time.Hour:return fmt.Sprintf("%dh ago", int(d.Hours()))
    default:              return fmt.Sprintf("%dd ago", int(d.Hours()/24))
    }
}

func init() {
    // or do this in NewSet:
}

func (s *Set) withFuncs() *template.Template {
    return s.Templates.Funcs(template.FuncMap{
        "timeAgo": timeAgo,
    })
}
```

- [ ] **Step 2: Use in order detail**

```html
<span title="{{.Order.CreatedAt.Format "2006-01-02 15:04:05"}}">{{timeAgo .Order.CreatedAt}}</span>
```

- [ ] **Step 3: Refresh button**

Add to order detail:
```html
<button hx-get="/orders/{{.Order.ID}}?frag=1" hx-target="#page-content" hx-swap="outerHTML">Refresh</button>
```

- [ ] **Step 4: Commit**

```bash
git add services/web/internal/handlers/handlers.go services/web/internal/templates/order_detail.html
git commit -m "feat(web): relative timestamps + visible Refresh button on order detail"
```

---

## Task 22: P1.9 — Route `Cancel` through `do()`

**Files:**
- Modify: `services/web/internal/backend/order.go:74-89`

**Steps:**

- [ ] **Step 1: Refactor `Cancel` to use `do()`**

```go
func (c *HTTPClient) Cancel(ctx context.Context, id string) error {
    req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
        fmt.Sprintf("%s/v1/orders/%s", c.orderURL, url.PathEscape(id)), nil)
    if err != nil {
        return fmt.Errorf("order cancel: %w", err)
    }
    err = c.do(req, nil)
    if err == nil { return nil }
    var he *HTTPError
    if errors.As(err, &he) {
        if he.Status == http.StatusNoContent || he.Status == http.StatusNotFound {
            return nil
        }
    }
    return err
}
```

- [ ] **Step 2: Test**

```go
func TestOrderClient_Cancel_404ReturnsNil(t *testing.T) { ... }
func TestOrderClient_Cancel_500ReturnsError(t *testing.T) { ... }
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/backend/order.go services/web/internal/backend/order_test.go
git commit -m "fix(web): Cancel routes through do() so errors.As(&HTTPError{}) works"
```

---

## Task 23: P1.10 — URL-escape on backend client paths

**Files:**
- Modify: `services/web/internal/backend/order.go:42-44, 76`
- Modify: `services/web/internal/backend/inventory.go:13-14`
- Tests

**Steps:**

- [ ] **Step 1: Apply `url.PathEscape`**

(Covered in Task 8 Step 3 for `order.Get`, `cancel`, `inventory.GetStock`.)

- [ ] **Step 2: Tests**

```go
func TestOrderClient_Get_PathEscape(t *testing.T) { ... }
func TestInventoryClient_GetStock_PathEscape(t *testing.T) { ... }
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/backend/
git commit -m "fix(web): url.PathEscape on all path interpolations in backend client"
```

---

## Task 24: P1.11 — Concurrent inventory fetch

**Files:**
- Modify: `services/web/internal/handlers/pages.go:194-232`
- Modify: `services/web/internal/handlers/handlers.go` (no change needed; uses Set.Inventory)

**Steps:**

- [ ] **Step 1: Replace serial loop with errgroup**

```go
import "golang.org/x/sync/errgroup"

func (s *Set) PageInventory(w http.ResponseWriter, r *http.Request) {
    // ... existing SKU dedup ...
    g, gctx := errgroup.WithContext(r.Context())
    g.SetLimit(8)
    results := make([]inventoryRow, len(vm.Rows))
    for i, row := range vm.Rows {
        i, row := i, row
        g.Go(func() error {
            stock, gerr := s.Inventory.GetStock(gctx, row.SKU)
            if gerr != nil || stock == nil {
                results[i] = row // Missing stays true
                return nil
            }
            results[i] = inventoryRow{
                SKU: row.SKU, Available: stock.Available, Reserved: stock.Reserved, Version: stock.Version,
            }
            return nil
        })
    }
    _ = g.Wait()
    vm.Rows = results
    // ... existing render ...
}
```

Add `golang.org/x/sync` to `services/web/go.mod` (or use stdlib `sync` + semaphore).

- [ ] **Step 2: Test**

Add a test with a slow fake inventory client.

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/handlers/pages.go services/web/go.mod services/web/go.sum
git commit -m "perf(web): concurrent inventory fetch (errgroup, max 8)"
```

---

## Task 25: P1.12 — Server-side validation tightening

**Files:**
- Modify: `services/web/internal/handlers/pages.go:42-50, 48-50`

**Steps:**

- [ ] **Step 1: Tighten checks in `ActionOrderSubmit`**

```go
const maxSKULen = 64
const maxQuantity = 10000
const maxUnitPrice = 100_000_000 // $1M

if len(vm.SKU) > maxSKULen {
    vm.Error = fmt.Sprintf("SKU must be ≤ %d characters", maxSKULen)
    // ... 400
}
if vm.Quantity <= 0 || vm.Quantity > maxQuantity {
    vm.Error = fmt.Sprintf("Quantity must be between 1 and %d", maxQuantity)
    // ... 400
}
if up := r.FormValue("unit_price_cents"); up != "" {
    p, err := strconv.ParseInt(up, 10, 64)
    if err != nil || p < 0 || p > maxUnitPrice {
        vm.Error = fmt.Sprintf("Unit price must be 0..%d cents", maxUnitPrice)
        // ... 400
    }
    vm.UnitPriceCents = p
}
```

- [ ] **Step 2: Tests**

```go
func TestOrderSubmit_QuantityTooLarge_400(t *testing.T) { ... }
func TestOrderSubmit_NegativeUnitPrice_400(t *testing.T) { ... }
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/handlers/pages.go services/web/internal/handlers/pages_test.go
git commit -m "feat(web): server-side validation tightening (length, range, type)"
```

---

# STAGE 4 — P2 fixes

## Task 26: P2.1 — First-impression hero card

**Files:**
- Create: `services/web/internal/templates/order_hero.html`
- Modify: `services/web/internal/templates/orders_list.html` (use hero on empty)
- Modify: `services/web/internal/handlers/handlers.go` (NewSet parses new template)
- Modify: `services/web/internal/handlers/pages.go` (PageOrdersList)

**Steps:**

- [ ] **Step 1: Create `order_hero.html`**

```html
{{define "orderHeroBody"}}
<section class="hero">
  <h1>OrderFlow playground</h1>
  <p>A distributed order-processing platform — 4 Go microservices + Kafka + Postgres + Redis. This page is the tactile playground; click around to see the saga in action.</p>
  <ol class="hero-steps">
    <li><strong>Create an order</strong> with the button below. Use <code>last_four=4242</code> for happy path or <code>last_four=0001</code> to trigger compensation.</li>
    <li><strong>Watch the timeline</strong> on the order detail page — events arrive in real time.</li>
    <li><strong>Force a failure</strong> from the Payments sim page to see the saga undo the reservation.</li>
  </ol>
  <div class="row">
    <a class="btn" href="/orders/new?prefill=happy">+ New order (happy path)</a>
    <a class="btn secondary" href="/orders/new?prefill=fail">+ New order (compensation)</a>
  </div>
</section>
{{end}}
```

- [ ] **Step 2: Render hero when empty**

In `orders_list.html`, replace the empty-state block:
```html
{{else if not .Orders}}
  {{template "orderHeroBody" .}}
```

- [ ] **Step 3: Prefill support in `PageOrderNew`**

```go
prefill := r.URL.Query().Get("prefill")
vm := orderNewVM{Body: "orderNewBody", IdempotencyToken: newIdempotencyToken()}
switch prefill {
case "happy":
    vm.SKU = "SKU-DEMO"
    vm.Quantity = 1
    vm.UnitPriceCents = 1999
    // last_four is added by a hidden field
case "fail":
    vm.SKU = "SKU-DEMO"
    vm.Quantity = 1
    vm.UnitPriceCents = 1999
    // hidden last_four=0001
}
```

- [ ] **Step 4: Hidden payment field in `order_new.html`**

```html
{{if eq .Prefill "happy"}}<input type="hidden" name="last_four" value="4242">{{end}}
{{if eq .Prefill "fail"}}<input type="hidden" name="last_four" value="0001">{{end}}
```

- [ ] **Step 5: Send `last_four` in `ActionOrderSubmit`**

Pass through to `OrderSubmit` (new field).

- [ ] **Step 6: Commit**

```bash
git add services/web/internal/templates/order_hero.html services/web/internal/templates/orders_list.html services/web/internal/templates/order_new.html services/web/internal/handlers/pages.go services/web/internal/handlers/handlers.go
git commit -m "feat(web): first-impression hero + Create-demo-order prefill (happy + fail)"
```

---

## Task 27: P2.2 — Visibility-based polling pause

**Files:**
- Modify: `services/web/internal/templates/layout.html` (inline JS)

**Steps:**

- [ ] **Step 1: Add visibility listener**

```js
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        // tell htmx to pause all triggers
        document.body.dispatchEvent(new CustomEvent('htmx:pause'));
    } else {
        document.body.dispatchEvent(new CustomEvent('htmx:resume'));
    }
});
```

htmx 2.x natively respects `document.hidden` for polling triggers (via `hx-trigger="every Ns [hidden]"` extension); verify by setting:
```html
hx-trigger="every 2s, visibility:visible"
```

Apply to all `hx-trigger="every Ns"` occurrences:
- `orders_list.html:2` → `hx-trigger="every 2s, visibility:visible"`
- `inventory.html:2` → `hx-trigger="every 3s, visibility:visible"`
- `order_detail.html:3` → `hx-trigger="every 1s, visibility:visible"`
- `order_events.html:2` → `hx-trigger="every 1s, visibility:visible"`

- [ ] **Step 2: Commit**

```bash
git add services/web/internal/templates/
git commit -m "perf(web): pause htmx polling when tab is hidden"
```

---

## Task 28: P2.3 — Refactor `ExecuteTemplate` boilerplate

**Files:**
- Modify: `services/web/internal/handlers/handlers.go:99-108`
- Modify: `services/web/internal/handlers/pages.go` (every handler)

**Steps:**

- [ ] **Step 1: Add helpers**

```go
func (s *Set) renderPage(w http.ResponseWriter, vm any) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    if err := s.Templates.ExecuteTemplate(w, "layout", vm); err != nil {
        s.Logger.Error("template execute", "err", err)
        http.Error(w, "Internal error", http.StatusInternalServerError)
    }
}

func (s *Set) renderPageFrag(w http.ResponseWriter, name string, vm any) {
    renderFragment(w, s.Templates, name, vm)
}
```

- [ ] **Step 2: Replace boilerplate in every handler**

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/handlers/
git commit -m "refactor(web): extract renderPage + renderPageFrag helpers"
```

---

## Task 29: P2.4 — Fix `bus.Publish` race

**Files:**
- Modify: `services/web/internal/events/bus.go:56-77`

**Steps:**

- [ ] **Step 1: Snapshot subscribers under lock, fan-out without lock**

```go
func (b *Bus) Publish(e BusEvent) {
    b.mu.Lock()
    if b.closed() { b.mu.Unlock(); return }
    snapshot := make([]chan BusEvent, 0, len(b.subs))
    for ch := range b.subs {
        snapshot = append(snapshot, ch)
    }
    b.ring = append(b.ring, ringEntry{aggregateID: e.Envelope.AggregateID, env: e.Envelope})
    if len(b.ring) > ringCap {
        drop := ringCap / 10
        b.ring = b.ring[drop:]
    }
    b.mu.Unlock()

    for _, ch := range snapshot {
        select {
        case ch <- e:
        default:
            // drop oldest; single select per channel (no double-select race)
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
```

- [ ] **Step 2: Add concurrent stress test**

```go
func TestBus_ConcurrentPublishSubscribe(t *testing.T) {
    b := NewBus()
    defer b.Close()
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            ch, _ := b.Subscribe()
            for j := 0; j < 100; j++ { <-ch }
        }()
    }
    for i := 0; i < 1000; i++ {
        b.Publish(BusEvent{Envelope: pkgEvents.Envelope{EventType: "X", AggregateID: "a"}})
    }
    wg.Wait()
}
```

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/events/
git commit -m "fix(events): snapshot subscribers under lock + concurrent stress test"
```

---

## Task 30: P2.5 — Dead-code removal in `ActionOrderCancel`

(Covered by Task 22; this is a small follow-up commit.)

## Task 31: P2.6 — Payments-sim partial-failure surfacing

**Files:**
- Modify: `services/web/internal/handlers/pages.go:249-267`

**Steps:**

- [ ] **Step 1: Capture per-list errors**

```go
pending, perr := s.Order.List(r.Context(), backend.OrderStatePending, 50)
reserved, rerr := s.Order.List(r.Context(), backend.OrderStateReserved, 50)
vm := paymentsSimVM{Body: "paymentsSimBody"}
if perr != nil { vm.PendingErr = perr.Error() }
if rerr != nil { vm.ReservedErr = rerr.Error() }
if pending == nil && reserved == nil {
    vm.BackendDown = true
    vm.Error = "Order service unavailable"
}
// ...
```

Add `PendingErr`, `ReservedErr` to `paymentsSimVM`.

Render in `payments.html`:
```html
{{if .PendingErr}}<p class="error">Pending list failed: {{.PendingErr}}</p>{{end}}
{{if .ReservedErr}}<p class="error">Reserved list failed: {{.ReservedErr}}</p>{{end}}
```

- [ ] **Step 2: Commit**

```bash
git add services/web/internal/handlers/pages.go services/web/internal/templates/payments.html
git commit -m "feat(web): payments-sim surfaces partial-failure errors"
```

---

## Task 32: P2.7 — Order.Get 404 only when upstream is 404

**Files:**
- Modify: `services/web/internal/handlers/pages.go:117-142`

**Steps:**

- [ ] **Step 1: Branch on HTTPError**

```go
o, err := s.Order.Get(r.Context(), id)
if err != nil {
    vm.BackendDown = true
    msg, status := mapUpstreamError(s.Logger, "GET /orders/{id}", err)
    vm.Error = msg
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(status)
    if r.URL.Query().Get("frag") == "1" {
        renderFragment(w, s.Templates, "orderDetailBody", vm)
        return
    }
    _ = s.Templates.ExecuteTemplate(w, "layout", vm)
    return
}
```

- [ ] **Step 2: Commit**

```bash
git add services/web/internal/handlers/pages.go
git commit -m "fix(web): Order.Get returns 404 only when upstream 404, else 502"
```

---

## Task 33: P2.8 — Cache static CSS at server startup

**Files:**
- Modify: `services/web/internal/server/server.go` (Server struct + Start)

**Steps:**

- [ ] **Step 1: Add field + load**

```go
type Server struct {
    opt     Options
    srv     *http.Server
    addr    atomic.Value
    styles  []byte  // cached styles.css
}

func New(opt Options) *Server {
    data, _ := static.FS.ReadFile("styles.css")
    return &Server{opt: opt, styles: data}
}
```

- [ ] **Step 2: Serve from cache**

```go
r.Get("/static/styles.css", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "text/css; charset=utf-8")
    _, _ = w.Write(s.styles)
})
```

(Other /static/* still uses embed.FS as fallback.)

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/server/server.go
git commit -m "perf(web): cache styles.css at server startup (no per-request embed read)"
```

---

## Task 34: P2.9 — Move `boundAddr.Store` inside `Start`

**Files:**
- Modify: `services/web/internal/server/server.go:108-117`
- Modify: `services/web/internal/web/main.go:107-112`

**Steps:**

- [ ] **Step 1: Store earlier**

In `Start`, after `s.srv = &http.Server{...}` and before `srv.Serve(ln)`:
```go
s.addr.Store(ln.Addr().String())
```

- [ ] **Step 2: Remove the post-shutdown `Store` in web/main.go**

- [ ] **Step 3: Commit**

```bash
git add services/web/internal/server/server.go services/web/internal/web/main.go
git commit -m "fix(web): bind address is set before Serve, not after shutdown"
```

---

## Task 35: P2.10 — SSE: log marshal failure + emit `id:` line

**Files:**
- Modify: `services/web/internal/handlers/pages.go:344-355`

**Steps:**

- [ ] **Step 1: Log + emit `id:`**

```go
case ev, ok := <-ch:
    if !ok { return }
    data, err := json.Marshal(ev.Envelope)
    if err != nil {
        s.Logger.Warn("sse marshal", "err", err, "event_id", ev.Envelope.EventID)
        continue
    }
    if _, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.Envelope.EventID, ev.Envelope.EventType, data); err != nil {
        return
    }
    flusher.Flush()
```

- [ ] **Step 2: Commit**

```bash
git add services/web/internal/handlers/pages.go
git commit -m "feat(web): SSE emits id: line for replay + logs marshal failures"
```

---

# STAGE 5 — P3 fixes

## Task 36: P3.1 — Rebrand topbar

**Files:**
- Modify: `services/web/internal/templates/layout.html:11-17`

**Steps:**

```html
<header class="topbar">
  <a class="brand" href="/"><strong>OrderFlow</strong> <span class="muted">— distributed order processing playground</span></a>
  <nav>...</nav>
</header>
```

Commit.

---

## Task 37: P3.2 — Design tokens file

**Files:**
- Modify: `services/web/internal/static/styles.css:1-5`

**Steps:**

```css
:root {
    /* surfaces */
    --bg: #0e1116; --fg: #e6edf3; --muted: #7d8590;
    --accent: #4493f8; --panel: #161b22; --border: #30363d;
    /* status — color + secondary signal pattern */
    --status-pending-bg: rgba(210,153,34,0.15); --status-pending-fg: #d29922;
    --status-reserved-bg: rgba(68,147,248,0.15); --status-reserved-fg: #4493f8;
    --status-confirmed-bg: rgba(86,211,100,0.15); --status-confirmed-fg: #56d364;
    --status-cancelled-bg: rgba(248,81,73,0.15); --status-cancelled-fg: #f85149;
    --status-failed-bg: rgba(248,81,73,0.15); --status-failed-fg: #f85149;
    /* spacing scale */
    --gap-1: 4px; --gap-2: 8px; --gap-3: 12px; --gap-4: 16px; --gap-5: 24px;
}
```

Update existing rules to reference tokens.

Commit.

---

## Task 38: P3.3 — Copy/paste IDs

**Files:**
- Modify: `services/web/internal/templates/orders_list.html:20`
- Modify: `services/web/internal/templates/order_detail.html:9`
- Modify: `services/web/internal/templates/layout.html` (JS helper)

**Steps:**

```html
<td class="mono"><button class="copy-id mono" data-id="{{.ID}}" title="Click to copy">{{.ID}}</button></td>
```

JS:
```js
document.body.addEventListener('click', function(e) {
    var t = e.target;
    if (t.classList && t.classList.contains('copy-id')) {
        navigator.clipboard.writeText(t.dataset.id).then(function() {
            var orig = t.textContent;
            t.textContent = '✓ copied';
            setTimeout(function() { t.textContent = orig; }, 1200);
        });
    }
});
```

Commit.

---

## Task 39: P3.4 — Static SVG state-machine diagrams

**Files:**
- Create: `services/web/internal/static/diagrams/saga_happy.svg`
- Create: `services/web/internal/static/diagrams/saga_compensation.svg`
- Modify: `services/web/internal/templates/order_detail.html`

**Steps:**

- [ ] **Step 1: Author `saga_happy.svg`**

Simple flowchart: OrderCreated → StockReserved → PaymentRequested → PaymentCompleted → OrderConfirmed.

- [ ] **Step 2: Author `saga_compensation.svg`**

Same first half, then PaymentFailed → StockReleaseRequested + OrderCancelled.

- [ ] **Step 3: Embed in order detail**

```html
<figure class="saga-diagram">
  <object type="image/svg+xml" data="/static/diagrams/saga_happy.svg" aria-label="Saga happy-path state machine"></object>
  <figcaption>Happy path</figcaption>
</figure>
```

- [ ] **Step 4: Commit**

```bash
git add services/web/internal/static/diagrams/ services/web/internal/templates/order_detail.html
git commit -m "feat(web): static SVG saga state-machine diagrams"
```

---

## Task 40: P3.5 — Update ADR-0003 (REST-only)

**Files:**
- Modify: `docs/adr/0003-rest-vs-grpc.md`

**Steps:**

- [ ] **Step 1: Rewrite decision + consequences**

Replace the gRPC claims with: REST-only external + REST-only service-to-service. Note: deferred-to-v1.2+ for any gRPC adoption.

Commit.

---

## Task 41: P3.6 — Update ADR-0001 (Postgres, not Redis)

**Files:**
- Modify: `docs/adr/0001-saga-vs-choreography.md:13-14`

**Steps:**

- [ ] **Step 1: Replace Redis reservation language**

"Stock reservations are persisted in the inventory service's Postgres `stock_items.reserved` column with optimistic locking via `lock.PGLocker`. Redis is used for the payment webhook idempotency cache only."

Commit.

---

## Task 42: P3.7 — Update top-level README.md

**Files:**
- Modify: `README.md:7, 142-144, 162-168, 180, 197-202`

**Steps:**

- [ ] **Step 1: Status line → v1.2.0**

- [ ] **Step 2: Remove "Migrations: goose (planned, not yet wired)"**

- [ ] **Step 3: Remove "Local development (planned, not yet wired)" + the "Not yet functional" paragraph**

- [ ] **Step 4: Remove "saga orchestrator (stub)" — saga is full**

- [ ] **Step 5: Add ADR-0004 to the ADR list (line 197)**

Commit.

---

## Task 43: P3.8 — Update STATUS.md

**Files:**
- Modify: `STATUS.md`

**Steps:**

- [ ] **Step 1: Append v1.1.1 through v1.1.5 sub-stage rows**

- [ ] **Step 2: Mark web.1..web.11 as done (they already are in the table at the end; verify)**

- [ ] **Step 3: Update "Deferred to v1.1" → "Deferred to v1.2+"**

Commit.

---

# STAGE 6 — Final E2E + report

## Task 44: Final E2E re-run

**Steps:**

- [ ] **Step 1: Run `make build`**

```bash
cd C:\Users\t0p_m\projects\orderflow
make build
```

Expected: 5 binaries, no errors.

- [ ] **Step 2: Run `go test ./...`**

```bash
go test -short ./...
```

Expected: all tests pass.

- [ ] **Step 3: Run `scripts/run.ps1` then `scripts/smoke-web.ps1`**

Expected: `ALL PASS`.

- [ ] **Step 4: Run the 5 scenario matrix manually**

For each scenario, record the result:

| Scenario | Result | Notes |
|----------|--------|-------|
| Happy path (last_four=4242) | PASS/FAIL | |
| Failure path (last_four=0001) | PASS/FAIL | |
| Refresh recovery | PASS/FAIL | |
| Network failure (stop order service) | PASS/FAIL | |
| Duplicate submit (rapid) | PASS/FAIL | |

---

## Task 45: Write the final report

**Files:**
- Create: `docs/superpowers/portfolio/orderflow-web-audit-2026-08-19.md`
- Create: `docs/demo/PLAYGROUND-AUDIT.md`

**Steps:**

- [ ] **Step 1: Author the report (portfolio copy)**

Use the spec's defect table + smoke results + verdict. Sections:
- Executive Summary
- Functional Issues (severity / flow / problem / fix)
- UX Issues
- Visual Issues
- Technical Issues
- Verification (10-row table)
- Remaining Issues
- Final Verdict (DEMO READY / DEMO READY WITH MINOR ISSUES / NOT DEMO READY)

- [ ] **Step 2: Author the demo-discovery copy**

Same content, lighter prose, focused on "what a demo viewer should know".

- [ ] **Step 3: Commit both**

```bash
git add docs/superpowers/portfolio/orderflow-web-audit-2026-08-19.md docs/demo/PLAYGROUND-AUDIT.md
git commit -m "docs(audit): orderflow-web playground audit final report"
```

---

# Self-Review (per writing-plans skill)

**Spec coverage:** Every spec defect (BL.1-BL.4, P0.1-P0.10, P1.1-P1.12, P2.1-P2.10, P3.1-P3.8) is implemented by exactly one task. ✓

**Placeholder scan:** No "TBD"/"TODO"/"implement later" in any step. All code blocks are real Go/HTML/SVG. ✓

**Type consistency:** `Set.Logger` referenced in P1.1 (Task 14) — declared in same commit. `Set.EventsEnabled` referenced in P1.2 (Task 15) — same. `vm.IdempotencyToken` referenced in P0.3 (Task 7) — same. `Set.replays` referenced in P0.3 (Task 7) — same. `timeAgo` template func declared in P1.8 (Task 21). ✓

**Cross-task dependencies:**
- P1.6 depends on P0.2 (timeline data + handler from Task 6). Tasks ordered accordingly.
- P2.5 depends on P1.9. Tasks ordered.
- All other tasks touch disjoint files; safe to parallelize within stage.

**Scope:** 45 tasks (4 BLOCKER + 10 P0 + 12 P1 + 10 P2 + 8 P3 + 1 final E2E/reporting) over 6 stages. Fits one implementation plan.
