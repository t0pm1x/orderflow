# Stage 3.13 — Docs + Demo + Asciinema + Sub-stages index

**Why:** README references `docs/superpowers/portfolio/orderflow-substages.md` (does not exist), `docs/demo/` is empty, no C4 diagram for saga, no ADR for the W3C tracecontext decision from 3.10. We need polished, linkable artifacts for the v0.2.0 release.

**Depends on:** Stages 3.10 and 3.12 done (so ADR-0004 and demo screenshots reference real artifacts).

### Task 3.13.a — ADR-0004 (W3C tracecontext) + ADR log index (PAR with 3.13.b, 3.13.c, 3.13.e)

**Files:**
- Create: `docs/adr/0004-w3c-tracecontext.md`
- Modify: `README.md` (add ADR list section referencing 0004)

**Interfaces:**
- Same format as `docs/adr/0001-saga-vs-choreography.md` (Status / Date / Context / Decision / Alternatives / Consequences / References).

- [ ] **Step 1: Write ADR**

Use the existing ADR template. Key sections:
- **Context**: cross-service trace correlation through Kafka was missing in v0.1.0-MVP (see `pkg/outbox/kafka.go:71` TODO).
- **Decision**: adopt W3C tracecontext (`traceparent`/`tracestate`) via the global `otel.GetTextMapPropagator()`. Store as `Envelope.TraceID/SpanID` JSON fields AND as Kafka record headers for cross-language consumers.
- **Alternatives**: B. OpenTelemetry-only (no W3C) — rejected; C. Custom header `x-orderflow-trace-id` — rejected for non-W3C compliance.
- **Consequences**: every consumer must be OTel-aware; legacy consumers must ignore the new fields.

- [ ] **Step 2: ADR log index in README**

Append a section after the existing `## ADRs` block:
```markdown
### Decision log

| # | Title | Status | Date |
|---|-------|--------|------|
| 0001 | Saga vs choreography | Accepted | 2026-08-17 |
| 0002 | Outbox pattern | Accepted | 2026-08-17 |
| 0003 | REST vs gRPC | Accepted | 2026-08-17 |
| 0004 | W3C tracecontext | Accepted | 2026-08-17 |
```

- [ ] **Step 3: Commit**

```powershell
git add docs/adr/0004-w3c-tracecontext.md README.md
git commit -m "orderflow/3.13.a: ADR-0004 W3C tracecontext + decision log index"
```

### Task 3.13.b — c4-level-3-saga.puml (PAR)

**Files:**
- Create: `docs/architecture/c4-level-3-saga.puml`

**Interfaces:**
- Mirrors the structure of `docs/architecture/c4-level-3-order.puml` but for saga.

- [ ] **Step 1: Diagram**

Components:
- `saga-orchestrator` container
  - `state-machine` (initiated → stock_reserved → completed/compensated)
  - `compensators` (ReleaseStock, RefundPayment)
  - `watchdog` (in-memory + TTL row sweep)
  - `consumer` (subscribes to OrderCreated, StockReserved, PaymentCompleted, PaymentFailed)
  - `outbox-publisher`

- [ ] **Step 2: Render and verify**

```powershell
plantuml docs/architecture/c4-level-3-saga.puml -tpng
```
Output: `docs/architecture/c4-level-3-saga.png`. Confirm visually that containers match order/payment/inventory diagrams in shape.

- [ ] **Step 3: Commit**

```powershell
git add docs/architecture/c4-level-3-saga.puml docs/architecture/c4-level-3-saga.png
git commit -m "orderflow/3.13.b: C4 component diagram for Saga orchestrator"
```

### Task 3.13.c — Demo script (PAR with 3.13.a, 3.13.b, 3.13.e)

**Files:**
- Create: `docs/demo/demo.sh` (POSIX; runnable via Git Bash on Windows or WSL)
- Create: `docs/demo/README.md` (instructions for the demo)

**Interfaces:**
- Script brings up docker-compose, runs the 4 services in background, POSTs a happy-path order, asserts `confirmed`, kills redpanda, recovers, asserts eventual confirmation of a delayed order, then tears down.

- [ ] **Step 1: Write `demo.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

cleanup() { docker compose -f deploy/docker-compose.yml down -v || true; }
trap cleanup EXIT

echo "==> starting infrastructure"
docker compose -f deploy/docker-compose.yml up -d
sleep 30

echo "==> building binaries"
make build

echo "==> starting services"
make run-order    &> /tmp/order.log    &
make run-payment  &> /tmp/payment.log  &
make run-inventory&> /tmp/inventory.log&
make run-saga     &> /tmp/saga.log     &
sleep 10

echo "==> happy-path POST /v1/orders"
ORDER_ID=$(curl -s -X POST http://localhost:8081/v1/orders \
    -H 'Content-Type: application/json' \
    -d @examples/order.json | jq -r .id)
echo "order: $ORDER_ID"
sleep 5
curl -s http://localhost:8081/v1/orders/$ORDER_ID | jq .

echo "==> verifying confirmed state"
for i in $(seq 1 30); do
    STATE=$(curl -s http://localhost:8081/v1/orders/$ORDER_ID | jq -r .state)
    [[ "$STATE" == "confirmed" ]] && { echo "OK"; exit 0; }
    sleep 1
done
echo "FAIL: order did not reach confirmed"
exit 1
```

- [ ] **Step 2: Demo README**

```markdown
# orderflow Demo

Runs the happy-path scenario end-to-end against the local docker-compose stack.

## Prerequisites
- Docker (running)
- `make`, `curl`, `jq`
- 8GB RAM available

## Run

    ./docs/demo/demo.sh

Expected output: order reaches `confirmed` state within ~35s.

## What it covers
- docker-compose stack up (postgres x3, redis, redpanda, otel-collector, prometheus, tempo, grafana)
- 4 service binaries built and started in background
- POST /v1/orders with the example payload
- Polling until order state == confirmed
```

- [ ] **Step 3: Smoke test**

```bash
chmod +x docs/demo/demo.sh
./docs/demo/demo.sh
```
Expected: prints `OK` and exits 0.

- [ ] **Step 4: Commit**

```powershell
git add docs/demo
git commit -m "orderflow/3.13.c: end-to-end demo script + README"
```

### Task 3.13.d — Asciinema recording (SEQ; depends on 3.13.c)

**Files:**
- Create: `docs/demo/orderflow.cast`
- Modify: `README.md` (embed cast in "Demo" section)

**Interfaces:**
- `.cast` file is asciinema v2 format. Embed via `<script src="https://asciinema.org/a/<id>.js" async></script>` after upload, OR via `<img src="docs/demo/orderflow.svg">` for an SVG export.

- [ ] **Step 1: Install asciinema**

```powershell
winget install asciinema
```
Or download a single-binary release from `https://asciinema.org/` (Linux binary runs under WSL).

- [ ] **Step 2: Record**

```powershell
cd C:\Users\t0p_m\projects\orderflow
asciinema rec --command "bash docs/demo/demo.sh" --title "orderflow v0.2.0 happy path" --idle-time-limit 2 docs/demo/orderflow.cast
```

- [ ] **Step 3: Verify playback**

```powershell
asciinema play docs/demo/orderflow.cast
```

- [ ] **Step 4: Add README embed**

In `README.md`, after the "Building" section:
```markdown
## Demo

[![asciicast](https://asciinema.org/a/<id>.svg)](https://asciinema.org/a/<id>)

See [`docs/demo/orderflow.cast`](docs/demo/orderflow.cast) for the raw recording.
```
(Replace `<id>` after uploading to asciinema.org, OR keep the local SVG via `agg docs/demo/orderflow.cast docs/demo/orderflow.svg` if no upload.)

- [ ] **Step 5: Commit**

```powershell
git add docs/demo/orderflow.cast docs/demo/orderflow.svg README.md
git commit -m "orderflow/3.13.d: asciinema recording of happy-path demo"
```

### Task 3.13.e — Substages index doc (PAR)

**Why:** README §"Deferred to v0.2.0" links to `docs/superpowers/portfolio/orderflow-substages.md` — file does not exist (link is broken).

**Files:**
- Create: `docs/superpowers/portfolio/orderflow-substages.md`

**Interfaces:**
- Index of all 75 planned sub-stages with status. Mirrors STATUS.md but adds the full planned list (not just done).

- [ ] **Step 1: Generate**

Pull every `3.X.Y` identifier from `STATUS.md` plus the 20 deferred ones from the existing checkpoint, produce a 75-row table.

- [ ] **Step 2: Commit**

```powershell
git add docs/superpowers/portfolio/orderflow-substages.md
git commit -m "orderflow/3.13.e: sub-stages index doc (75 rows, fixes README broken link)"
```