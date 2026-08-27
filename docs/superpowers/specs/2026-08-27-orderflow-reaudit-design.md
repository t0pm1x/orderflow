# orderflow — v1.2 Senior-Go Adversarial Re-Audit

**Project:** orderflow
**Path:** `C:\Users\t0p_m\projects\orderflow`
**Date:** 2026-08-27
**Author:** Senior Go reviewer (build mode)
**Prior state:** v1.1.7 + completed FINAL_AUDIT (1487 lines, all P0–P3 = 0)
**Scope:** Full adversarial re-audit from scratch; previous findings treated as untrusted.

---

## 1. Purpose

The orderflow platform has been audited five times (v1.1.1 → v1.2.0). The
most recent audit (`audit/FINAL_AUDIT.md`, v1.2.0 final-validation pass) reports
"P0: 0, P1: 0, P2: 0, P3: 0; Overall: READY for tag as v1.2.0." However:

- The web service has been rewritten (htmx → SvelteKit SPA, commit `96755b3`)
  AFTER the v1.2 audit closed. Three files in `services/web/` are modified and
  uncommitted at session start.
- The audit's verifier (the AI engineer who wrote `FINAL_AUDIT.md`) admits in
  §8 (Self-Audit Note) that "the remaining P1+ findings are based on
  sub-agent reports with file:line evidence I did not personally re-read."
- One production-fatal gap was deliberately documented rather than fixed
  (ADR-0005: outbox "data-losing restart" on broker kill+restart).
- Several verification rows remain "NOT VERIFIED" (k8s, helm on real cluster,
  Grafana K8s manifest).

This spec commissions a **fresh, from-scratch adversarial re-audit** that does
NOT inherit prior findings. Each pass walks the source as if no prior audit
existed. New findings are classified P0–P3, fixed under TDD, and verified end
to end. The session ends with the platform in "maximally working" state:
green build/test/race/vet, both E2E tests passing under Docker, all
P0/P1 new findings fixed with regression tests, P2/P3 either fixed or
explicitly documented as TODO.

## 2. Goals

1. `make build` produces all 5 binaries with correctly-injected `Version`
   LDFLAGS, on the current `main` HEAD including the uncommitted web
   changes.
2. `make test` (short mode) is green for all 15 workspace modules.
3. `go test -race -short -count=3 ./...` shows no data races.
4. `go vet ./...` is clean across all 15 modules.
5. `make e2e-happy` and `make e2e-compensation` PASS (Docker required;
   environment has Docker per user confirmation).
6. The three uncommitted web files are either committed (with a regression
   test that proves the rewrite is functionally equivalent or better) or
   reverted (with a rationale in the audit findings).
7. A new `audit/REAUDIT_FINDINGS.md` lists every finding with
   `file:line + reproduction + fix`.
8. Every P0/P1 new finding is fixed in the same session and accompanied by
   a regression test that fails on the pre-fix code and passes after.
9. P2/P3 findings are either fixed or explicitly recorded as TODO with
   owner and rationale.

## 3. Non-Goals

- No architectural changes (saga stays choreography+orchestrator; outbox
  pattern stays; Postgres per-service stays).
- No new features.
- No k8s/Helm changes unless they are regression-grade broken under
  `helm template` (Docker available; kind/k8s not available, so live cluster
  verification stays out of scope and is documented as such).
- No cosmetic-only changes (typos, comment polish) unless required by a
  finding.
- No refactoring of working code on the way to a finding.

## 4. Architecture (of the audit itself)

```
┌─────────────────────────────────────────────────────────────┐
│                Senior-Go Re-Audit Pipeline                  │
│                                                             │
│  Phase 1: Baseline                                          │
│   ├─ make build / test / vet / tidy                         │
│   ├─ Decide dirty-state policy for web (commit / revert)    │
│   └─ Record starting HEAD SHA + uncommitted state           │
│                                                             │
│  Phase 2: Static deep-read (5 parallel tracks)              │
│   ├─ Track A: pkg/outbox + pkg/consumer (hot path)          │
│   ├─ Track B: services/saga (state, watchdog, TTL, outbox)  │
│   ├─ Track C: services/{order,payment,inventory}            │
│   ├─ Track D: services/web (SvelteKit SPA, embed, server)   │
│   └─ Track E: deploy/ + tests/ + Makefile + ADRs            │
│                                                             │
│  Phase 3: Cross-cutting pass                                │
│   ├─ Security: secrets, auth, idempotency replay,           │
│   │             HMAC, TLS, input validation                 │
│   ├─ Observability: slog, metrics, OTel, trace propagation  │
│   └─ Operations: graceful shutdown, goroutine leaks,         │
│                  resource leaks, error wrapping             │
│                                                             │
│  Phase 4: Dynamic verification                              │
│   ├─ go test -race -short -count=3 ./...                    │
│   ├─ Targeted integration tests with Docker (e2e/chaos)     │
│   └─ go test -race -short -count=20 on hot paths            │
│                                                             │
│  Phase 5: TDD implementation                                │
│   ├─ Each P0/P1: red test → fix → green                     │
│   ├─ Each P2/P3: red test → fix OR TODO w/ rationale        │
│   └─ Each fix re-runs the full verification chain           │
│                                                             │
│  Phase 6: Final adversarial pass                            │
│   ├─ Fresh eyes on the fixed code                           │
│   └─ New findings → back to Phase 5                         │
└─────────────────────────────────────────────────────────────┘
```

### 4.1 Track assignments

Each track owns files exclusively to avoid merge conflicts. Tracks A, C, D
are read-then-write; Track B is the highest-risk for new findings (saga state
machine is documented in `state.go` and implemented in `handlers.go` per
the previous audit's P3-SAGA-16, and this divergence has never been
fully reconciled); Track E is mostly config verification.

| Track | Files (read scope)                              | Read time |
|-------|-------------------------------------------------|-----------|
| A     | `pkg/outbox/*`, `pkg/consumer/*`                | 90 min    |
| B     | `services/saga/**`                              | 90 min    |
| C     | `services/order/**`, `services/payment/**`, `services/inventory/**` | 120 min |
| D     | `services/web/**` (incl. uncommitted SvelteKit) | 60 min    |
| E     | `deploy/**`, `tests/**`, `Makefile`, `go.work`, `docs/adr/**`, `cmd/**` | 60 min |
| Cross | Security/obs/ops across all                     | 60 min    |

### 4.2 Finding classification

Each finding is documented with:

- **Severity**: P0 (production-fatal, fix immediately) / P1 (significant
  correctness/UX, fix in session) / P2 (correctness/UX, fix if time,
  else TODO) / P3 (cosmetic/maintenance, TODO allowed).
- **Category**: `bug` / `race` / `leak` / `security` / `observability` /
  `error-handling` / `config` / `doc-drift`.
- **Evidence**: `file:line` + reproduction (test or runtime log).
- **Fix**: code change summary.
- **Regression test**: name of the test that fails pre-fix and passes post.

## 5. Components under audit

The full module surface area to read in this pass:

```
pkg/
├── outbox/        # Poller, KafkaPublisher, Source, Metrics, DLQ
├── consumer/      # dispatch, deduper, kafka_dlq, runner
└── platform/
    ├── logging.go # slog + piiHandler + Redact (SEC-11/12)
    ├── otel.go    # tracing init
    ├── middleware # chi stack + requestMetrics (OBS-2) + readyz (OBS-1)
    ├── events/    # Envelope + Client + franz-go wrapper (OBS-5)
    ├── errors/    # APIError + sentinels
    ├── types/     # Money, UUIDs
    └── instrumentation/kafkaprop/

services/
├── order/
│   ├── cmd/order/main.go
│   └── internal/{domain,api,repository,outbox,consumer}
├── payment/
│   ├── cmd/payment/main.go
│   └── internal/{provider,idempotency,webhook,repository,outbox,consumer,events}
├── inventory/
│   ├── cmd/inventory/main.go
│   └── internal/{model,repository,lock,outbox,consumer,events}
├── saga/
│   ├── cmd/saga/main.go
│   └── internal/{state,timeout,watchdog,consumer,repository,outbox,events,saga}
└── web/                  # SvelteKit SPA (UNCOMMITTED)
    ├── cmd/web/main.go
    ├── spa.go            # embed
    ├── internal/server,handlers,backend,kafkatail,events
    └── frontend/         # SvelteKit source

cmd/                 # per-service Dockerfile roots
deploy/              # docker-compose, helm, kustomize, observability
tests/               # harness, e2e, chaos, k8s, load
docs/adr/            # ADR-0001 .. 0005
Makefile, go.work, .golangci.yml
```

### 5.1 Areas of known suspicion (not prior findings — hypotheses to verify)

These are hypotheses the audit should actively test, NOT prior findings
that should bias the search:

- **web SvelteKit rewrite**: SPA served via `embed.FS`, Go BFF proxies
  `/api/*` to backend services. Hypothesis: routes are wired correctly,
  but the embed boundary may double-include assets; the BFF may have
  drift between the URL the SPA calls and the BFF endpoint.
- **saga state.go vs handlers.go**: `state.go` may still be decorative;
  per P3-SAGA-16 it "contradicts the runtime". Hypothesis: handlers are
  canonical and state.go is misleading.
- **outbox header carrier**: OBS-5 fix uses `RecordHeaderCarrier`; the
  outbox hops through `r.Headers` which is written empty at INSERT.
  Hypothesis: traceparent breaks across the outbox hop on the
  consumer→outbox→publish→consumer round trip.
- **graceful shutdown**: `services/saga/cmd/saga/main.go:146-151` calls
  `wgWait(ctx, ...)` after `<-ctx.Done()`; P2-SAGA-10 says "the
  HTTP-disabled path receives an already-cancelled context". Hypothesis:
  wgWait does not honor a fresh shutdown grace context.
- **consumer deduper hit mark**: P0-NEW-P0-1 was a one-line fix to add
  `c.markRecord(rec)` before the dedupe-hit early-return. Hypothesis:
  similar patterns exist in other dedupe-style early returns.
- **gRPC vs REST drift**: ADR-0003 was rewritten "REST-only" in v1.1.4;
  any leftover gRPC plumbing is dead code.
- **kustomize hand-roll drift**: `deploy/kustomize/base/services.yaml`
  is hand-rolled. Hypothesis: probes/SA/RBAC may have drifted from helm.
- **ADR-0005 gap**: outbox "data-losing restart" is documented as
  known. Hypothesis: nothing in code (or tests) prevents re-introducing
  the regression.

## 6. Data flow of one finding

```
1. Read source in assigned track
2. Spot anomaly → classify (severity + category)
3. Write a failing test that reproduces the anomaly (red)
4. Implement the fix (green)
5. Re-run full verification chain (make build; make test; -race -count=3)
6. Document in audit/REAUDIT_FINDINGS.md with file:line + repro + fix + test
7. Continue track (no stopping for each finding; batch verification at end of phase)
```

## 7. Error handling within the audit

- If a finding requires changes to >5 files OR an architectural decision,
  pause and ask the user before implementing.
- If a fix conflicts with an existing regression test, the existing test
  is wrong — fix the test (after explaining the conflict in the
  findings doc).
- If `make build` or `go vet` becomes red at any point, stop the track,
  fix immediately, then continue.
- If Docker is unavailable at verification time, document the gap and
  skip the e2e rows; the rest of the verification chain still applies.

## 8. Testing strategy

- Every P0/P1 fix MUST have a regression test that:
  - Fails (or is skipped-but-compiled) on the pre-fix code.
  - Passes on the post-fix code.
  - Is committed in the same commit as the fix.
- Race detector: `-race -short -count=3` on every workspace module after
  Phase 5; `-count=20` on `pkg/outbox`, `pkg/consumer`,
  `services/saga`, `services/order` (hot paths).
- E2E: run `make e2e-happy` and `make e2e-compensation` once after
  Phase 5; gate the session on green (Docker is available).
- Chaos: `make e2e-chaos` if time permits; not a blocker.
- Tests requiring `DATABASE_URL` skip cleanly without it; documented per
  test where the skip applies.

## 9. Acceptance criteria

The session ends successfully when ALL of the following are true:

1. **Baseline**:
   - `make build` PASS (5 binaries, correct LDFLAGS).
   - `make test` PASS (15 modules, short mode).
   - `go test -race -short -count=3 ./...` PASS across all modules.
   - `go vet ./...` clean.
   - `make e2e-happy` PASS (Docker).
   - `make e2e-compensation` PASS (Docker).
2. **Dirty state**: either committed (web SPA rewrite + tests) or
   reverted (with rationale); no half-applied work in the tree.
3. **Findings**: `audit/REAUDIT_FINDINGS.md` exists with every new
   finding documented (P0/P1/P2/P3 × category × file:line × repro ×
   fix × regression test).
4. **Fixes**: every P0/P1 finding has a passing regression test
   committed in the same session.
5. **TODOs**: every P2/P3 finding is either fixed or has a TODO with
   owner (default: future session) and rationale.
6. **No regressions**: the post-fix tree passes the full verification
   chain; the only differences from `main` HEAD are the new findings,
   their fixes, and their tests.
7. **Final adversarial pass**: a fresh read of the post-fix code
   surfaces either zero new findings or only minor follow-ups
   explicitly recorded.

## 10. Deliverables

- `audit/REAUDIT_FINDINGS.md` — the consolidated findings list with
  file:line + reproduction + fix + test pointer per item.
- New or modified Go files for each fix.
- New or modified test files for each regression test.
- One or more commits on the current branch (no force-push, no rebase).
- An updated `STATUS.md` line summarising the audit outcome.

## 11. Risks and unknowns

- **Docker availability**: user confirmed Docker is present; if `make
  e2e-happy` fails for environment reasons (image pull, port conflict),
  the e2e gate is replaced by `go test -race -short -count=5 ./...` and
  the gap is documented.
- **Hidden tests**: any test not run by `make test` may have bugs; the
  full set of `*_test.go` files is enumerated by the workspace.
- **Cross-track conflicts**: tracks own disjoint files; the BFF
  cross-track touchpoint (`services/web/internal/backend/*`) is
  serialised after Track D.
- **Time**: this is a multi-hour session. If the audit runs long, the
  priority order is: Phase 1 baseline → Phase 2 Track A → Phase 4
  verification → Phase 5 fixes for whatever was found → Phase 6. The
  remaining tracks proceed in B, C, D, E, cross order, with P0/P1
  fixes interleaved.

## 12. After the audit

The next session picks up from `STATUS.md` and the new
`audit/REAUDIT_FINDINGS.md`. P2/P3 TODOs are prioritised there. The
ADR-0005 gap (outbox "data-losing restart") remains documented as a
known design issue; future work is to either run Kafka on persistent
volumes or implement `OUTBOX_REEMIT_ON_STARTUP`.
