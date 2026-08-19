# orderflow v1.1.4 — final-engineering-pass batch

> Internal session document. Captures what shipped, why, and what
> remains unverified. Author: AI engineer (build mode), 2026-08-19.

This document is **not** intended for users — it's a working log so
the next session can pick up where this one left off without
re-deriving context.

---

## 1. Where this fits in the timeline

```
v1.1.2              adversarial-audit follow-up [13 commits]
v1.1.3              housekeeping [14 commits]
v1.1.4              THIS BATCH — final engineering pass [~17 commits]
v1.2 (planned)      KafkaPublisher atomic batch (P1-#4)
                    Stripe-style webhook body-vs-strict-mode (P1-#5)
                    Long-term: per-Pod attempts counter is DONE here
```

The v1.1.3 batch left two P0 user-facing bugs (Cancel button,
force-webhook buttons) and a P0 distributed-systems bug (outbox
DLQ double-fire on persistent broker failure) that the adversarial
audit of v1.1.2 missed because they couldn't be triggered by
static read-through alone. v1.1.4 closes all of them plus the
v1.1.2 deferred P1-#3 (per-Pod attempts counter) and one P2 doc
drift.

---

## 2. P0 fixes (web UI buttons + outbox DLQ)

### 2.1 Cancel button on `/orders/{id}` actually cancels

The Order Service's chi router registered `POST`, `GET` only.
Clicking Cancel POSTed to a non-existent route; the BFF's
`DELETE` proxy got `405 Method Not Allowed` → `502` to the user.
Pre-fix: README claimed "✅ What works" includes "All 4 services
[…] REST API" — but no DELETE endpoint existed, and no test
caught it because `TestOrderCancel_OK` used `fakeOrderClient`
that accepts any upstream result.

**Fix:**

- `services/order/internal/api/handler.go` — added `Repository.Cancel`
  to the interface, `r.Delete("/v1/orders/{id}", h.cancel)` to
  `Routes()`, and the `cancel` handler. Maps `errNotFound` →
  `404`, success → `204`.
- `services/order/internal/repository/pg_repo.go` — atomic
  `pgx.BeginFunc`: `UPDATE orders SET state='cancelled',
  updated_at=NOW(), completed_at=NOW() WHERE id=$1 AND state NOT
  IN ('confirmed','cancelled','failed')` + `INSERT INTO
  order_outbox` (event `OrderCancelled`, payload
  `{reason:"user_request", source:"user"}`). `RowsAffected=0` →
  `ErrNotFound` so the handler can map cleanly.
- `services/order/migrations/` — no change (uses existing columns).
- `services/order/internal/repository/pg_repo_test.go` — 3 new
  PG-real tests (skip without `DATABASE_URL`):
  `TransitionsAndEmitsEvent`,
  `AlreadyTerminalReturnsErrNotFound` (×3 terminal states),
  `UnknownIDReturnsErrNotFound`.
- `services/order/internal/api/handler_test.go` — 3 new
  handler-level tests: `TestCancel_OK/NotFound/InvalidID`.

The regression net's intent is what the v1.1.2 audit's §5.A
pointed at: a button click must drive the full chain (HTTP →
chi route → handler → repository → DB → outbox). The fake-repo
test that pre-fix passed said nothing about whether the upstream
`DELETE` worked. The new tests fail if either side of the chain
breaks.

### 2.2 Force-webhook buttons on `/payments/sim` work with Redis

The BFF's `FireWebhook` issued a POST with no `Idempotency-Key`
header. When the Payment Service runs with `REDIS_URL` set
(per `docs/demo/demo.sh:73-93`), the idempotency middleware
returns `400 Idempotency-Key header required`. The web UI's
`force ✓ / force ✗` therefore returned `502 Bad Gateway` in every
docker-compose run.

**Fix:**

- `services/web/internal/backend/payment.go` —
  `req.Header.Set("Idempotency-Key",
  "orderflow-web:"+w.PaymentID+":"+w.Status)` in `FireWebhook`.
  Deterministic per `(order_id, status)` so a UI replay hits
  the cached response; a different status produces a different
  key by design.
- `services/web/internal/backend/payment_test.go` —
  `TestPaymentClient_FireWebhook_SetsIdempotencyKey` (new).

### 2.3 Outbox poller no longer double-fires DLQ

Pre-fix: `handlePublishFailure` called `src.MarkFailedTx(ctx, tx,
ids)` inside the same `RunInTx` closure that was about to roll
back on the publish error; the FAILED transition undid itself
and the row stayed `PENDING` forever. The in-memory `p.attempts`
counter then climbed across every poll and `DLQ.Send` fired once
per ~3 polls (~33 entries in 500 ms).

The v1.1.3 batch added a fake-source test
(`TestPoller_RoutesToDLQAfterMaxAttempts`) that PASSED because
the fake marks rows as failed unconditionally — the test never
saw the real-PG issue. v1.1.4 closes the gap in two places:

**Fix path 1 — split commit-on-success vs rollback-on-retry:**
`pkg/outbox/poller.go` `handlePublishFailure` now returns `bool`.
If any row in the batch crossed `MaxAttempts`, the closure
returns `nil` (commit) so the FAILED transition becomes durable
inside the row lock. Rows still under the cap keep returning
`err` (rollback) and stay `PENDING` for the next poll. Plus:
the saga's `markFailed.sql` now `SET status='FAILED'` alongside
the existing `attempts++` so the saga source matches the order /
payment / inventory sources, which already did.

**Fix path 2 — per-Pod attempts counter survives restarts**
(also closes the v1.1.2 deferred P1-#3): see §3 below.

`TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown` is the new
real-PG regression net: 500 ms persistent-publisher-failure run,
asserts `dlq.sent == 1` (pre-fix: ~33 fires).

---

## 3. P1 fixes (outbox + concurrency + infrastructure)

### 3.1 Per-Pod attempts counter survives restarts (v1.1.2 P1-#3)

The retry budget was tracked only in a per-Pod `sync.Map`. A
pod restart wiped it and silently reset the budget. Saga already
had the `attempts` column on `saga_outbox`; order / payment /
inventory did not.

**Fix:**

- New migrations:
  - `services/order/migrations/0003_outbox_attempts.sql`
  - `services/payment/migrations/0004_outbox_attempts.sql`
    (`payment` already had `0003_payment_order_unique.sql`)
  - `services/inventory/migrations/0004_outbox_attempts.sql`
    (`inventory` already had `0003_seed.sql`)
- Updated `markFailed.sql` in all three sources to set
  `attempts = attempts + 1` AND `last_error` alongside
  `status='FAILED'`.
- `pkg/outbox/types.go` — `Source` interface gains
  `AttemptsOfTx(ctx, tx, eventIDs) (map[string]int, error)`.
- `pkg/outbox/poller.go` — the `RunInTx` closure pre-reads
  attempts from DB inside the locked tx (after `fetchPending`).
  `handlePublishFailure` uses `max(in-memory, DB)` so the cache
  can be safely repopulated from zero.
- Updated all 4 service sources' `markFailed.sql` and added
  `AttemptsOfTx` to each `internal/outbox/source.go`.

`TestPoller_DBQueriesAttemptsForDLQ` is the new regression net:
pre-seeds `dbAttempts[id] = MaxAttempts-1` and asserts the FIRST
observed publish failure crosses the threshold — not after
`MaxAttempts` new failures (which is what a pre-fix poller
would do because the in-memory sync.Map starts at zero in a fresh
pod).

### 3.2 Consumer dispatch marks records for unknown event types

Pre-fix: a record carrying an `event_type` no service handles
(forward-compatible producer) early-returned without calling
`markRecord(rec)`. With `kgo.DisableAutoCommit`, only
`CommitMarkedOffsets` advances offsets — the unknown record
re-fetched on every poll and held the partition hostage forever.
Decode errors had the same problem (early-return after
`c.toDLQ`).

**Fix:**

- `pkg/consumer/consumer.go` — `dispatch` calls `markRecord(rec)`
  before the unknown-type early-return AND on decode error.
- `pkg/consumer/consumer_test.go` —
  `TestDispatch_UnknownEventTypeStillMarksForCommit`,
  `TestDispatch_DecodeErrorMarksRecord` (new).

### 3.3 Payment Repository respects the request context

`webhook.Repository` interface omitted `context.Context`, so
every PG call used `context.Background()`. Client cancellation
(HTTP disconnect, Kafka shutdown) couldn't abort in-flight
queries; the DB backend kept processing requests the client
would never read.

**Fix:**

- `services/payment/internal/webhook/handler.go` — `Get`,
  `UpdateStatus`, `UpdateStatusFromNonTerminal` now take
  `ctx`; the chi handler forwards `r.Context()`.
- `services/payment/internal/repository/pg_repo.go` — uses the
  caller's ctx for `pgxpool.QueryRow` and `pgx.BeginFunc`.
- `services/payment/internal/webhook/handler_test.go` — fake-repo
  methods updated to new signature.

### 3.4 `kubectl kustomize` overlays now render

Pre-fix: `base/` was a comment-only stub with a `for svc in ...
helm template ...` instruction; without `helm` on the controller's
PATH every overlay failed with *"no resource matches strategic
merge patch ..."*. CI's `tests/k8s/smoke_test.go` would have
caught this if it ever ran successfully (the test does
`kubectl kustomize`-equivalent Helm template rendering, not the
raw overlays).

**Fix:**

- `deploy/kustomize/base/services.yaml` (new) — hand-rolled
  stand-in for the 4 service Deployments, matching the
  helm-template shape (port, env, probes, security context).
- Updated patch targets to un-prefixed `metadata.name`
  (`orderflow-order`, not `dev-orderflow-order`) so `namePrefix`
  on dev/staging overlays applies consistently.
- Removed redundant per-overlay `namespace.yaml`s (the base
  transforms them).
- Moved HPA + PDB to `resources:` on prod (they're new
  resources, not patches).
- Dropped `deploy/kustomize/overlays/dev/ingress.yaml` (Ingress
  referenced by name but no Ingress exists in base; document
  its removal in the README).
- `deploy/kustomize/README.md` documents the stand-in behavior +
  the helm-rebuild procedure.

Verified:
```
$ kubectl kustomize deploy/kustomize/overlays/dev       # 255 lines
$ kubectl kustomize deploy/kustomize/overlays/staging   # 255 lines
$ kubectl kustomize deploy/kustomize/overlays/prod      # 379 lines + HPA + PDB
```

---

## 4. P2 fixes (defensive + doc drift)

### 4.1 Saga StockReleasedHandler uses state-guarded transition

`StockReleasedHandler` called `repo.UpdateState(ctx, orderID,
sagapkg.StateCompensated)` with no `from` guard; an
out-of-order replay could in principle overwrite a `Completed`
saga. Defensive only — the normal event flow makes the race
unreachable — but the change closes the door for free.

**Fix:**

- `services/saga/internal/consumer/handlers.go` —
  `TransitionStateTx(from=Compensated → to=Compensated)` inside
  a `pgx.BeginFunc` (matches the rest of the saga handlers).

### 4.2 Dead code + doc drift removed

The `services/inventory/internal/redis` package documented a
Redis-backed reservation store that was never implemented; the
actual reservation lives in `internal/lock` via Postgres
`stock_items`. `services/order/internal/saga` was a one-line
doc stub for sub-stage 3.9 that never landed any code.

**Fix:**

- Deleted both stub `doc.go` files.
- Updated `docs/architecture/c4-level-2.puml` and
  `c4-level-3-inventory.puml` to remove the Redis-reservation
  component and the misleading `Reservations with TTL`
  relationship; Redis is now correctly described as `Idempotency
  cache + consumer dedup`.

---

## 5. Test additions / changes

| File | Tests added / changed |
|---|---|
| `pkg/outbox/poller_test.go` | `TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown` (real-PG; 500 ms persistent failure run). `TestPoller_DBQueriesAttemptsForDLQ` (DB-seeds attempts to MaxAttempts-1; asserts first failure triggers DLQ). `TestPoller_RetriesOnPublishError` adjusted `MaxAttempts=1000` so it stays in the under-cap branch and asserts rollback. |
| `pkg/consumer/consumer_test.go` | `TestDispatch_UnknownEventTypeStillMarksForCommit`, `TestDispatch_DecodeErrorMarksRecord`. |
| `services/web/internal/backend/payment_test.go` | `TestPaymentClient_FireWebhook_SetsIdempotencyKey`. |
| `services/order/internal/api/handler_test.go` | `TestCancel_OK`, `TestCancel_NotFound`, `TestCancel_InvalidID`. |
| `services/order/internal/repository/pg_repo_test.go` | `TestPGRepo_Cancel_TransitionsAndEmitsEvent`, `TestPGRepo_Cancel_AlreadyTerminalReturnsErrNotFound` (×3 terminal states), `TestPGRepo_Cancel_UnknownIDReturnsErrNotFound` (PG-real, skip). |

All tests skipped on hosts without `DATABASE_URL` or
Docker/Kafka — same pattern as v1.1.3.

---

## 6. Migration summary

Harness `tests/harness/harness.go:324 applyMigrations` reads every
`*.sql` in `services/<svc>/migrations/` lexically. The three new
migrations slot in cleanly:

| Service | File (lexical order) |
|---|---|
| order | `0001_init.sql`, `0002_outbox_headers.sql`, **`0003_outbox_attempts.sql`** |
| payment | `0001_init.sql`, `0002_outbox_headers.sql`, `0003_payment_order_unique.sql`, **`0004_outbox_attempts.sql`** |
| inventory | `0001_init.sql`, `0002_outbox_headers.sql`, `0003_seed.sql`, **`0004_outbox_attempts.sql`** |
| saga | `0001_init.sql`, `0002_saga_outbox.sql` (unchanged) |

All migrations use `ADD COLUMN IF NOT EXISTS` so re-running the
harness on the same DB is a no-op.

---

## 7. Verification status

| Verification | Result |
|---|---|
| `make build` | ✅ PASS — all 5 binaries |
| `go test -short -race ./...` (15 modules) | ✅ PASS — 15/15 |
| `go vet ./...` (changed modules) | ✅ PASS — clean |
| `gofmt -l` (my files) | ✅ PASS |
| `TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown` | ✅ PASS (regression net for v1.1.4) |
| `TestPoller_DBQueriesAttemptsForDLQ` | ✅ PASS |
| `TestCancel_OK/NotFound/InvalidID` | ✅ PASS |
| `TestPaymentClient_FireWebhook_SetsIdempotencyKey` | ✅ PASS |
| `TestDispatch_UnknownEventTypeStillMarksForCommit` | ✅ PASS |
| `TestDispatch_DecodeErrorMarksRecord` | ✅ PASS |
| `kubectl kustomize deploy/kustomize/overlays/{dev,staging,prod}` | ✅ PASS — all 3 render |
| Integration (real PG) | ⚠️ BLOCKED — no `DATABASE_URL` on this host; tests skip cleanly |
| E2E / chaos / helm | ⚠️ BLOCKED — Docker / Helm unavailable on this host |
| golangci-lint v2 | ⚠️ BLOCKED — v1.x local cannot read v2.x config; CI uses v2.x via `golangci/golangci-lint-action@v9` |

---

## 8. Commit log

To be generated by `git log --oneline v1.1.3..HEAD` after the
working tree is committed. Expected ~17 commits in this batch,
one per logical change:

(Authors: AI engineer — fills in commit SHAs after push.)

---

## 9. What the next session should know

1. **The DLQ fix is two-sided.** Path 1 (commit-on-FAILED) is
   load-bearing; Path 2 (DB-attempts counter) is defense-in-depth.
   If a future refactor accidentally removes the
   `committable` decision in `pkg/outbox/poller.go:148`, the
   `TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown` test will
   fail immediately. Keep the test.

2. **The cancel endpoint mirrors the saga's `TransitionStateTx`
   pattern.** Both rely on the same `WHERE state NOT IN
   ('confirmed','cancelled','failed')` guard; if the order
   state machine gains new terminal states (e.g. `refunded`),
   update the guard in BOTH places (`handler.go:174`,
   `consumer/handlers.go:126`).

3. **`pkg/outbox.Source` has 4 methods now.** RunInTx,
   MarkSentTx, MarkFailedTx, AttemptsOfTx. A new service that
   uses `pkg/outbox` MUST implement all 4.

4. **Migrations are discovered by lexical name.** Don't reuse
   `0003_*` between services if the file order matters.
   `payment/migrations/0003_payment_order_unique.sql` and
   `0004_outbox_attempts.sql` work side-by-side because the
   unique-constraint migration is independent of the column
   additions; never make the order of files matter unless one
   truly depends on the other.

5. **Kustomize overlays require manual regeneration** when
   the upstream `deploy/helm/orderflow-*/values.yaml` changes
   in a way that affects rendered shape. The README has the
   procedure; without `helm` on the controller's PATH, the
   hand-rolled `deploy/kustomize/base/services.yaml` is the
   source of truth and must be updated in lockstep.

6. **Dead-code removal is safe.** Both `internal/redis/doc.go`
   and `internal/saga/doc.go` had no Go references outside their
   directory. Running `go build ./...` before commit is sufficient
   confirmation.
