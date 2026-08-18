# orderflow v0.2.0 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete deferred stages 3.10 (tracing), 3.11 (E2E), 3.12 (Helm/K8s), 3.13 (docs/demo), 3.14 (review) to reach v0.2.0 release.

**Architecture:** Extend the existing v0.1.0-MVP foundation with W3C tracecontext propagation through Kafka, testcontainers-based E2E/chaos/load suites, Helm+Kustomize+ArgoCD GitOps delivery, polished docs + recorded demo, and a final whole-branch review.

**Tech Stack:** Go 1.25.13, OpenTelemetry SDK 1.45.0, franz-go (kgo) + `otelfranzgo`, chi v5, Postgres 16, Redpanda, Redis 7, Helm 3, Kustomize, ArgoCD, kind, testcontainers-go, k6 (load), asciinema.

## Global Constraints

- **Repo:** `C:\Users\t0p_m\projects\orderflow` (Windows PowerShell 5.1, NOT bash). Use PowerShell syntax in command examples.
- **Commit style:** `orderflow/<stage>: <imperative summary>` (matches existing 97076bd, 6e7e82f, etc.). Multi-stage grouping: `orderflow/3.X.a + 3.X.b: ...`. Body uses `- file: change` bullets. No `feat:`/`fix:` prefixes.
- **One logical concern per commit.** Do not bundle unrelated formatting fixes into functional commits (run `gofmt -w .` separately).
- **Testing:** stdlib `testing` only — no testify. `if got != want { t.Errorf(...) }` assertions. Fakes over mocks. `t.Setenv` for env-var tests.
- **Lint:** `golangci-lint run` must pass (linters: govet, staticcheck, unused, errcheck, gosimple, gocritic, revive, gofmt, goimports).
- **Module layout:** keep `go.work` flat. Per-service modules: `pkg/platform`, `pkg/outbox`, `pkg/consumer`, `services/{order,payment,inventory,saga}`, `cmd/{order,payment,inventory,saga}`. Any new shared package goes under `pkg/` and gets its own go.mod.
- **Working branch:** `main`. No long-lived feature branches; each stage finishes with commits on `main`.
- **No secrets in commits.** PATs live outside repo (e.g. `Documents\dbguard-token.txt`).
- **Docker available locally:** v29.6.1 confirmed. **kind NOT installed** — when a task requires kind, either install it (winget/choco) or stub with a `make kind-up` script that documents the prerequisite and fails fast.
- **All paths in this plan are repo-relative.** Convert with `C:\Users\t0p_m\projects\orderflow\<path>` when running from PowerShell.

## Stage ordering & parallelization map

Stages execute sequentially (3.10 → 3.11 → 3.12 → 3.13 → 3.14). **Within** each stage, tasks marked `PAR` can run in parallel; tasks marked `SEQ` must run in order.

| Stage | Tasks | Parallel groups |
|------:|-------|------------------|
| 3.10 | 3.10.a → 3.10.b, 3.10.c, 3.10.d, 3.10.e, 3.10.f | `a` first; then `b`‖`c`‖`d`‖`e`; then `f` |
| 3.11 | 3.11.a → 3.11.b, 3.11.c, 3.11.d, 3.11.e, 3.11.f | `a` first; then `b`‖`c`‖`d`‖`e`; then `f` |
| 3.12 | 3.12.a, 3.12.b, 3.12.c, 3.12.d, 3.12.e, 3.12.f | `a.1..a.4`‖`b`‖`e`; then `c`‖`d`; then `f` |
| 3.13 | 3.13.a, 3.13.b, 3.13.c, 3.13.d, 3.13.e | `a`‖`b`‖`c`‖`e` parallel; then `d` |
| 3.14 | 3.14.a (single task with checklist) | n/a |

## Sub-plans (one file per stage, same path prefix)

| Stage | File |
|-------|------|
| 3.10 — Tracing | `docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.10.md` |
| 3.11 — E2E | `docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.11.md` |
| 3.12 — Helm/K8s | `docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.12.md` |
| 3.13 — Docs/demo | `docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.13.md` |
| 3.14 — Final review | `docs/superpowers/plans/2026-08-17-orderflow-v0.2.0-3.14.md` |

Read each sub-plan before executing its stage. Every sub-plan inherits the Global Constraints above.