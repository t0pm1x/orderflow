# orderflow Stage 1 — Render C4 diagrams to PNG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render the 6 PlantUML C4 architecture diagrams in `docs/architecture/*.puml` to PNG files alongside their sources, so they can be embedded inline in the README and viewed on GitHub without requiring local PlantUML tooling. Single new commit, no code changes.

**Architecture:** Install PlantUML (or use Java + plantuml.jar fallback), then run `plantuml -tpng` on each of the 6 `.puml` files. PlantUML fetches `C4_Container.puml` / `C4_Component.puml` from the standard library via `!include` at render time, so the local machine must have internet access. The resulting PNGs are committed to the repo alongside their sources so the README can reference them via relative paths.

**Tech Stack:** PlantUML (CLI), Java 11+ runtime (PlantUML dependency), C4-PlantUML standard library (fetched at render time via `!include https://...`).

## Global Constraints

- No code changes — this is a presentation-only commit.
- Working directory is the existing worktree at `C:\Users\t0p_m\projects\orderflow-v1.1.pre`, branch `v1.1.pre`. All commits land on this branch.
- Each output PNG must be < 2MB (GitHub inline image soft-limit).
- The 6 `.puml` files already exist and are unchanged; this stage only renders them.
- The `.gitkeep` placeholder file in `docs/architecture/` is not removed.
- Existing patterns: the repo uses LF line endings for new files (per `.gitattributes` if any, otherwise default). PlantUML's PNG output is binary — not affected by line-ending choice.
- Verification command: open each PNG in any image viewer and confirm it visually matches the description in the corresponding `.puml`.
- No external dependencies tracked in `go.mod` — this stage touches no Go code.

---

## Task 1: Install PlantUML and verify it works

**Files:** none modified; tooling installed at user/system level.

**Why first:** PlantUML must be on PATH before Task 2 can run. Installing it inside Task 2 would couple the render to installation problems.

- [ ] **Step 1.1: Try `winget install`**

Open PowerShell as Administrator (required for `winget` system installs) and run:

```powershell
winget install --id PlantUML.PlantUML --accept-source-agreements --accept-package-agreements
```

Expected: PlantUML installs to `C:\Program Files\PlantUML\` and registers on PATH.

- [ ] **Step 1.2: Verify PlantUML is on PATH**

Open a **new** PowerShell window (the install modifies PATH for new processes, not the current one) and run:

```powershell
plantuml -version
```

Expected: prints `plantuml version X.Y.Z (some date)`. If `plantuml` is not recognized, fall back to Step 1.3.

- [ ] **Step 1.3: Fallback — Java + plantuml.jar**

If `winget` install fails or `plantuml` is not on PATH:

```powershell
# Check Java is available
java -version
```

Expected: prints `openjdk version "X.Y.Z"` or similar (Java 11+ required).

Download `plantuml.jar` (the same JAR winget would have installed):

```powershell
New-Item -ItemType Directory -Force -Path "$env:LOCALAPPDATA\PlantUML"
Invoke-WebRequest -Uri "https://github.com/plantuml/plantuml/releases/latest/download/plantuml.jar" -OutFile "$env:LOCALAPPDATA\PlantUML\plantuml.jar"
```

Add a `plantuml.cmd` shim so the rest of the plan can call `plantuml` uniformly:

```powershell
$shim = "$env:LOCALAPPDATA\PlantUML\plantuml.cmd"
Set-Content -Path $shim -Value "@echo off`njava -jar %LOCALAPPDATA%\PlantUML\plantuml.jar %*"
[Environment]::SetEnvironmentVariable("Path", "$env:LOCALAPPDATA\PlantUML;$env:Path", "User")
```

Open a **new** PowerShell window and verify:

```powershell
plantuml -version
```

Expected: same as Step 1.2. If still failing, the user must resolve PATH or Java setup before proceeding.

- [ ] **Step 1.4: Smoke test — render the smallest diagram**

The smallest `.puml` is `docs/architecture/c4-level-1.puml` (13 lines). Run PlantUML against it to confirm the C4-PlantUML stdlib `!include` resolves correctly:

```powershell
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
plantuml -tpng docs/architecture/c4-level-1.puml
```

Expected: writes `docs/architecture/c4-level-1.png` (no error output). If PlantUML fails to fetch `https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml`, the rendering fails with a download error — verify internet access with:

```powershell
Test-NetConnection raw.githubusercontent.com -Port 443
```

Expected: `TcpTestSucceeded: True`. If false, this stage cannot proceed; defer to user.

- [ ] **Step 1.5: Inspect the smoke-test PNG**

Open `docs/architecture/c4-level-1.png` in any image viewer (Windows Photos, IrfanView, etc.) and confirm it shows:
- A "Person" figure labeled "Client"
- A "System" box labeled "orderflow" with description
- A second "System" box labeled "Mock Payment Provider"
- An arrow "POST /v1/orders" between Client and orderflow
- An arrow "Charge/refund" between orderflow and Payment Provider

If the diagram renders correctly, PlantUML is working. If boxes are missing or text is garbled, check PlantUML version compatibility with C4-PlantUML stdlib (newer PlantUML versions may render differently).

- [ ] **Step 1.6: Commit the smoke test PNG**

The smoke-test PNG is a useful artifact on its own (GitHub renders it inline). Stage it and commit:

```bash
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
git add docs/architecture/c4-level-1.png
git commit -m "render: c4-level-1.png (system context diagram)"
```

This makes Task 1 an independently-reviewable commit. Task 2 continues from here.

---

## Task 2: Render remaining 5 diagrams

**Files:**
- Create: `docs/architecture/c4-level-2.png`
- Create: `docs/architecture/c4-level-3-order.png`
- Create: `docs/architecture/c4-level-3-payment.png`
- Create: `docs/architecture/c4-level-3-inventory.png`
- Create: `docs/architecture/c4-level-3-saga.png`

**Why separate from Task 1:** Task 1's smoke test verified PlantUML works. Task 2 is the bulk render — separating them keeps each task independently testable and small.

- [ ] **Step 2.1: Render all 5 remaining diagrams in one command**

```powershell
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
plantuml -tpng -failfast2 `
  docs/architecture/c4-level-2.puml `
  docs/architecture/c4-level-3-order.puml `
  docs/architecture/c4-level-3-payment.puml `
  docs/architecture/c4-level-3-inventory.puml `
  docs/architecture/c4-level-3-saga.puml
```

`-failfast2` makes PlantUML abort on the first render error rather than continuing silently.

Expected: 5 new `.png` files appear next to their `.puml` sources. No output on stdout (PlantUML is quiet on success). If any diagram fails to render, PlantUML exits non-zero with a detailed error message — fix the underlying `.puml` (likely a syntax issue) and retry.

- [ ] **Step 2.2: Verify file sizes**

Each PNG should be well under 2MB. List sizes:

```powershell
Get-ChildItem docs/architecture/*.png | Select-Object Name, @{N="MB";E={[math]::Round($_.Length/1MB, 2)}}
```

Expected: each entry shows MB < 2.0. Typical PlantUML C4 diagrams are 50-500KB. If any is unexpectedly large (> 2MB), inspect visually — it may indicate broken layout.

- [ ] **Step 2.3: Visual verification — open each PNG**

Open each new PNG in any image viewer and confirm:

- **c4-level-2.png** — Container-level: shows 4 services (Order, Payment, Inventory, Saga), Postgres ×3, Redis, Redpanda, OTel Collector, with arrows between them.
- **c4-level-3-order.png** — Order Service components: api handler, domain, repository, outbox, consumer.
- **c4-level-3-payment.png** — Payment Service components: webhook handler, provider, idempotency, outbox, repository.
- **c4-level-3-inventory.png** — Inventory Service components: handler, stock model, redis reservation, outbox, consumer.
- **c4-level-3-saga.png** — Saga Service components: state machine, compensation, watchdog, consumer, outbox.

Any diagram with missing boxes, garbled text, or wrong connections means PlantUML couldn't resolve the C4 stdlib for that specific diagram — check the `.puml` source and PlantUML version.

- [ ] **Step 2.4: Commit the 5 PNGs**

```bash
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
git add docs/architecture/c4-level-2.png docs/architecture/c4-level-3-order.png docs/architecture/c4-level-3-payment.png docs/architecture/c4-level-3-inventory.png docs/architecture/c4-level-3-saga.png
git commit -m "render: C4 component and container diagrams as PNG"
```

- [ ] **Step 2.5: Push branch to origin**

```bash
git push origin v1.1.pre
```

Expected: `Everything up-to-date` or push succeeds. (The branch was already pushed earlier — this is to publish the new PNG commits.)

---

## Task 3: Open PR on GitHub

**Files:** none modified; URL created on github.com.

**Why last:** Without rendered PNGs, the PR is meaningless. After push, open the PR so the GitHub UI renders the PNGs inline and reviewers can verify them visually.

- [ ] **Step 3.1: Open the GitHub PR creation URL**

Visit:

```
https://github.com/t0pm1x/orderflow/compare/main...v1.1.pre?expand=1
```

(Or run `gh pr create --base main --head v1.1.pre --title "render: C4 architecture diagrams as PNG for inline GitHub display" --body "..."` if `gh` CLI is installed.)

Expected: GitHub shows a comparison page between `main` and `v1.1.pre`. Click **Create pull request**.

- [ ] **Step 3.2: Fill PR title and description**

**Title:** `render: C4 architecture diagrams as PNG for inline GitHub display`

**Body:**

```markdown
## What

Renders the 6 PlantUML C4 architecture diagrams (`.puml`) to PNG and commits them alongside their sources, so GitHub can display the architecture inline in the README and other markdown documents without requiring readers to install PlantUML locally.

## Why

The `.puml` sources exist and are referenced from `docs/architecture/` and from `docs/superpowers/specs/orderflow-v1.1-design.md`, but GitHub does not render PlantUML natively. PNGs make the diagrams accessible in the GitHub web UI.

## Files

- `docs/architecture/c4-level-1.png` — system context (Client, orderflow, Mock Payment Provider)
- `docs/architecture/c4-level-2.png` — containers (4 services + Postgres ×3 + Redis + Redpanda + OTel)
- `docs/architecture/c4-level-3-order.png` — Order Service components
- `docs/architecture/c4-level-3-payment.png` — Payment Service components
- `docs/architecture/c4-level-3-inventory.png` — Inventory Service components
- `docs/architecture/c4-level-3-saga.png` — Saga Service components

## Test plan

- [ ] View each PNG in the GitHub PR "Files changed" tab
- [ ] Confirm each diagram visually matches the corresponding `.puml` source

## Followup

README rewrite (Stage 3 of the GitHub-beauty plan) will embed these PNGs.

---

Part of v1.1.0-beauty. See `docs/superpowers/specs/2026-08-17-orderflow-v1.1.beauty-design.md`.
```

- [ ] **Step 3.3: Submit the PR**

Click **Create pull request**. The PR is now open against `main`. CI (`ci.yml`) will run the build matrix + lint + e2e jobs. Since this PR only adds PNG files and no code, all jobs should pass (PNG files are ignored by Go toolchain).

---

## Self-Review Checklist

- [x] **Spec coverage:** Stage 1 of `2026-08-17-orderflow-v1.1.beauty-design.md` requires 6 PNGs alongside `.puml` sources — covered by Task 1 (1 PNG) + Task 2 (5 PNGs) = 6 total.
- [x] **Placeholder scan:** No "TBD"/"TODO"/"fill in"/"similar to Task N" in plan. Every step has exact commands or verification text.
- [x] **Type consistency:** File paths match the spec exactly (6 PNGs in `docs/architecture/`).
- [x] **PR boundary:** This plan produces a single logical change (PNG render), split into Task 1 (smoke test) + Task 2 (bulk render) + Task 3 (PR open). Each is independently testable.
- [x] **No external system changes:** only `winget install` (PlantUML). No Go code changes. No CI workflow changes. No tag/branch creation.

## Plan Complete

After this plan finishes:
- 6 PNG files exist in `docs/architecture/` (commit `render: C4 component and container diagrams as PNG`).
- Smoke-test PNG from Task 1 is its own commit (one extra commit — that's fine).
- PR is open against `main` for visual review.
- Stage 6 (social preview PNG) and Stage 3 (README rewrite that embeds these PNGs) become unblocked.