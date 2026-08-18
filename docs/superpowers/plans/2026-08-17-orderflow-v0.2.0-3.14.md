# Stage 3.14 — Final whole-branch review

**Why:** After all deferred work lands, a single reviewer pass catches inconsistencies across stages 3.10–3.13.

**Depends on:** Stages 3.10, 3.11, 3.12, 3.13 all complete and pushed.

### Task 3.14.a — Review checklist pass + CHANGELOG + tag (SEQ; single task)

**Files:**
- Modify: `STATUS.md` (move 3.10–3.13 rows from "Next up" to "Sub-stages")
- Modify: `CHANGELOG.md` (add v0.2.0 entry)
- Modify: `README.md` (Status: v0.2.0; refresh deferred list)
- Modify: `docs/superpowers/portfolio/orderflow-checkpoint.md` (archive)

**Interfaces:**
- Produces: tag `v0.2.0` on the commit that closes 3.14.

- [ ] **Step 1: Build + vet + full test pass**

```powershell
cd C:\Users\t0p_m\projects\orderflow
make tidy
make build
make test
make lint
```
All four must succeed. If lint fails, run `gofmt -w .` and re-run.

- [ ] **Step 2: Self-review checklist**

Read every file changed in stages 3.10–3.13 (`git log --stat 97076bd..HEAD` if v0.2.0 is the next tag, or `git diff origin/main..HEAD` against current). Check:

- [ ] No `fmt.Println` left in non-test code
- [ ] No commented-out code blocks larger than 3 lines
- [ ] Every exported function has a godoc comment
- [ ] Every new external dep is in `go.mod` AND `go.sum`
- [ ] Every Helm chart has a NOTES.txt (or explicit "no notes") and passes `helm lint`
- [ ] Every Kustomize overlay passes `kustomize build` cleanly
- [ ] Every ADR has all 6 required sections
- [ ] Every `// TODO` has a corresponding issue or follow-up ticket reference
- [ ] No file in repo contains a hardcoded credential or local path
- [ ] All new env vars documented in `README.md` "Stack" or "Building" section
- [ ] All new Makefile targets listed in README "Building" section
- [ ] `tests/manual/3.10-tracecheck.md` and `docs/demo/README.md` produce visible artifacts (screenshots/SVGs)

- [ ] **Step 3: Update STATUS.md**

Move every 3.10.x, 3.11.x, 3.12.x, 3.13.x row from "Next up" to "Sub-stages" with the actual commit hashes from `git log --oneline`. Add a row `v0.2.0 — CHANGELOG + README + tag`.

- [ ] **Step 4: Update CHANGELOG.md**

Add at top (above `## [Unreleased]`):
```markdown
## [0.2.0] - <today's date>

### Added
- W3C tracecontext propagation through Kafka (Envelope + headers)
- Per-message consumer span (`consumer.<event_type>`)
- chi middleware on /healthz and /metrics for all 4 service binaries
- `service.version` resource attribute on every service
- testcontainers-based E2E suite (happy / compensation)
- Chaos test: redpanda kill mid-flow
- k6 load test: 100 RPS for 60s, p95 < 1s
- Helm charts for all 4 services + 3 infra deps
- Kustomize overlays (dev/staging/prod)
- ArgoCD ApplicationSet for GitOps delivery
- kind cluster config + smoke test
- ADR-0004 (W3C tracecontext decision)
- C4 component diagram for Saga orchestrator
- Demo script + asciinema recording

### Changed
- `outbox.Record` gains `Headers map[string]string`
- New `outbox.headers` JSONB column per service

### Fixed
- README broken link to `docs/superpowers/portfolio/orderflow-substages.md`
```

- [ ] **Step 5: Update README**

- Status line: `## Status: v0.2.0`
- Refresh "What works" with the new artifacts
- Add "Demo" section linking to `docs/demo/orderflow.cast`
- Refresh "Stack" with: Helm 3, kind, testcontainers-go, k6, asciinema

- [ ] **Step 6: Archive checkpoint**

Append to `docs/superpowers/portfolio/orderflow-checkpoint.md` a final section:
```markdown
## v0.2.0 archive — 2026-08-17

All deferred sub-stages (3.10–3.13) closed. v0.2.0 tag created.

Next session resumes at: any post-v0.2.0 work (currently none planned).
```

- [ ] **Step 7: Final commit**

```powershell
git add STATUS.md CHANGELOG.md README.md docs/superpowers/portfolio/orderflow-checkpoint.md
git commit -m "orderflow/3.14: final review — v0.2.0 CHANGELOG, README, STATUS"
```

- [ ] **Step 8: Tag and push**

```powershell
git tag -a v0.2.0 -m "v0.2.0 — tracing, E2E, Helm/K8s, docs/demo"
git push origin main --follow-tags
```

- [ ] **Step 9: Verify**

```powershell
git ls-remote --tags origin | Select-String "v0.2.0"
curl -s https://api.github.com/repos/t0pm1x/orderflow/releases/tags/v0.2.0 | jq .tag_name
```
Expected: tag visible on origin. (Release creation on GitHub is optional, out of plan scope.)