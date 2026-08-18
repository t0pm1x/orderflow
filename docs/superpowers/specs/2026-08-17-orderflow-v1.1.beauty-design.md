# orderflow GitHub beautification — Design

**Created:** 2026-08-17
**Status:** approved (brainstorming complete)
**Goal:** Polish orderflow's GitHub presence to portfolio quality — beautiful README, rendered architecture diagrams, community files, profile README, social preview, and 9 GitHub Releases matching existing tags. No code changes; this is purely presentation + community hygiene.

## Context

orderflow reached v1.0.0 (then v1.1.pre) as a working microservices platform. The repo is functional but its GitHub presentation is bare-bones:

- **README** is decent but lacks: badges, rendered diagrams, demo embed, performance numbers, contributors section, supported platforms table, social preview image, roadmap visualization.
- **C4 diagrams** exist as PlantUML source files (`.puml`) only — not rendered to PNG/SVG; GitHub cannot display them inline.
- **Community files** missing: `PULL_REQUEST_TEMPLATE.md`, `ISSUE_TEMPLATE/`, `CODEOWNERS`, `dependabot.yml`.
- **GitHub Releases** not created — 9 tags (`v0.1.0-MVP` through `v1.1.pre`) exist in git but no release notes / binaries / changelogs on the Releases page.
- **Profile README** not created — github.com/t0pm1x has no profile content.
- **Social preview** image not uploaded — repo cards render with default placeholder.
- **Web UI settings** (description, topics, Wiki/Projects toggles) not configured.

This plan addresses all of the above except asciinema demo recording (Stage 8, optional) and web UI settings (Stage 5, deferred to user).

## Goals

1. README communicates the platform's value in 30 seconds of scanning.
2. Architecture diagrams are visible inline in README without requiring local tooling.
3. Repo is welcoming to contributors (templates, CODEOWNERS, dependabot).
4. All 9 release tags have GitHub Releases with notes matching CHANGELOG.
5. Profile page (github.com/t0pm1x) showcases orderflow as the primary project.

## Non-goals (explicitly out of scope)

- **Asciinema demo recording** (Stage 8) — optional, depends on docker stack availability at execution time. Skip if blocked.
- **Web UI settings** (Stage 5) — description, topics, social preview upload, Wiki/Projects toggles. Deferred; user will configure via web UI manually.
- **Documentation site** (mkdocs-material + GitHub Pages) — out of scope; markdown files in `docs/` work via GitHub viewer.
- **Domain purchase** or external hosting.
- **CI/CD workflow changes** — existing `.github/workflows/ci.yml` is sufficient.
- **Renaming or reorganizing** the repo.

## Stages

Eight stages total, executed sequentially with one exception (Stage 6 piggybacks on Stage 1 tooling).

### Stage 1 — Render C4 PlantUML → PNG

**Deliverable:** 6 PNG files alongside `.puml` sources in `docs/architecture/`.

| Source `.puml` | Output `.png` |
|---|---|
| `docs/architecture/c4-level-1.puml` | `docs/architecture/c4-level-1.png` |
| `docs/architecture/c4-level-2.puml` | `docs/architecture/c4-level-2.png` |
| `docs/architecture/c4-level-3-order.puml` | `docs/architecture/c4-level-3-order.png` |
| `docs/architecture/c4-level-3-payment.puml` | `docs/architecture/c4-level-3-payment.png` |
| `docs/architecture/c4-level-3-inventory.puml` | `docs/architecture/c4-level-3-inventory.png` |
| `docs/architecture/c4-level-3-saga.puml` | `docs/architecture/c4-level-3-saga.png` |

**Tooling:** PlantUML via `winget install --id PlantUML.PlantUML` (preferred) or Docker `plantuml/plantuml:latest`.

**Render command:**

```bash
plantuml -tpng -failfast2 docs/architecture/c4-level-1.puml docs/architecture/c4-level-2.puml \
                       docs/architecture/c4-level-3-order.puml docs/architecture/c4-level-3-payment.puml \
                       docs/architecture/c4-level-3-inventory.puml docs/architecture/c4-level-3-saga.puml
```

**Verify:** each PNG < 2MB, opens in image viewer, content matches the `.puml` source.

**Commit:** one commit "render: C4 diagrams as PNG for inline GitHub display".

### Stage 2 — Community files

Four new files in `.github/`:

**`.github/PULL_REQUEST_TEMPLATE.md`:**

```markdown
## Description

<!-- What does this PR do? Why? -->

## Related issue

<!-- Link to issue: Fixes #NNN or N/A -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that breaks existing behavior)
- [ ] Documentation update
- [ ] Refactor / chore

## Testing

<!-- How was this tested? Which commands? -->

- [ ] `make test` passes locally
- [ ] `make e2e` passes (if relevant)
- [ ] New tests added (if applicable)

## Checklist

- [ ] Code follows project style (`go vet`, `gofmt`)
- [ ] Self-review performed
- [ ] Comments added for non-obvious logic
- [ ] Documentation updated (README, CHANGELOG, STATUS, ADRs as needed)
```

**`.github/ISSUE_TEMPLATE/bug_report.md`:**

Standard bug report template (Summary, Steps to reproduce, Expected, Actual, Environment, Logs, Screenshots).

**`.github/ISSUE_TEMPLATE/feature_request.md`:**

Standard feature request template (Problem, Proposed solution, Alternatives, Additional context).

**`.github/CODEOWNERS`:**

```
# Default owners for everything in this repo
* @t0pm1x

# Service modules — primary maintainer
/services/order/      @t0pm1x
/services/payment/    @t0pm1x
/services/inventory/  @t0pm1x
/services/saga/       @t0pm1x

# Shared platform code
/pkg/                 @t0pm1x

# Documentation
/docs/                @t0pm1x
*.md                  @t0pm1x
```

(Solo-dev pattern — `* @t0pm1x` covers everything; sub-sections are documentation of intent for future co-maintainers.)

**`.github/dependabot.yml`:**

```yaml
version: 2

updates:
  - package-ecosystem: "gomod"
    directory: "/"
    schedule:
      interval: "weekly"
    groups:
      minor-and-patch:
        patterns:
          - "*"
        update-types: ["minor", "patch"]

  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
```

**Commit:** one commit "feat: add GitHub community files (PR template, issue templates, CODEOWNERS, dependabot)".

### Stage 3 — README rewrite

**Deliverable:** `README.md` (~250-350 lines), top-down structure:

1. **Hero block** (lines 1-15):
   - Title: `# orderflow`
   - Tagline: `> Event-driven order processing platform. 4 microservices (Order, Payment, Inventory, Saga), outbox + saga patterns, OpenTelemetry tracing, deployable to Kubernetes.`
   - Badges: `[MIT]` `[CI]` `[Go 1.25.13]` `[v1.1.pre]` (shields.io URLs)
2. **What is it** (lines 17-25): 2-3 sentence elevator pitch.
3. **Status** (lines 27-35): `v1.0.0` released, `v1.1.pre` available (saga shutdown fix), link to CHANGELOG.
4. **Quickstart** (lines 37-60): 3-command TL;DR + extended steps for verification.
5. **Architecture** (lines 62-85): embed `c4-level-2.png` with 1-paragraph explanation + link to full C4 set.
6. **Features** (lines 87-115): table of what works (✅ list, expanded from current README) + what doesn't (❌ list).
7. **Tech stack** (lines 117-140): badges for Go, Redpanda, Postgres, Redis, OpenTelemetry, Helm, ArgoCD.
8. **Demo** (lines 142-160): embed asciinema if recording exists (Stage 8); otherwise link to `docs/demo/demo.sh`.
9. **Performance** (lines 162-180): table from `tests/load` results (50 VUs, p95 < 1000ms, throughput).
10. **Roadmap** (lines 182-200): v1.0 ✅, v1.1.pre ✅, v1.1.a-e 📋 — link to spec doc.
11. **Project structure** (lines 202-235): tree from repo root.
12. **Documentation** (lines 237-260): links to `docs/superpowers/specs/`, `docs/adr/`, `api/openapi.yaml`.
13. **Contributing** (lines 262-275): paragraph + link to issue templates.
14. **License** (lines 277-285): MIT + link.

**Commit:** one commit "docs: README rewrite with badges, rendered diagrams, roadmap".

### Stage 4 — Profile README

**Deliverable:** new public repo `github.com/t0pm1x/t0pm1x` containing `README.md` with:

- Intro: "Event-driven platform engineer. Building **orderflow** — a Go microservices reference for saga + outbox patterns."
- Featured projects: orderflow (with description + link), any other repos the user has.
- Tech stack badges.
- "Currently working on:" line with v1.1 stage.
- Contact section.

**Tooling:** `gh repo create t0pm1x/t0pm1x --public --description "GitHub profile README" --add-readme` then commit the actual README content.

**Commit (in the new repo):** single initial commit "Initial profile README".

### Stage 6 — Social preview PNG via PlantUML

**Deliverable:** `docs/assets/social-preview.png` (1280×640, < 1MB).

**Approach:** PlantUML deployment diagram styled to render as a banner card.

**Content:** project title "orderflow", tagline "Event-driven order processing with saga + outbox patterns", 4 colored rectangles for the 4 services (order/payment/inventory/saga), Go gopher silhouette or Go logo, version badge "v1.1.pre", small "github.com/t0pm1x/orderflow" footer.

**Tooling:** PlantUML (same as Stage 1). Render via `plantuml -tpng docs/assets/social-preview.puml`.

**Commit:** one commit "render: add social-preview.png for repo card display".

### Stage 7 — GitHub Releases × 9

**Deliverable:** 9 GitHub Releases for the existing tags.

| Tag | Title | Notes source | Type |
|---|---|---|---|
| `v0.1.0-MVP` | `v0.1.0-MVP` | `CHANGELOG.md` "0.1.0-MVP" section | stable |
| `v0.2.0` | `v0.2.0` | `CHANGELOG.md` "0.2.0" section | stable |
| `v0.3.0` | `v0.3.0` | `CHANGELOG.md` "0.3.0" section | stable |
| `v0.4.0` | `v0.4.0` | `CHANGELOG.md` "0.4.0" section | stable |
| `v0.5.0` | `v0.5.0` | `CHANGELOG.md` "0.5.0" section | stable |
| `v0.6.0` | `v0.6.0` | `CHANGELOG.md` "0.6.0" section | stable |
| `v1.0.0` | `v1.0.0` | `CHANGELOG.md` "1.0.0" section | **latest** |
| `v1.1.pre` | `v1.1.pre` | `CHANGELOG.md` "1.1.0-pre" section | **prerelease** |

**Note:** v1.0.0 must be marked `latest` (the most recent stable release). v1.1.pre is a prerelease, so GitHub's automatic "latest" detection should leave v1.0.0 as latest; verify after creation.

**Tooling:** `gh CLI` via `winget install --id GitHub.cli` then `gh auth login --with-token`.

**Create command (one per tag):**

```bash
gh release create v0.1.0-MVP \
  --title "v0.1.0-MVP" \
  --notes-file /tmp/release-notes-v0.1.0-MVP.md
```

Where each `/tmp/release-notes-vX.Y.Z.md` is the corresponding CHANGELOG section extracted verbatim.

**Verification:** visit `github.com/t0pm1x/orderflow/releases` and confirm 9 releases visible, v1.0.0 marked latest, v1.1.pre marked prerelease.

### Stage 8 — Asciinema recording (optional)

**Deliverable:** `docs/demo/orderflow.cast` (and possibly embedded GIF) capturing the full demo.

**Status:** OPTIONAL. Depends on Docker stack running successfully at execution time. Skip if blocked.

**Process:** start `docker compose up`, run `docs/demo/demo.sh`, record with `asciinema rec docs/demo/orderflow.cast`. Convert to GIF if needed via `agg` (asciinema gif generator).

**Commit:** "docs: add asciinema recording of happy-path demo".

### Stage 5 — Web UI settings (DEFERRED)

User will configure manually via GitHub web UI:

- Description (matches README hero)
- Topics: `go`, `microservices`, `kafka`, `redpanda`, `saga-pattern`, `outbox-pattern`, `opentelemetry`, `postgresql`, `kubernetes`, `event-driven-architecture`
- Wiki: disabled
- Projects: disabled (solo dev)
- Social preview: upload PNG from Stage 6

Not part of this plan's execution. Documented for completeness.

## Critical path

```
Stage 1 (render C4 PNG)
  ├──► Stage 3 (README, embeds PNGs)
  └──► Stage 6 (social preview, uses PlantUML)
            └──► Stage 5 (web UI manual, uploads PNG)

Stage 2 (community files) ──► independent

Stage 3 (README) ──► needs Stages 1 + 6
Stage 4 (profile README) ──► independent (separate repo)
Stage 7 (releases ×9) ──► independent (uses existing tags)
Stage 8 (recording) ──► optional, after Stage 3
```

## Cross-cutting decisions

| Decision | Value |
|---|---|
| Tooling for diagram rendering | PlantUML (`winget install --id PlantUML.PlantUML`) |
| Tooling for releases | gh CLI (`winget install --id GitHub.cli`) |
| Tooling for recording | asciinema (`winget install --id asciinema.asciinema`) |
| PR strategy | Multiple small PRs (one per stage) |
| Social preview style | PlantUML layout, not Pillow (no Python installed) |
| Release notes source | Extract verbatim from CHANGELOG.md sections |
| Latest release marker | Manual `gh release edit v1.0.0 --latest` after creating all |
| Stage 5 (web UI) | Deferred to user |
| Stage 8 (recording) | Optional, skip if blocked |

## Risk register

| Risk | Mitigation |
|---|---|
| PlantUML not in PATH after `winget install` | Verify with `plantuml -version` before Stage 1; fallback to `java -jar plantuml.jar` download |
| PlantUML rendering produces unreadable PNGs | Render one diagram first, inspect; adjust .puml themes if needed |
| `gh CLI` install fails (admin rights) | Use alternative: download GitHub CLI MSI from cli.github.com |
| Existing tags not all pushed to origin | Verify `git ls-remote origin refs/tags/v*` before Stage 7; push missing tags |
| Profile repo creation requires gh auth | Same auth flow as releases |
| New profile repo appears empty initially | `gh repo create --add-readme` creates initial empty README; commit real content immediately after |
| Asciinema recording requires full stack running (8GB RAM) | Skip Stage 8 if blocked; record is optional |
| README link check fails on broken anchors | Run `lychee` (or manual) link check before commit |
| PlantUML social preview looks "engineered" not "designed" | Acceptable tradeoff; can revisit later with Pillow |

## Dependencies

External tools to install before execution (Windows):

```bash
winget install --id PlantUML.PlantUML
winget install --id GitHub.cli
winget install --id asciinema.asciinema   # only if Stage 8 attempted
```

Git access: `github.com/t0pm1x/orderflow` (existing, authenticated via system git credential manager).

GH auth: `gh auth login --hostname github.com --with-token < token.txt` with a PAT that has `repo` and `write:packages` scopes.

## References

- `STATUS.md` — current project status
- `CHANGELOG.md` — release notes source
- `README.md` — to be rewritten in Stage 3
- `docs/architecture/*.puml` — PlantUML sources for Stage 1
- `docs/demo/RECORDING.md` — Stage 8 runbook (if needed)