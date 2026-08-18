# orderflow Stage 2 — Community files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 4 GitHub community files (PR template, 2 issue templates, CODEOWNERS, dependabot.yml) to `.github/`. Single PR, no code changes.

**Architecture:** Direct file additions under `.github/`. Each file is a static text artifact (Markdown or YAML); no logic, no dependencies. Files are validated by GitHub natively when PRs and issues are opened.

**Tech Stack:** Markdown (`.md`), YAML (`.yml`), GitHub Actions schema (for dependabot.yml).

## Global Constraints

- No code changes — this is a documentation/community-only commit.
- Working directory is the existing worktree at `C:\Users\t0p_m\projects\orderflow-v1.1.pre`, branch `v1.1.pre`.
- All files land in `.github/`. No other directory is modified.
- Existing `.github/workflows/ci.yml` is preserved (not modified).
- The branch `v1.1.pre` already has commits from v1.1.pre saga-fix work AND Stage 1 (C4 PNGs). New commits add to this branch.
- Verification: YAML lint on `dependabot.yml`; visual review of markdown files.
- No external tooling required.

---

## Task 1: Add 5 community files

**Files:**
- Create: `.github/PULL_REQUEST_TEMPLATE.md`
- Create: `.github/ISSUE_TEMPLATE/bug_report.md`
- Create: `.github/ISSUE_TEMPLATE/feature_request.md`
- Create: `.github/CODEOWNERS`
- Create: `.github/dependabot.yml`

**Why single task:** All 5 files are small, related, and land in one commit. Splitting them across multiple commits adds no value.

**Step-by-step:**

- [ ] **Step 1.1: Verify current `.github/` state**

```bash
ls -la .github/
ls -la .github/ISSUE_TEMPLATE/ 2>/dev/null || echo "no ISSUE_TEMPLATE yet"
```

Expected: `.github/workflows/ci.yml` exists. `ISSUE_TEMPLATE/` does not exist yet. The new files will create `ISSUE_TEMPLATE/` and add 4 new top-level files alongside `workflows/`.

- [ ] **Step 1.2: Create `.github/PULL_REQUEST_TEMPLATE.md`**

Content (verbatim from design spec):

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

- [ ] **Step 1.3: Create `.github/ISSUE_TEMPLATE/bug_report.md`**

Content:

```markdown
---
name: Bug report
about: Report incorrect behavior or a defect
title: "[bug] "
labels: bug
---

## Summary

<!-- One-sentence description of the bug -->

## Steps to reproduce

1.
2.
3.

## Expected behavior

<!-- What did you expect to happen? -->

## Actual behavior

<!-- What actually happened? Include error messages. -->

## Environment

- OS:
- Go version (`go version`):
- orderflow commit/tag:
- Deployment (local docker compose / kind / k8s):

## Logs

```
<!-- Paste relevant logs here. -->
```

## Screenshots

<!-- If applicable -->
```

- [ ] **Step 1.4: Create `.github/ISSUE_TEMPLATE/feature_request.md`**

Content:

```markdown
---
name: Feature request
about: Suggest a new feature or enhancement
title: "[feature] "
labels: enhancement
---

## Problem

<!-- What problem does this feature solve? -->

## Proposed solution

<!-- What do you want to happen? -->

## Alternatives considered

<!-- What other approaches did you consider? -->

## Additional context

<!-- Links, screenshots, related issues, ADR references -->
```

- [ ] **Step 1.5: Create `.github/CODEOWNERS`**

Content:

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

- [ ] **Step 1.6: Create `.github/dependabot.yml`**

Content:

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

- [ ] **Step 1.7: Validate YAML file**

```bash
# Quick syntactic check (no schema validation tool installed locally)
python -c "import yaml; yaml.safe_load(open('.github/dependabot.yml'))" 2>/dev/null || \
  echo "Python not available, skipping YAML parse check"
```

Or use any other YAML parser available. The schema validation happens at GitHub when the file is committed.

If `python -c "import yaml"` fails (Python not installed — confirmed in Stage 1 setup), fall back to a simple text-based check:

```bash
# Verify required keys are present
grep -q "version: 2" .github/dependabot.yml && \
grep -q "package-ecosystem: \"gomod\"" .github/dependabot.yml && \
grep -q "package-ecosystem: \"github-actions\"" .github/dependabot.yml && \
echo "YAML keys present" || echo "YAML keys missing"
```

- [ ] **Step 1.8: Verify all 5 files exist and have content**

```bash
ls -la .github/PULL_REQUEST_TEMPLATE.md .github/ISSUE_TEMPLATE/bug_report.md .github/ISSUE_TEMPLATE/feature_request.md .github/CODEOWNERS .github/dependabot.yml
```

Expected: all 5 files exist, none empty.

- [ ] **Step 1.9: Commit all 5 files**

```bash
git add .github/PULL_REQUEST_TEMPLATE.md .github/ISSUE_TEMPLATE/bug_report.md .github/ISSUE_TEMPLATE/feature_request.md .github/CODEOWNERS .github/dependabot.yml
git commit -m "feat: add GitHub community files (PR template, issue templates, CODEOWNERS, dependabot)"
```

- [ ] **Step 1.10: Push to origin**

```bash
git push origin v1.1.pre
```

If `git push origin v1.1.pre` fails due to branch/tag shadowing (as in Stage 1), use:

```bash
git push origin refs/heads/v1.1.pre:refs/heads/v1.1.pre
```

---

## Task 2: Open PR on GitHub

**Files:** none modified; PR created on github.com.

- [ ] **Step 2.1: Check for pre-existing PRs**

Visit https://github.com/t0pm1x/orderflow/pulls and look for any open PRs from `v1.1.pre` → `main`. The Stage 1 PR (C4 PNGs) may or may not exist yet. Earlier v1.1.pre → main PRs from the saga-fix work may also exist.

Decision logic:
- If **no PRs** for v1.1.pre → main exist: open a fresh one at the URL below.
- If **one open PR** exists: edit it to include the new commit (commit `8a4f...` will appear in the diff after push).
- If **one closed/merged PR** exists: open a fresh one.

- [ ] **Step 2.2: Open the comparison page**

Visit: https://github.com/t0pm1x/orderflow/compare/main...v1.1.pre?expand=1

GitHub will show all changes from `main` to current `v1.1.pre` HEAD. This will include Stage 1 (C4 PNGs) + this Stage 2 (community files) + all earlier v1.1.pre saga-fix commits.

- [ ] **Step 2.3: If creating a new PR, fill title and body**

**Title:** `feat: add GitHub community files (PR template, issue templates, CODEOWNERS, dependabot)`

**Body:**

```markdown
## What

Adds the standard GitHub community files for an open-source project:
- `PULL_REQUEST_TEMPLATE.md` — structured PR description with description, type-of-change checkboxes, testing checklist
- `ISSUE_TEMPLATE/bug_report.md` — structured bug reports
- `ISSUE_TEMPLATE/feature_request.md` — structured feature requests
- `CODEOWNERS` — auto-assigns @t0pm1x as reviewer (solo maintainer)
- `dependabot.yml` — weekly auto-PRs for `gomod` and `github-actions` updates

## Why

Pre-1.0 the repo had no PR/issue templates, no CODEOWNERS, and no Dependabot config. Adding these:
- Makes PR review more uniform (everyone fills the same sections)
- Lowers the bar for filing good bug reports / feature requests
- Auto-assigns reviewers so nothing lands unreviewed
- Keeps dependencies current via Dependabot PRs (with auto-merge once CI is green)

## Files

- `.github/PULL_REQUEST_TEMPLATE.md` (new)
- `.github/ISSUE_TEMPLATE/bug_report.md` (new)
- `.github/ISSUE_TEMPLATE/feature_request.md` (new)
- `.github/CODEOWNERS` (new)
- `.github/dependabot.yml` (new)

No code changes. No existing files modified.

## Test plan

- [ ] Open a test PR — confirm `PULL_REQUEST_TEMPLATE.md` populates the description
- [ ] File a test issue, choose "Bug report" — confirm form renders
- [ ] File a test issue, choose "Feature request" — confirm form renders
- [ ] Visit a file in the repo, look at "Blame" or "Owners" UI — confirm @t0pm1x is listed
- [ ] Visit Insights → Dependency graph → Dependabot — confirm version updates are scheduled

## Followup

This PR is part of v1.1.0-beauty (GitHub polish). See `docs/superpowers/specs/2026-08-17-orderflow-v1.1.beauty-design.md`.
```

If you are **editing an existing PR** (rather than creating a new one), add a comment instead:

```markdown
Added community files in commit <SHA>. Same change set as the title/body above, just abbreviated.
```

- [ ] **Step 2.4: Click "Create pull request" or save the edit**

The PR is open. CI (`ci.yml`) will run build matrix + lint + e2e — all should pass since no Go code changed.

---

## Self-Review Checklist

- [x] **Spec coverage:** Stage 2 of the v1.1.0-beauty design requires exactly 5 files in `.github/` (PULL_REQUEST_TEMPLATE, 2 issue templates, CODEOWNERS, dependabot.yml). All 5 created in Task 1.
- [x] **Placeholder scan:** No "TBD"/"TODO"/"fill in"/"similar to Task N" in plan. Every step has exact content.
- [x] **Type consistency:** File paths match the design spec exactly.
- [x] **PR boundary:** Single logical change (5 community files), one commit. The PR step is the same as Stage 1 (manual web UI).

## Plan Complete

After this plan finishes:
- 5 files exist in `.github/` (commit `feat: add GitHub community files...`).
- Branch `v1.1.pre` is pushed to origin with the new commit.
- PR is open (or an existing PR is updated).
- Stage 6 (social preview PNG) and Stage 3 (README rewrite) become unblocked.