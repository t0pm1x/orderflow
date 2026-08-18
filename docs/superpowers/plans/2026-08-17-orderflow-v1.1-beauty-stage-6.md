# orderflow Stage 6 — Social preview PNG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Generate a 1280×640 social preview PNG using PlantUML, commit it, and push to origin. Single new commit, no code changes.

**Architecture:** PlantUML deployment diagram with custom layout/colors/spacing to render as a banner card. The same PlantUML toolchain from Stage 1 is used (jar at `%LOCALAPPDATA%\PlantUML\plantuml.jar`, Java 8 + PlantUML v1.2024.7).

**Tech Stack:** PlantUML (already installed from Stage 1), Java 8.

## Global Constraints

- No code changes — presentation only.
- Working directory is the existing worktree `C:\Users\t0p_m\projects\orderflow-v1.1.pre`, branch `v1.1.pre`.
- Output file: `docs/assets/social-preview.png` (1280×640, < 1 MB).
- Use PlantUML via the shim at `%LOCALAPPDATA%\PlantUML\plantuml.cmd` (set `$env:Path` in PowerShell session).
- Java 8 + PlantUML v1.2024.7 — confirmed working in Stage 1.
- Branch already contains 7 saga-fix commits + 2 Stage 1 commits + 1 Stage 2 commit. New commit appends.
- No new tooling required.

---

## Task 1: Design and render social preview PNG

**Files:**
- Create: `docs/assets/social-preview.puml` (PlantUML source)
- Create: `docs/assets/social-preview.png` (rendered output)

- [ ] **Step 1.1: Verify PlantUML is reachable**

Open a new PowerShell session and run:

```powershell
$env:Path = "$env:LOCALAPPDATA\PlantUML;$env:Path"
plantuml -version
```

Expected: prints PlantUML version (should be v1.2024.7 from Stage 1). If the shim is missing, fall back to direct jar invocation:

```powershell
java -jar "$env:LOCALAPPDATA\PlantUML\plantuml.jar" -version
```

- [ ] **Step 1.2: Create the PlantUML source**

Create `docs/assets/social-preview.puml` with the following content. This produces a 1280×640 banner card with the project name, tagline, four service rectangles, and a small footer.

```plantuml
@startuml social-preview
skinparam backgroundColor #0D1117
skinparam dpi 150
skinparam DefaultFontName "Segoe UI"
skinparam DefaultFontSize 18
skinparam DefaultFontColor #E6EDF3

title "<size:48><b>orderflow</b></size>\n<size:24>Event-driven order processing with saga + outbox patterns</size>" orderflow

rectangle "<size:20><b>order</b></size>\n<size:14>REST API +\nstate machine</size>" as ORDER #58A6FF
rectangle "<size:20><b>payment</b></size>\n<size:14>Mock provider +\nidempotency</size>" as PAYMENT #A371F7
rectangle "<size:20><b>inventory</b></size>\n<size:14>Optimistic\nlocking</size>" as INVENTORY #3FB950
rectangle "<size:20><b>saga</b></size>\n<size:14>State machine +\nTTL watchdog</size>" as SAGA #F78166

ORDER -[#58A6FF,thickness=2]-> SAGA : emits
SAGA -[#F78166,thickness=2]-> PAYMENT : requests
SAGA -[#F78166,thickness=2]-> INVENTORY : reserves

center bottom
<size:16>4 Go microservices · Kafka · Postgres · OpenTelemetry · Kubernetes</size>\n<size:14>github.com/t0pm1x/orderflow</size>
end title

center right
<size:14>v1.1.pre</size>
end title

@enduml
```

Notes:
- 1280×640 PNG at 150 dpi gives ~ 12 inches × 4.3 inches. PlantUML will size the layout based on content + dpi.
- Colors are GitHub dark-theme inspired (#0D1117 bg, #E6EDF3 text, accent colors).
- If the rendered image is smaller than 1280×640, scale by increasing `dpi` (e.g., 200) until width is ~1280 px.
- If colors render incorrectly, verify PlantUML version supports `skinparam` background (v1.2024.7 does).

- [ ] **Step 1.3: Render the PNG**

```powershell
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
$env:Path = "$env:LOCALAPPDATA\PlantUML;$env:Path"
plantuml -tpng docs/assets/social-preview.puml
```

Expected: writes `docs/assets/social-preview.png` next to the source.

- [ ] **Step 1.4: Verify dimensions**

```powershell
$img = [System.IO.File]::OpenRead("docs/assets/social-preview.png")
$hdr = New-Object byte[] 24
$img.Read($hdr, 0, 24) | Out-Null
$img.Close()
$w = [BitConverter]::ToInt32($hdr[16..19], 0)
$h = [BitConverter]::ToInt32($hdr[20..23], 0)
Write-Host "Dimensions: ${w}x${h}"
```

Expected: width between 1200 and 1300, height between 600 and 700. If too small, increase `dpi` in the .puml (re-render). If too large, decrease `dpi`. Iterate until close to 1280×640.

If pure PlantUML output is consistently too small or even in size, alternative: render then add white padding to reach 1280×640 (skip this if dimensions are already close).

- [ ] **Step 1.5: Verify file size**

```powershell
(Get-Item docs/assets/social-preview.png).Length
```

Expected: < 1,048,576 bytes (1 MB). If too large, reduce `dpi` and re-render.

- [ ] **Step 1.6: Visual verification**

Open `docs/assets/social-preview.png` in any image viewer and confirm:
- Dark background
- "orderflow" title prominently displayed
- Tagline "Event-driven order processing with saga + outbox patterns"
- Four rectangles labeled `order`, `payment`, `inventory`, `saga` with brief descriptions
- Arrows between them
- "v1.1.pre" version badge
- Footer with tech stack and repo URL

If anything is missing or illegible, edit `docs/assets/social-preview.puml` and re-render.

- [ ] **Step 1.7: Commit the PNG**

```powershell
cd "C:\Users\t0p_m\projects\orderflow-v1.1.pre"
git add docs/assets/social-preview.png docs/assets/social-preview.puml
git commit -m "render: social-preview.png (1280x640 GitHub social card)"
```

- [ ] **Step 1.8: Push to origin**

```powershell
git push origin refs/heads/v1.1.pre:refs/heads/v1.1.pre
```

If the bare `git push origin v1.1.pre` form fails due to branch/tag shadowing (as in Stages 1 and 2), use the explicit `refs/heads/v1.1.pre:refs/heads/v1.1.pre` form above.

---

## Self-Review Checklist

- [x] **Spec coverage:** Stage 6 of v1.1.0-beauty design requires exactly one new file `docs/assets/social-preview.png` (1280×640, < 1MB). Created in Task 1.
- [x] **Placeholder scan:** No "TBD"/"TODO"/"fill in"/"similar to Task N". Every step has exact commands.
- [x] **Type consistency:** Output path matches spec exactly.
- [x] **PR boundary:** This stage is presentation-only; adding to existing PR #2 is fine, or user can open separate PR. No separate PR required.

## Plan Complete

After this plan finishes:
- `docs/assets/social-preview.png` (1280×640, < 1MB) is committed and pushed to `origin/v1.1.pre`.
- Branch is now ready for the README rewrite (Stage 3) which will reference this PNG.
- User can download the PNG from the PR and upload it to GitHub → Settings → Social preview in the web UI.