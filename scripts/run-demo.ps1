# PowerShell orchestrator that runs docs/demo/demo.sh via Git Bash,
# polls the playground until it's ready, then signals teardown.

param(
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
$root = 'C:\Users\t0p_m\projects\orderflow'
Set-Location $root

$bash = 'C:\Program Files\Git\bin\bash.exe'
if (-not (Test-Path $bash)) { throw "Git Bash not found at $bash" }

Write-Host "==> launching demo.sh via Git Bash (background) ..."
$proc = Start-Process -FilePath $bash `
    -ArgumentList @('docs/demo/demo.sh') `
    -WorkingDirectory $root `
    -RedirectStandardOutput (Join-Path $env:TEMP 'demo.sh.out.log') `
    -RedirectStandardError (Join-Path $env:TEMP 'demo.sh.err.log') `
    -PassThru -WindowStyle Normal
Write-Host "    demo.sh PID: $($proc.Id)"

function Get-HttpCode([string]$url) {
    try { (Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 2).StatusCode } catch { return 0 }
}

$startTime = Get-Date
$orderReady = $false
$paymentReady = $false
$invReady = $false
$sagaReady = $false
$webReady = $false
$webUrl = 'http://localhost:8085'
$allReady = $false

while (-not $allReady) {
    Start-Sleep -Seconds 1
    $elapsed = (Get-Date) - $startTime
    if ($elapsed.TotalSeconds -gt $TimeoutSeconds) {
        Write-Host "==> timeout ($TimeoutSeconds s) -- checking what we have"
        break
    }
    if (-not $orderReady) { $orderReady = (Get-HttpCode 'http://localhost:8081/healthz') -eq 200 }
    if (-not $paymentReady) { $paymentReady = (Get-HttpCode 'http://localhost:8082/healthz') -eq 200 }
    if (-not $invReady) { $invReady = (Get-HttpCode 'http://localhost:8083/healthz') -eq 200 }
    if (-not $sagaReady) { $sagaReady = (Get-HttpCode 'http://localhost:8084/healthz') -eq 200 }
    if (-not $webReady) { $webReady = (Get-HttpCode "$webUrl/healthz") -eq 200 }
    Write-Host ("    t={0,3}s  order={1}  payment={2}  inventory={3}  saga={4}  web={5}" -f `
        [int]$elapsed.TotalSeconds, $orderReady, $paymentReady, $invReady, $sagaReady, $webReady)
    $allReady = ($orderReady -and $paymentReady -and $invReady -and $sagaReady -and $webReady)
}

Write-Host ""
Write-Host "==> service readiness:"
Write-Host "    order=$orderReady  payment=$paymentReady  inventory=$invReady  saga=$sagaReady  web=$webReady"

if ($webReady) {
    Write-Host ""
    Write-Host "*** Playground live at $webUrl -- open in your browser ***"
    Write-Host "*** log files: $($root)\docs\demo\logs\ ***"
    Write-Host ""
    Write-Host "    /                 orders list"
    Write-Host "    /orders/new       submit a new order"
    Write-Host "    /orders/{id}      order detail (polls every 1s while non-terminal)"
    Write-Host "    /inventory        per-SKU stock viewer"
    Write-Host "    /payments/sim     force-success / force-fail webhook simulator"
    Write-Host "    /events/stream    live saga event tail (SSE)"
    Write-Host ""
    Write-Host "Press Ctrl+C here to stop. demo.sh will tear everything down."
    $idleLimit = 60
    $idle = 0
    while ($true) {
        Start-Sleep -Seconds 5
        if ((Get-HttpCode "$webUrl/healthz") -eq 200) {
            $idle = 0
        } else {
            $idle = $idle + 5
            if ($idle -ge $idleLimit) {
                Write-Host "==> web stopped responding for ${idle}s; tearing down"
                break
            }
        }
    }
}

Write-Host ""
Write-Host "==> teardown: killing demo.sh + ensure compose down"
if (-not $proc.HasExited) {
    Start-Sleep -Seconds 1
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 2
try { docker compose -f deploy/docker-compose.yml down -v 2>&1 | Out-Null } catch {}

foreach ($svc in @('order','payment','inventory','saga','web')) {
    Get-Process -Name $svc -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}

Write-Host "==> done."
