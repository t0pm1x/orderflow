#!/usr/bin/env pwsh
# scripts/stop.ps1 — tear down the orderflow easy run (Windows).
#
# Kills the 5 service binaries (if running) and stops the docker
# compose infra. Volumes are preserved; pass -Volumes to drop them
# too (resets Postgres state — order_sagas, payments, reservations).

[CmdletBinding()]
param(
    [switch]$Volumes = $false
)

$ErrorActionPreference = 'Continue'

$root   = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$log    = Join-Path $root 'tests\logs'
$deploy = Join-Path $root 'deploy'

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "  ok: $msg" -ForegroundColor Green }

Step "Stopping service binaries"
$killed = $false
Get-Process order,payment,inventory,saga,web -ErrorAction SilentlyContinue |
    ForEach-Object {
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        Write-Host "  killed $($_.ProcessName) pid=$($_.Id)"
        $killed = $true
    }
foreach ($name in 'order','payment','inventory','saga','web') {
    Remove-Item (Join-Path $log "$name.pid") -ErrorAction SilentlyContinue
}
if (-not $killed) { Write-Host "  (none running)" }
Ok "service binaries stopped"

Step "Stopping docker compose infra"
$args = @('-f', (Join-Path $deploy 'docker-compose.yml'), 'down')
if ($Volumes) { $args += '-v' }
docker compose @args 2>&1 | Out-Null
if ($Volumes) { Ok "compose down -v (volumes dropped)" }
else { Ok "compose down (volumes preserved)" }