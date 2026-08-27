#!/usr/bin/env pwsh
# scripts/run.ps1 — easy run for orderflow (Windows).
#
# Brings up the full local stack: docker compose infra (postgres x3,
# redis, redpanda, kafka-init, otel-collector, prometheus, tempo,
# grafana), builds the 5 service binaries via `make build`, starts
# them with the correct env vars, and waits for every service to
# pass /healthz.
#
# Tear down with scripts/stop.ps1 (kills the binaries + stops
# docker compose, volumes preserved). Drop volumes with
# `docker compose -f deploy/docker-compose.yml down -v` to reset
# Postgres state.
#
# Usage:
#   pwsh -ExecutionPolicy Bypass -File scripts\run.ps1
#   pwsh -ExecutionPolicy Bypass -File scripts\run.ps1 -NoBuild
#
# Requires: Docker Desktop running, Go 1.25+, GNU Make, Node.js
# 20+ (only for the SPA build step that `make build` runs before
# the Go build). ~4 GB RAM.
# Logs land in tests\logs\<svc>.log (one per service).
#
# Compatible with both Windows PowerShell 5.1 (the default that
# ships with Windows) and PowerShell 7+. Invoke it from cmd.exe /
# PowerShell with:
#     powershell -ExecutionPolicy Bypass -File scripts\run.ps1
# or, if you've installed PowerShell 7 (`winget install
# Microsoft.PowerShell`):
#     pwsh -ExecutionPolicy Bypass -File scripts\run.ps1

[CmdletBinding()]
param(
    [switch]$NoBuild = $false
)

$ErrorActionPreference = 'Continue'

$root   = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$bin    = Join-Path $root 'bin'
$log    = Join-Path $root 'tests\logs'
$deploy = Join-Path $root 'deploy'

New-Item -ItemType Directory -Force -Path $log | Out-Null

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "  ok: $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "  !! $msg" -ForegroundColor Yellow }
function Die($msg)  { Write-Host "  X  $msg" -ForegroundColor Red; exit 1 }

# ---- 0. docker daemon reachable ----
Step "Verifying Docker daemon"
docker info >$null 2>&1
if ($LASTEXITCODE -ne 0) {
    Die "Docker daemon is not reachable. Start Docker Desktop from the system tray and re-run."
}
Ok "docker daemon reachable"

# ---- 1. kill stale local binaries ----
Step "Killing stale service binaries (best-effort)"
$killed = $false
Get-Process order,payment,inventory,saga,web -ErrorAction SilentlyContinue |
    ForEach-Object {
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        Write-Host "  killed $($_.ProcessName) pid=$($_.Id)"
        $killed = $true
    }
if (-not $killed) { Write-Host "  (none running)" }
Start-Sleep -Seconds 1

# ---- 2. bring up infra ----
Step "Bringing up docker compose infra (postgres x3, redis, redpanda, otel, prometheus, tempo, grafana)"
$composeArgs = @(
    '-f', (Join-Path $deploy 'docker-compose.yml')
    'up', '-d'
    'postgres-order', 'postgres-payment', 'postgres-inventory'
    'redis', 'redpanda', 'kafka-init'
    'otel-collector', 'prometheus', 'tempo', 'grafana'
)
docker compose @composeArgs 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Die "docker compose up failed" }
Ok "compose up"

# ---- 2a. wait for postgres + redpanda healthchecks ----
Step "Waiting for postgres-order/payment/inventory + redpanda healthchecks"
$sw = [Diagnostics.Stopwatch]::StartNew()
$critical = 'postgres-order','postgres-payment','postgres-inventory','redpanda'
while ($sw.Elapsed -lt [TimeSpan]::FromSeconds(60)) {
    $ok = $true
    foreach ($svc in $critical) {
        $state = docker inspect --format '{{.State.Health.Status}}' "deploy-$svc-1" 2>$null
        if ($state -ne 'healthy') { $ok = $false; break }
    }
    if ($ok) { Ok "infra healthy in $([int]$sw.Elapsed.TotalSeconds)s"; break }
    Start-Sleep -Seconds 2
}
if (-not $ok) {
    foreach ($svc in $critical) {
        $state = docker inspect --format '{{.State.Health.Status}}' "deploy-$svc-1" 2>$null
        Warn "$svc health=$state"
    }
    Die "infra failed to become healthy in 60s"
}

# ---- 3. saga migrations on order DB ----
# The saga service shares the order PG (DATABASE_URL points at
# the order DB). deploy/postgres/init-order.sh only applies order
# migrations, so the saga's schema (order_sagas, saga_outbox) must
# be applied here too. Without these the saga runtime fails on
# every event with `relation "order_sagas" does not exist`.
Step "Saga migrations on order DB (saga shares the order PG)"
$hasSagas = docker exec deploy-postgres-order-1 psql -U orderflow -d order_order -tAc "SELECT 1 FROM pg_tables WHERE tablename='order_sagas'" 2>$null
if ($hasSagas -ne '1') {
    # order_sagas missing entirely: apply the full saga migration
    # set (0001_init, 0002_saga_outbox, 0003_payment_last_four).
    # 0001+0002 are unconditional CREATE TABLE / ALTER TABLE; 0003
    # uses ADD COLUMN IF NOT EXISTS so re-runs are idempotent.
    $sagaMigDir = Join-Path $root 'services\saga\migrations'
    foreach ($f in '0001_init.sql','0002_saga_outbox.sql','0003_saga_payment_last_four.sql') {
        Get-Content (Join-Path $sagaMigDir $f) |
            docker exec -i deploy-postgres-order-1 psql -U orderflow -d order_order 2>&1 | Out-Null
        Ok "applied saga migration $f"
    }
} else {
    $hasLastFour = docker exec deploy-postgres-order-1 psql -U orderflow -d order_order -tAc "SELECT 1 FROM information_schema.columns WHERE table_name='order_sagas' AND column_name='last_four'" 2>$null
    if ($hasLastFour -ne '1') {
        # Existing pre-v1.1.5 install: order_sagas exists but the
        # last_four column is missing. Apply just 0003.
        Get-Content (Join-Path $root 'services\saga\migrations\0003_saga_payment_last_four.sql') |
            docker exec -i deploy-postgres-order-1 psql -U orderflow -d order_order 2>&1 | Out-Null
        Ok "applied 0003_saga_payment_last_four.sql"
    } else {
        Ok "order_sagas.last_four already present"
    }
}

# ---- 4. build binaries ----
if ($NoBuild) {
    Step "Skipping build (-NoBuild)"
    Ok "using existing binaries in bin/"
} else {
    Step "Building binaries (make build)"
    & make -C $root build 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { Die "make build failed" }
    Ok "binaries up-to-date in bin/"
}

# ---- 5. start services ----
Step "Starting order/payment/inventory/saga/web"
$env:OTEL_EXPORTER = 'stdout'

function Start-Svc {
    param($name, $exe, $envHash)
    foreach ($k in $envHash.Keys) {
        if ($envHash[$k]) {
            [Environment]::SetEnvironmentVariable($k, $envHash[$k], 'Process')
        } else {
            # PowerShell 5.1 doesn't resolve `Env:$k` inside double-quoted
            # strings for `Remove-Item`; use the .NET API which works on
            # both 5.1 and 7+.
            [Environment]::SetEnvironmentVariable($k, $null, 'Process')
        }
    }
    $p = Start-Process -FilePath $exe `
        -RedirectStandardOutput "$log\$name.log" `
        -RedirectStandardError  "$log\$name.err" `
        -NoNewWindow -PassThru
    Ok "$name pid=$($p.Id)"
}

Start-Svc 'order' "$bin\order.exe" @{
    DATABASE_URL = 'postgres://orderflow:orderflow@127.0.0.1:5432/order_order?sslmode=disable'
    KAFKA_BROKERS = '127.0.0.1:9092'
    HTTP_ADDR    = '127.0.0.1:8081'
}
Start-Svc 'payment' "$bin\payment.exe" @{
    DATABASE_URL = 'postgres://orderflow:orderflow@127.0.0.1:5433/payment_payment?sslmode=disable'
    KAFKA_BROKERS = '127.0.0.1:9092'
    HTTP_ADDR    = '127.0.0.1:8082'
}
Start-Svc 'inventory' "$bin\inventory.exe" @{
    DATABASE_URL = 'postgres://orderflow:orderflow@127.0.0.1:5434/inventory_inventory?sslmode=disable'
    KAFKA_BROKERS = '127.0.0.1:9092'
    REDIS_URL    = 'redis://127.0.0.1:6379'
    HTTP_ADDR    = '127.0.0.1:8083'
}
Start-Svc 'saga' "$bin\saga.exe" @{
    DATABASE_URL = 'postgres://orderflow:orderflow@127.0.0.1:5432/order_order?sslmode=disable'
    KAFKA_BROKERS = '127.0.0.1:9092'
    HTTP_ADDR    = '127.0.0.1:8084'
}
Start-Svc 'web' "$bin\web.exe" @{
    ORDER_URL     = 'http://127.0.0.1:8081'
    PAYMENT_URL   = 'http://127.0.0.1:8082'
    INVENTORY_URL = 'http://127.0.0.1:8083'
    KAFKA_BROKERS = '127.0.0.1:9092'
    HTTP_ADDR     = '127.0.0.1:8085'
}

# ---- 6. healthcheck ----
Step "Healthchecks"
$ports = @{ order=8081; payment=8082; inventory=8083; saga=8084; web=8085 }
$sw = [Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt [TimeSpan]::FromSeconds(30)) {
    $allOk = $true
    foreach ($svc in $ports.Keys) {
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:$($ports[$svc])/healthz" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -ne 200) { $allOk = $false }
        } catch { $allOk = $false }
    }
    if ($allOk) { break }
    Start-Sleep -Seconds 1
}
foreach ($svc in $ports.Keys) {
    try {
        $r = Invoke-WebRequest -Uri "http://127.0.0.1:$($ports[$svc])/healthz" -UseBasicParsing -TimeoutSec 2
        Ok "$svc :$($ports[$svc]) -> $($r.StatusCode)"
    } catch { Warn "$svc :$($ports[$svc]) -> DOWN" }
}

# ---- 7. summary ----
Step "READY"
$summary = @(
    '',
    '  Web UI           :  http://127.0.0.1:8085',
    '  Order Service    :  http://127.0.0.1:8081   (POST/GET/LIST/DELETE /v1/orders)',
    '  Payment Service  :  http://127.0.0.1:8082',
    '  Inventory Service:  http://127.0.0.1:8083',
    '  Saga Service     :  http://127.0.0.1:8084',
    '',
    '  Grafana          :  http://127.0.0.1:3000   (admin / admin)',
    '  Prometheus       :  http://127.0.0.1:9091',
    '  Tempo            :  http://127.0.0.1:3200',
    '',
    '  Logs (tail-able):',
    ('    ' + (Join-Path $log 'order.log')),
    ('    ' + (Join-Path $log 'payment.log')),
    ('    ' + (Join-Path $log 'inventory.log')),
    ('    ' + (Join-Path $log 'saga.log')),
    ('    ' + (Join-Path $log 'web.log')),
    '',
    '  Smoke test (happy path):',
    '    curl -X POST http://127.0.0.1:8081/v1/orders -H ''Content-Type: application/json'' -d @examples\order.json',
    '    curl http://127.0.0.1:8081/v1/orders/<id-from-201>',
    '',
    '  Smoke test (compensation: last_four=0001 -> declined -> state=cancelled):',
    '    curl -X POST http://127.0.0.1:8081/v1/orders -H ''Content-Type: application/json'' -d "{\"customer_id\":\"8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f\",\"items\":[{\"sku\":\"SKU-001\",\"quantity\":1,\"unit_price_cents\":1999}],\"payment\":{\"last_four\":\"0001\"}}"',
    '    curl http://127.0.0.1:8081/v1/orders/<id-from-201>',
    '',
    '  Tear down:',
    '    pwsh -ExecutionPolicy Bypass -File scripts\stop.ps1',
    '    docker compose -f deploy\docker-compose.yml down',
    ''
)
$summary | ForEach-Object { Write-Host $_ -ForegroundColor White }