# Manual orchestrator that skips `make build` (not on Windows PATH).
# Brings up compose infra + starts the 5 binaries directly.

$ErrorActionPreference = 'Stop'
$root = 'C:\Users\t0p_m\projects\orderflow'
Set-Location $root

Write-Host "==> docker compose up (infra)"
docker compose -f deploy/docker-compose.yml up -d 2>&1 | Out-Null
Write-Host "==> waiting 60s for infra to be ready"
Start-Sleep -Seconds 60
docker ps --format "{{.Names}}\t{{.Status}}" | ForEach-Object { Write-Host "    $_" }

Write-Host ""
Write-Host "==> starting 5 service binaries"

$env:OTEL_EXPORTER = 'stdout'
$env:DATABASE_URL = 'postgres://orderflow:orderflow@localhost:5432/order_order?sslmode=disable'
$env:KAFKA_BROKER = 'localhost:9092'
$env:HTTP_ADDR   = ':8081'
Start-Process -FilePath .\bin\order.exe -PassThru -RedirectStandardOutput docs/demo/logs/order.log -RedirectStandardError docs/demo/logs/order.err.log | Out-Null

$env:DATABASE_URL = 'postgres://orderflow:orderflow@localhost:5433/payment_payment?sslmode=disable'
$env:KAFKA_BROKER = 'localhost:9092'
$env:HTTP_ADDR   = ':8082'
Start-Process -FilePath .\bin\payment.exe -PassThru -RedirectStandardOutput docs/demo/logs/payment.log -RedirectStandardError docs/demo/logs/payment.err.log | Out-Null

$env:DATABASE_URL = 'postgres://orderflow:orderflow@localhost:5434/inventory_inventory?sslmode=disable'
$env:KAFKA_BROKER = 'localhost:9092'
$env:REDIS_URL   = 'redis://localhost:6379/0'
$env:HTTP_ADDR   = ':8083'
Start-Process -FilePath .\bin\inventory.exe -PassThru -RedirectStandardOutput docs/demo/logs/inventory.log -RedirectStandardError docs/demo/logs/inventory.err.log | Out-Null

$env:DATABASE_URL = 'postgres://orderflow:orderflow@localhost:5432/order_order?sslmode=disable'
$env:KAFKA_BROKER = 'localhost:9092'
$env:HTTP_ADDR   = ':8084'
Start-Process -FilePath .\bin\saga.exe -PassThru -RedirectStandardOutput docs/demo/logs/saga.log -RedirectStandardError docs/demo/logs/saga.err.log | Out-Null

Remove-Item Env:DATABASE_URL,Env:KAFKA_BROKER -ErrorAction SilentlyContinue
$env:ORDER_URL    = 'http://localhost:8081'
$env:PAYMENT_URL  = 'http://localhost:8082'
$env:INVENTORY_URL = 'http://localhost:8083'
$env:KAFKA_BROKERS = 'localhost:9092'
$env:HTTP_ADDR    = ':8085'
Start-Process -FilePath .\bin\web.exe -PassThru -RedirectStandardOutput docs/demo/logs/web.log -RedirectStandardError docs/demo/logs/web.err.log | Out-Null

Write-Host "==> waiting 10s for services to boot"
Start-Sleep -Seconds 10

function Get-HttpCode([string]$url) {
    try { (Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 2).StatusCode } catch { return 0 }
}

$start = Get-Date
$ready = $false
while (-not $ready) {
    Start-Sleep -Seconds 1
    $elapsed = (Get-Date) - $start
    if ($elapsed.TotalSeconds -gt 90) { Write-Host "==> timeout"; break }
    $o = (Get-HttpCode 'http://localhost:8081/healthz') -eq 200
    $p = (Get-HttpCode 'http://localhost:8082/healthz') -eq 200
    $i = (Get-HttpCode 'http://localhost:8083/healthz') -eq 200
    $s = (Get-HttpCode 'http://localhost:8084/healthz') -eq 200
    $w = (Get-HttpCode 'http://localhost:8085/healthz') -eq 200
    Write-Host ("    t={0,2}s  order={1}  payment={2}  inventory={3}  saga={4}  web={5}" -f [int]$elapsed.TotalSeconds,$o,$p,$i,$s,$w)
    $ready = ($o -and $p -and $i -and $s -and $w)
}

Write-Host ""
Write-Host "==> summary: order=$o payment=$p inventory=$i saga=$s web=$w"

if ($w) {
    Write-Host ""
    Write-Host "*** Playground LIVE at http://localhost:8085 ***"
    Write-Host "    /                 orders list"
    Write-Host "    /orders/new       submit a new order"
    Write-Host "    /orders/{id}      order detail (polls every 1s)"
    Write-Host "    /inventory        per-SKU stock"
    Write-Host "    /payments/sim     force-success/fail"
    Write-Host "    /events/stream    live saga tail (SSE)"
    Write-Host ""
    Write-Host "Press any key to tear down..."
    [void]$Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown')
}

Write-Host ""
Write-Host "==> teardown"
Get-Process order,payment,inventory,saga,web -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
docker compose -f deploy/docker-compose.yml down -v 2>&1 | Out-Null
Write-Host "==> done."
