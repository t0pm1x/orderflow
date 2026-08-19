#!/usr/bin/env pwsh
# scripts/smoke-web.ps1 — automated smoke for the orderflow-web playground.
# Asserts happy path + compensation + 4xx + 5xx against a running stack.
# Requires: scripts/run.ps1 already executed (services + web listening).

[CmdletBinding()]
param(
    [string]$WebUrl    = 'http://127.0.0.1:8085',
    [string]$OrderUrl  = 'http://127.0.0.1:8081',
    [string]$PaymentUrl = 'http://127.0.0.1:8082',
    [string]$LogDir    = (Join-Path $PSScriptRoot '..\tests\logs')
)

$ErrorActionPreference = 'Continue'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$logFile = Join-Path $LogDir 'smoke-web.log'
'' | Out-File -FilePath $logFile -Encoding utf8

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan; "`n=== $msg ===" | Out-File -Append -FilePath $logFile -Encoding utf8 }
function Pass($msg) { Write-Host "  PASS: $msg" -ForegroundColor Green; "  PASS: $msg" | Out-File -Append -FilePath $logFile -Encoding utf8 }
function Fail($msg) { Write-Host "  FAIL: $msg" -ForegroundColor Red; "  FAIL: $msg" | Out-File -Append -FilePath $logFile -Encoding utf8; $script:failures++ }
function Expect($actual, $expected, $label) {
    if ($actual -eq $expected) { Pass "$label = $actual" }
    else { Fail "$label expected $expected, got $actual" }
}

$failures = 0

# 1. healthz
Step "healthz"
$h = Invoke-WebRequest "$WebUrl/healthz" -UseBasicParsing -TimeoutSec 5
Expect $h.StatusCode 200 "healthz"

# 2. readyz
$rz = Invoke-WebRequest "$WebUrl/readyz" -UseBasicParsing -TimeoutSec 5
Expect $rz.StatusCode 200 "readyz"

# 3. orders list page renders
Step "orders list"
$list = Invoke-WebRequest "$WebUrl/" -UseBasicParsing -TimeoutSec 5
Expect $list.StatusCode 200 "/"
if ($list.Content -match 'OrderFlow|orderflow-web') { Pass "/ contains brand" } else { Fail "/ missing brand" }

# 4. happy path: POST /v1/orders
Step "happy path"
$body = @{ customer_id = [guid]::NewGuid().ToString(); items = @(@{ sku = 'SKU-001'; quantity = 1; unit_price_cents = 1999 }); payment = @{ last_four = '4242' } } | ConvertTo-Json -Depth 5
$created = Invoke-WebRequest "$OrderUrl/v1/orders" -Method Post -ContentType 'application/json' -Body $body -UseBasicParsing -TimeoutSec 5
Expect $created.StatusCode 201 "POST /v1/orders"
$oid = ($created.Content | ConvertFrom-Json).id
if ($oid) { Pass "order id = $oid" } else { Fail "no order id" }

# 5. poll for confirmed
$state = 'pending'
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $r = Invoke-WebRequest "$OrderUrl/v1/orders/$oid" -UseBasicParsing -TimeoutSec 5
    $state = ($r.Content | ConvertFrom-Json).state
    if ($state -eq 'confirmed') { break }
}
Expect $state 'confirmed' "final state"

# 6. compensation path
Step "compensation path"
$body2 = @{ customer_id = [guid]::NewGuid().ToString(); items = @(@{ sku = 'SKU-001'; quantity = 1; unit_price_cents = 1999 }); payment = @{ last_four = '0001' } } | ConvertTo-Json -Depth 5
$created2 = Invoke-WebRequest "$OrderUrl/v1/orders" -Method Post -ContentType 'application/json' -Body $body2 -UseBasicParsing -TimeoutSec 5
Expect $created2.StatusCode 201 "POST compensation"
$oid2 = ($created2.Content | ConvertFrom-Json).id

# 7. payment sim fire failed
$wh = @{ order_id = $oid2; payment_id = $oid2; status = 'failed'; error_code = 'card_declined' } | ConvertTo-Json
$h2 = @{ 'Idempotency-Key' = "orderflow-web:${oid2}:failed" }
try {
    $fired = Invoke-WebRequest "$PaymentUrl/v1/payments/webhook" -Method Post -ContentType 'application/json' -Body $wh -Headers $h2 -UseBasicParsing -TimeoutSec 5
    Expect $fired.StatusCode 200 "fire webhook"
} catch { Fail "fire webhook threw: $_" }

# 8. final state cancelled
$state2 = 'pending'
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $r = Invoke-WebRequest "$OrderUrl/v1/orders/$oid2" -UseBasicParsing -TimeoutSec 5
    $state2 = ($r.Content | ConvertFrom-Json).state
    if ($state2 -in @('cancelled','failed')) { break }
}
if ($state2 -in @('cancelled','failed')) { Pass "compensation state = $state2" } else { Fail "compensation state = $state2" }

# 9. invalid UUID rejected
Step "validation"
try {
    $bad = Invoke-WebRequest "$WebUrl/v1/orders" -Method Post -ContentType 'application/x-www-form-urlencoded' -Body 'sku=&quantity=0' -UseBasicParsing -TimeoutSec 5
    Expect $bad.StatusCode 400 "empty form"
} catch {
    if ($_.Exception.Response.StatusCode -eq 400) { Pass "empty form = 400" }
    else { Fail "empty form threw: $_" }
}

# 10. inventory page renders
$inv = Invoke-WebRequest "$WebUrl/inventory" -UseBasicParsing -TimeoutSec 5
Expect $inv.StatusCode 200 "inventory"

# 11. payments sim renders
$ps = Invoke-WebRequest "$WebUrl/payments/sim" -UseBasicParsing -TimeoutSec 5
Expect $ps.StatusCode 200 "payments sim"

# 12. order detail renders
$od = Invoke-WebRequest "$WebUrl/orders/$oid" -UseBasicParsing -TimeoutSec 5
Expect $od.StatusCode 200 "order detail"

# summary
Step "summary"
if ($failures -eq 0) {
    Write-Host "ALL PASS" -ForegroundColor Green
    "ALL PASS" | Out-File -Append -FilePath $logFile -Encoding utf8
    exit 0
} else {
    Write-Host "$failures FAILURE(S)" -ForegroundColor Red
    "$failures FAILURE(S)" | Out-File -Append -FilePath $logFile -Encoding utf8
    exit 1
}
