#!/usr/bin/env bash
# scripts/smoke-web.sh — automated smoke for the orderflow-web playground.
# POSIX sibling of smoke-web.ps1. Asserts happy path + compensation +
# 4xx + page renders against a running stack.
# Requires: scripts/run.sh already executed (services + web listening).
#
# Usage: bash scripts/smoke-web.sh
# Optional env overrides:
#   WEB_URL    (default http://127.0.0.1:8085)
#   ORDER_URL  (default http://127.0.0.1:8081)
#   PAYMENT_URL (default http://127.0.0.1:8082)
#
# Idempotency key prefix must match the web playground's backend
# client (services/web/internal/backend/payment.go):
#   orderflow-web:<payment_id>:<status>
# The payment service rejects webhooks with a missing or unknown
# Idempotency-Key with HTTP 400, so the smoke fails fast with a
# clear message instead of producing a phantom downstream event.

# Intentionally no `set -e`: we want to keep accumulating failures
# across steps and exit with a non-zero code only at the very end so
# `tests/logs/smoke-web.log` records the full picture.
set -u
set -o pipefail

WEB_URL="${WEB_URL:-http://127.0.0.1:8085}"
ORDER_URL="${ORDER_URL:-http://127.0.0.1:8081}"
PAYMENT_URL="${PAYMENT_URL:-http://127.0.0.1:8082}"
IDEMPOTENCY_PREFIX="orderflow-web:"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)/tests/logs}"
LOG_FILE="$LOG_DIR/smoke-web.log"

mkdir -p "$LOG_DIR"
: > "$LOG_FILE"

failures=0

# ANSI colors (suppressed when not on a tty).
if [ -t 1 ]; then
    C_CYAN=$'\033[1;36m'; C_GREEN=$'\033[1;32m'; C_RED=$'\033[1;31m'; C_RESET=$'\033[0m'
else
    C_CYAN=''; C_GREEN=''; C_RED=''; C_RESET=''
fi

# log_tee prints to stdout AND appends to LOG_FILE. Single
# implementation so we don't drift between the two sinks.
log_tee() {
    printf '%s\n' "$*"
    printf '%s\n' "$*" >> "$LOG_FILE"
}

step() {
    log_tee ""
    log_tee "=== $* ==="
}

pass() {
    log_tee "  PASS: $*"
}

fail() {
    log_tee "  FAIL: $*"
    failures=$((failures + 1))
}

expect() {
    local actual="$1" expected="$2" label="$3"
    if [ "$actual" = "$expected" ]; then
        pass "$label = $actual"
    else
        fail "$label expected $expected, got $actual"
    fi
}

# url_status <url> [curl-flags...] -> echoes "<status>" and stashes
# the response body in $RESP_BODY for the caller to inspect.
RESP_BODY=""
url_status() {
    local url="$1"; shift
    local tmp
    tmp="$(mktemp)"
    # -sS keeps curl quiet on success but surfaces connection errors.
    # -w writes the status to stdout after the body lands in $tmp.
    local code
    code=$(curl -sS -o "$tmp" -w '%{http_code}' "$@" "$url" || echo "000")
    RESP_BODY="$(cat "$tmp")"
    rm -f "$tmp"
    printf '%s' "$code"
}

# new_uuid -> echoes a v4 UUID using /proc/sys/kernel/random/uuid
# (Linux) or uuidgen (macOS/BSD). Both are pre-installed on every
# host that ships a recent coreutils.
new_uuid() {
    if [ -r /proc/sys/kernel/random/uuid ]; then
        cat /proc/sys/kernel/random/uuid
    else
        uuidgen
    fi
}

# 1. healthz
step "healthz"
code=$(url_status "$WEB_URL/healthz")
expect "$code" "200" "healthz"

# 2. readyz
code=$(url_status "$WEB_URL/readyz")
expect "$code" "200" "readyz"

# 3. orders list page renders
step "orders list"
code=$(url_status "$WEB_URL/")
expect "$code" "200" "/"
if printf '%s' "$RESP_BODY" | grep -Eq 'OrderFlow|orderflow-web'; then
    pass "/ contains brand"
else
    fail "/ missing brand"
fi

# 4. happy path: POST /v1/orders (last_four=4242 -> succeeded)
step "happy path"
HAPPY_BODY=$(jq -nc \
    --arg cid "$(new_uuid)" \
    '{customer_id: $cid, items: [{sku: "SKU-SMOKE", quantity: 1, unit_price_cents: 1999}], payment: {last_four: "4242"}}')
code=$(url_status "$ORDER_URL/v1/orders" \
    -X POST -H 'Content-Type: application/json' -d "$HAPPY_BODY")
expect "$code" "201" "POST /v1/orders"
oid=$(printf '%s' "$RESP_BODY" | jq -r '.id // empty')
if [ -n "$oid" ] && [ "$oid" != "null" ]; then
    pass "order id = $oid"
else
    fail "no order id"
fi

# 5. poll for confirmed
state="pending"
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    code=$(url_status "$ORDER_URL/v1/orders/$oid")
    if [ "$code" = "200" ]; then
        state=$(printf '%s' "$RESP_BODY" | jq -r '.state // "pending"')
    fi
    if [ "$state" = "confirmed" ]; then
        break
    fi
    sleep 1
done
expect "$state" "confirmed" "final state"

# 6. compensation path
step "compensation path"
FAIL_BODY=$(jq -nc \
    --arg cid "$(new_uuid)" \
    '{customer_id: $cid, items: [{sku: "SKU-SMOKE", quantity: 1, unit_price_cents: 1999}], payment: {last_four: "0001"}}')
code=$(url_status "$ORDER_URL/v1/orders" \
    -X POST -H 'Content-Type: application/json' -d "$FAIL_BODY")
expect "$code" "201" "POST compensation"
oid2=$(printf '%s' "$RESP_BODY" | jq -r '.id // empty')
if [ -n "$oid2" ] && [ "$oid2" != "null" ]; then
    pass "compensation order id = $oid2"
else
    fail "no compensation order id"
fi

# 7. fire failed payment webhook (payment sim path)
# Idempotency-Key MUST match the format services/web/internal/backend/
# payment.go uses or the payment service's idempotency middleware
# returns 400 "Idempotency-Key header required".
wh_body=$(jq -nc \
    --arg oid "$oid2" \
    '{order_id: $oid, payment_id: $oid, status: "failed", error_code: "card_declined"}')
wh_key="${IDEMPOTENCY_PREFIX}${oid2}:failed"
code=$(url_status "$PAYMENT_URL/v1/payments/webhook" \
    -X POST -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $wh_key" \
    -d "$wh_body")
expect "$code" "200" "fire webhook"

# 8. poll for cancelled/failed
state2="pending"
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    code=$(url_status "$ORDER_URL/v1/orders/$oid2")
    if [ "$code" = "200" ]; then
        s2=$(printf '%s' "$RESP_BODY" | jq -r '.state // "pending"')
        case "$s2" in
            cancelled|failed) state2="$s2"; break ;;
        esac
    fi
    sleep 1
done
case "$state2" in
    cancelled|failed) pass "compensation state = $state2" ;;
    *)                fail "compensation state = $state2" ;;
esac

# 9. invalid form (empty SKU + zero qty) -> web returns 400
step "validation"
code=$(url_status "$WEB_URL/v1/orders" \
    -X POST -d 'sku=&quantity=0')
expect "$code" "400" "empty form"

# 10. inventory page renders
code=$(url_status "$WEB_URL/inventory")
expect "$code" "200" "inventory"

# 11. payments sim renders
code=$(url_status "$WEB_URL/payments/sim")
expect "$code" "200" "payments sim"

# 12. order detail renders
code=$(url_status "$WEB_URL/orders/$oid")
expect "$code" "200" "order detail"

# summary
step "summary"
if [ "$failures" -eq 0 ]; then
    log_tee "ALL PASS"
    exit 0
else
    log_tee "$failures FAILURE(S)"
    exit 1
fi
