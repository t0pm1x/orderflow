#!/usr/bin/env bash
# scripts/run.sh — easy run for orderflow (macOS / Linux / WSL).
#
# Brings up the full local stack: docker compose infra (postgres x3,
# redis, redpanda, kafka-init, otel-collector, prometheus, tempo,
# grafana), builds the 5 service binaries via `make build`, starts
# them with the correct env vars, and waits for every service to
# pass /healthz.
#
# Tear down with scripts/stop.sh (kills the binaries + stops docker
# compose, volumes preserved). Drop volumes with
# `docker compose -f deploy/docker-compose.yml down -v` to reset
# Postgres state.
#
# Usage:
#   bash scripts/run.sh
#   bash scripts/run.sh --no-build
#
# Requires: docker daemon running, docker compose v2, Go 1.25+, GNU
# Make, ~4 GB RAM. Logs land in tests/logs/<svc>.log (one per service).
#
# Windows users: there is no bash script here — use the sibling
# scripts/run.ps1 instead:
#     powershell -ExecutionPolicy Bypass -File scripts\run.ps1
# (or `pwsh ...` if you have PowerShell 7+ installed).

set -uo pipefail

# ---- parse args ----
NO_BUILD=0
for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=1 ;;
        -h|--help)
            sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

# ---- paths ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$ROOT/bin"
LOG="$ROOT/tests/logs"
DEPLOY="$ROOT/deploy"
mkdir -p "$LOG"

# ---- helpers ----
step() { printf "\n\033[1;36m=== %s ===\033[0m\n" "$*"; }
ok()   { printf "  \033[1;32mok:\033[0m %s\n" "$*"; }
warn() { printf "  \033[1;33m!!\033[0m %s\n" "$*"; }
die()  { printf "  \033[1;31mX \033[0m%s\n" "$*" >&2; exit 1; }

# ---- 0. docker daemon reachable ----
step "Verifying Docker daemon"
if ! docker info >/dev/null 2>&1; then
    case "$(uname -s)" in
        Darwin) die "Docker daemon not reachable. Start Docker Desktop: open -a Docker, then re-run." ;;
        Linux)  die "Docker daemon not reachable. Start it: sudo systemctl start docker (or 'open -a Docker' if on Docker Desktop), then re-run." ;;
        *)      die "Docker daemon not reachable. Start Docker and re-run." ;;
    esac
fi
ok "docker daemon reachable"

# ---- 1. kill stale local binaries ----
step "Killing stale service binaries (best-effort)"
killed=0
for proc in order payment inventory saga web; do
    pids=$(pgrep -f "$BIN/$proc" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        for pid in $pids; do
            kill -9 "$pid" 2>/dev/null || true
            echo "  killed $proc pid=$pid"
            killed=1
        done
    fi
done
[ "$killed" -eq 0 ] && echo "  (none running)"
sleep 1

# ---- 2. bring up infra ----
step "Bringing up docker compose infra (postgres x3, redis, redpanda, otel, prometheus, tempo, grafana)"
if ! docker compose -f "$DEPLOY/docker-compose.yml" up -d \
        postgres-order postgres-payment postgres-inventory \
        redis redpanda kafka-init \
        otel-collector prometheus tempo grafana; then
    die "docker compose up failed"
fi
ok "compose up"

# ---- 2a. wait for postgres + redpanda healthchecks ----
step "Waiting for postgres-order/payment/inventory + redpanda healthchecks"
deadline=$(( $(date +%s) + 60 ))
critical=(postgres-order postgres-payment postgres-inventory redpanda)
while [ "$(date +%s)" -lt "$deadline" ]; do
    ok_all=1
    for svc in "${critical[@]}"; do
        state=$(docker inspect --format '{{.State.Health.Status}}' "deploy-$svc-1" 2>/dev/null || echo unknown)
        if [ "$state" != "healthy" ]; then ok_all=0; break; fi
    done
    if [ "$ok_all" = 1 ]; then
        ok "infra healthy"
        break
    fi
    sleep 2
done
if [ "$ok_all" != 1 ]; then
    for svc in "${critical[@]}"; do
        state=$(docker inspect --format '{{.State.Health.Status}}' "deploy-$svc-1" 2>/dev/null || echo unknown)
        warn "$svc health=$state"
    done
    die "infra failed to become healthy in 60s"
fi

# ---- 3. saga migrations on order DB ----
step "Saga migrations on order DB (saga shares the order PG)"
has_sagas=$(docker exec deploy-postgres-order-1 psql -U orderflow -d order_order -tAc "SELECT 1 FROM pg_tables WHERE tablename='order_sagas'" 2>/dev/null || echo "")
if [ "$has_sagas" != "1" ]; then
    die "order_sagas missing in order_order - postgres-order volume predates the v1.1.5 fix. Reset: docker compose -f deploy/docker-compose.yml down -v"
fi
has_lf=$(docker exec deploy-postgres-order-1 psql -U orderflow -d order_order -tAc "SELECT 1 FROM information_schema.columns WHERE table_name='order_sagas' AND column_name='last_four'" 2>/dev/null || echo "")
if [ "$has_lf" != "1" ]; then
    docker exec -i deploy-postgres-order-1 psql -U orderflow -d order_order \
        < "$ROOT/services/saga/migrations/0003_saga_payment_last_four.sql" >/dev/null
    ok "applied 0003_saga_payment_last_four.sql"
else
    ok "order_sagas.last_four already present"
fi

# ---- 4. build binaries ----
if [ "$NO_BUILD" = 1 ]; then
    step "Skipping build (--no-build)"
    ok "using existing binaries in bin/"
else
    step "Building binaries (make build)"
    if ! make -C "$ROOT" build; then
        die "make build failed"
    fi
    ok "binaries up-to-date in bin/"
fi

# ---- 5. start services ----
step "Starting order/payment/inventory/saga/web"
export OTEL_EXPORTER=stdout

start_svc() {
    local name="$1"
    local exe="$BIN/$name"
    local out="$LOG/$name.log"
    local err="$LOG/$name.err"
    if [ ! -x "$exe" ]; then
        die "binary not found or not executable: $exe (build failed?)"
    fi
    nohup "$exe" >"$out" 2>"$err" &
    local pid=$!
    echo "$pid" > "$LOG/$name.pid"
    ok "$name pid=$pid"
}

DATABASE_URL='postgres://orderflow:orderflow@127.0.0.1:5432/order_order?sslmode=disable' \
KAFKA_BROKER='127.0.0.1:9092' \
HTTP_ADDR='127.0.0.1:8081' \
    start_svc order

DATABASE_URL='postgres://orderflow:orderflow@127.0.0.1:5433/payment_payment?sslmode=disable' \
KAFKA_BROKER='127.0.0.1:9092' \
HTTP_ADDR='127.0.0.1:8082' \
    start_svc payment

DATABASE_URL='postgres://orderflow:orderflow@127.0.0.1:5434/inventory_inventory?sslmode=disable' \
KAFKA_BROKER='127.0.0.1:9092' \
REDIS_URL='redis://127.0.0.1:6379' \
HTTP_ADDR='127.0.0.1:8083' \
    start_svc inventory

DATABASE_URL='postgres://orderflow:orderflow@127.0.0.1:5432/order_order?sslmode=disable' \
KAFKA_BROKER='127.0.0.1:9092' \
HTTP_ADDR='127.0.0.1:8084' \
    start_svc saga

ORDER_URL='http://127.0.0.1:8081' \
PAYMENT_URL='http://127.0.0.1:8082' \
INVENTORY_URL='http://127.0.0.1:8083' \
KAFKA_BROKERS='127.0.0.1:9092' \
HTTP_ADDR='127.0.0.1:8085' \
    start_svc web

# ---- 6. healthcheck ----
step "Healthchecks"
ports=( order:8081 payment:8082 inventory:8083 saga:8084 web:8085 )
deadline=$(( $(date +%s) + 30 ))
all_ok=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    all_ok=1
    for entry in "${ports[@]}"; do
        svc="${entry%:*}"
        port="${entry#*:}"
        code=$(curl -fs -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz" 2>/dev/null || echo 000)
        if [ "$code" != "200" ]; then all_ok=0; break; fi
    done
    [ "$all_ok" = 1 ] && break
    sleep 1
done
for entry in "${ports[@]}"; do
    svc="${entry%:*}"
    port="${entry#*:}"
    code=$(curl -fs -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz" 2>/dev/null || echo 000)
    if [ "$code" = "200" ]; then
        ok "$svc :$port -> $code"
    else
        warn "$svc :$port -> DOWN ($code)"
    fi
done

# ---- 7. summary ----
step "READY"
cat <<'EOF'

  Web UI           :  http://127.0.0.1:8085
  Order Service    :  http://127.0.0.1:8081   (POST/GET/LIST/DELETE /v1/orders)
  Payment Service  :  http://127.0.0.1:8082
  Inventory Service:  http://127.0.0.1:8083
  Saga Service     :  http://127.0.0.1:8084

  Grafana          :  http://127.0.0.1:3000   (admin / admin)
  Prometheus       :  http://127.0.0.1:9091
  Tempo            :  http://127.0.0.1:3200

  Logs (tail-able):
EOF
for name in order payment inventory saga web; do
    echo "    $LOG/$name.log"
done
cat <<'EOF'

  Smoke test (happy path):
    curl -X POST http://127.0.0.1:8081/v1/orders -H 'Content-Type: application/json' -d @examples/order.json
    curl http://127.0.0.1:8081/v1/orders/<id-from-201>

  Smoke test (compensation: last_four=0001 -> declined -> state=cancelled):
    curl -X POST http://127.0.0.1:8081/v1/orders -H 'Content-Type: application/json' -d '{"customer_id":"8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f","items":[{"sku":"SKU-001","quantity":1,"unit_price_cents":1999}],"payment":{"last_four":"0001"}}'
    curl http://127.0.0.1:8081/v1/orders/<id-from-201>

  Tear down:
    bash scripts/stop.sh
    docker compose -f deploy/docker-compose.yml down
EOF