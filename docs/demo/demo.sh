#!/usr/bin/env bash
set -euo pipefail

# --- config ---
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT}/deploy/docker-compose.yml"
LOG_DIR="${ROOT}/docs/demo/logs"
mkdir -p "${LOG_DIR}"
EXAMPLE_BODY="${ROOT}/examples/order.json"

# --- cleanup on exit ---
cleanup() {
  set +e
  echo "==> tearing down"
  if [[ -n "${ORDER_PID:-}" ]]; then kill "${ORDER_PID}" 2>/dev/null || true; fi
  if [[ -n "${PAYMENT_PID:-}" ]]; then kill "${PAYMENT_PID}" 2>/dev/null || true; fi
  if [[ -n "${INV_PID:-}" ]]; then kill "${INV_PID}" 2>/dev/null || true; fi
  if [[ -n "${SAGA_PID:-}" ]]; then kill "${SAGA_PID}" 2>/dev/null || true; fi
  docker compose -f "${COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- prerequisites ---
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not installed"; exit 1; }
command -v jq >/dev/null 2>&1     || { echo "ERROR: jq not installed"; exit 1; }
command -v curl >/dev/null 2>&1   || { echo "ERROR: curl not installed"; exit 1; }

# --- bring up infra ---
echo "==> starting infrastructure (postgres×3, redis, redpanda, otel-collector, prometheus, tempo, grafana)"
docker compose -f "${COMPOSE_FILE}" up -d
echo "==> waiting 30s for services to be ready"
sleep 30

# --- build binaries ---
echo "==> building service binaries"
cd "${ROOT}"
make build

# --- start services ---
echo "==> starting order, payment, inventory, saga in background"
DATABASE_URL="postgres://orderflow:orderflow@localhost:5432/order_order?sslmode=disable" \
  KAFKA_BROKER="localhost:9092" \
  HTTP_ADDR=":8081" \
  ./bin/order    >"${LOG_DIR}/order.log"    2>&1 & ORDER_PID=$!
DATABASE_URL="postgres://orderflow:orderflow@localhost:5433/payment_payment?sslmode=disable" \
  KAFKA_BROKER="localhost:9092" \
  HTTP_ADDR=":8082" \
  ./bin/payment  >"${LOG_DIR}/payment.log"  2>&1 & PAYMENT_PID=$!
DATABASE_URL="postgres://orderflow:orderflow@localhost:5434/inventory_inventory?sslmode=disable" \
  KAFKA_BROKER="localhost:9092" \
  REDIS_URL="redis://localhost:6379/0" \
  HTTP_ADDR=":8083" \
  ./bin/inventory>"${LOG_DIR}/inventory.log" 2>&1 & INV_PID=$!
DATABASE_URL="postgres://orderflow:orderflow@localhost:5432/order_order?sslmode=disable" \
  KAFKA_BROKER="localhost:9092" \
  HTTP_ADDR=":8084" \
  ./bin/saga     >"${LOG_DIR}/saga.log"     2>&1 & SAGA_PID=$!

echo "==> waiting 10s for services to boot"
sleep 10

# --- happy path ---
echo "==> POST /v1/orders with examples/order.json"
RESP=$(curl -sS -X POST http://localhost:8081/v1/orders \
  -H 'Content-Type: application/json' \
  --data-binary "@${EXAMPLE_BODY}")
echo "${RESP}" | jq .
ORDER_ID=$(echo "${RESP}" | jq -r .id)
echo "==> order id: ${ORDER_ID}"

# --- poll until confirmed ---
echo "==> polling order state..."
DEADLINE=$(($(date +%s) + 60))
while [[ $(date +%s) -lt ${DEADLINE} ]]; do
  STATE=$(curl -sS "http://localhost:8081/v1/orders/${ORDER_ID}" | jq -r .state)
  echo "    state=${STATE}"
  if [[ "${STATE}" == "confirmed" ]]; then
    echo "OK: order reached 'confirmed' state"
    exit 0
  fi
  sleep 1
done

echo "FAIL: order did not reach 'confirmed' within 60s"
echo "logs:"
ls -1 "${LOG_DIR}"
exit 1
