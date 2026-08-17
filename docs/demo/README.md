# orderflow Demo

End-to-end happy-path demo against the local docker-compose stack.

## Prerequisites
- Docker (running)
- `make`, `curl`, `jq` (all pre-installed on most Linux/macOS dev hosts)
- 8 GB RAM available

## Run

    ./docs/demo/demo.sh

The script will:
1. Bring up the infrastructure stack (postgres×3, redis, redpanda, otel-collector, prometheus, tempo, grafana).
2. Build the 4 service binaries.
3. Start order/payment/inventory/saga in the background.
4. POST a sample order from `examples/order.json`.
5. Poll until the order reaches `confirmed` (or fail after 60s).

## What you'll see
- Container startup logs (docker-compose)
- Build output
- Service boot logs in `docs/demo/logs/`
- A `POST /v1/orders` JSON response
- Polling output: `state=initiated`, `state=reserved`, `state=confirmed`
- Exit code 0 on success

## Cleanup
The script traps `EXIT` and tears down all services + docker-compose stack. You can also run `docker compose -f deploy/docker-compose.yml down -v` manually.

## Troubleshooting
- **"order did not reach confirmed"**: check `docs/demo/logs/*.log` — most likely the saga didn't start, or one of the services failed to connect to its database.
- **Port conflicts**: the script assumes ports 5432/5433/5434/6379/9092 are free. Check with `docker ps` and `netstat`.
- **Slow docker pull**: first run pulls 7+ images and can take 5-10 minutes on a cold machine.
