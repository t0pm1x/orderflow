#!/usr/bin/env bash
# scripts/stop.sh — tear down the orderflow easy run (macOS / Linux / WSL).
#
# Kills the 5 service binaries (if running) and stops the docker
# compose infra. Volumes are preserved; pass --volumes to drop them
# too (resets Postgres state — order_sagas, payments, reservations).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="$ROOT/tests/logs"
DEPLOY="$ROOT/deploy"

WITH_VOLUMES=""
for arg in "$@"; do
    case "$arg" in
        --volumes|-v) WITH_VOLUMES="-v" ;;
        -h|--help)
            sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

step() { printf "\n\033[1;36m=== %s ===\033[0m\n" "$*"; }
ok()   { printf "  \033[1;32mok:\033[0m %s\n" "$*"; }

step "Stopping service binaries"
killed=0
for name in order payment inventory saga web; do
    pidfile="$LOG/$name.pid"
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile" 2>/dev/null || true)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 0.3
            kill -9 "$pid" 2>/dev/null || true
            echo "  killed $name pid=$pid"
            killed=1
        fi
        rm -f "$pidfile"
    fi
    # Fallback: pgrep for processes started without pidfile
    pids=$(pgrep -f "$ROOT/bin/$name" 2>/dev/null || true)
    for pid in $pids; do
        kill -9 "$pid" 2>/dev/null || true
        echo "  killed $name pid=$pid (pgrep)"
        killed=1
    done
done
[ "$killed" -eq 0 ] && echo "  (none running)"
ok "service binaries stopped"

step "Stopping docker compose infra"
docker compose -f "$DEPLOY/docker-compose.yml" down $WITH_VOLUMES >/dev/null 2>&1 || true
ok "compose down ${WITH_VOLUMES:-(volumes preserved)}"