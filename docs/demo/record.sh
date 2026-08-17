#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CAST="${ROOT}/docs/demo/orderflow.cast"

if ! command -v asciinema >/dev/null 2>&1; then
  echo "ERROR: asciinema not installed."
  echo "  pip install asciinema        # Python (cross-platform)"
  echo "  winget install asciinema.asciinema  # Windows"
  echo "  See docs/demo/RECORDING.md for details."
  exit 1
fi

cd "${ROOT}"
asciinema rec \
  --command "bash ${ROOT}/docs/demo/demo.sh" \
  --title "orderflow v1.0 — happy path demo" \
  --idle-time-limit 2 \
  "${CAST}"