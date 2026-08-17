# Recording the orderflow Demo

Step-by-step runbook for capturing the demo as a `.cast` file (asciinema v2 format).

## Prerequisites

### Install asciinema

**Linux / macOS**:
    pip install asciinema

**Windows** (PowerShell):
    winget install --id asciinema.asciinema
# OR if winget version unavailable:
    pip install asciinema

Verify:
    asciinema --version

## Capture

From the repo root:

    asciinema rec --command "bash docs/demo/demo.sh" \
        --title "orderflow v1.0 — happy path demo" \
        --idle-time-limit 2 \
        docs/demo/orderflow.cast

The recording will capture the demo end-to-end (~60 seconds of activity).

## Playback locally

    asciinema play docs/demo/orderflow.cast

## Upload to asciinema.org (optional)

    asciinema upload docs/demo/orderflow.cast

This prints a public URL like `https://asciinema.org/a/<id>`. Embed in README:

```markdown
[![asciicast](https://asciinema.org/a/<id>.svg)](https://asciinema.org/a/<id>)
```

## Convert to SVG (offline embed)

    docker run --rm -v $PWD/docs/demo:/data asciinema/asciicast2gif docs/demo/orderflow.cast docs/demo/orderflow.gif

## Cleanup

The `.cast` file is plain JSON — commit it to git. The `.gif` (if generated) is binary and ~MBs — gitignore or store elsewhere.

## Notes

- The demo runs against the local docker-compose stack + 4 service binaries.
- Real recording requires: docker, 8GB RAM, the 4 binaries built (`make build`).
- The script traps `EXIT` and tears down docker-compose + child processes, so the recording captures cleanup too.