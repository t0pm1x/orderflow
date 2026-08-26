# services/web/Makefile
#
# Frontend (SvelteKit) + Go binary build for the orderflow-web service.
#
# Standard workflow:
#   make build      # npm ci + npm run build + go build
#   make dev        # vite dev on :5173 with proxy to :8085 Go backend
#   make test       # go test ./...
#
# The Makefile at the repo root delegates here via:
#   make -C services/web build
# so a top-level `make web-build` rebuilds the full binary after
# `make web-frontend-build` is run.

SHELL := /bin/bash
.EXPORT_ALL_VARIABLES:
.EXCLUDE := node_modules .svelte-kit frontend/dist

.PHONY: help frontend-install frontend-build frontend-dev frontend-check build test run-clean

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# ---- frontend (SvelteKit SPA) -----------------------------------

frontend-install: ## npm ci in services/web/frontend
	cd frontend && npm ci

frontend-build: frontend-install ## build the SvelteKit SPA into frontend/dist/
	cd frontend && npm run build

frontend-dev: ## vite dev on :5173 (proxies /api + /events/stream to :8085)
	cd frontend && npm run dev

frontend-check: ## svelte-check (TypeScript)
	cd frontend && npm run check

# ---- go binary ----------------------------------------------------

build: frontend-build ## embed frontend/dist + go build ./cmd/web
	go build -o ../../bin/web$(EXE) ./cmd/web

test: ## go test ./internal/...
	go test -count=1 -timeout 60s ./internal/...

run-clean: ## remove built binary (frontend/dist is left in place; gitignored)
	rm -f ../../bin/web
