.PHONY: build build-web web-frontend-build web-frontend-install web-build test lint run clean tidy run-web help

# Version baked into each binary at build time. The Version
# variable lives in services/<svc>/cmd/<svc> (package <svc>), not
# cmd/<svc>/main.go (package main), so the -X flag must target the
# real package path. The previous -X main.Version=... was a silent
# no-op because cmd/<svc>/main.go has no Version symbol (it just
# delegates to the service package).
#
# On Windows the harness appends ".exe" itself, but the Makefile
# produces plain names (bin/order). Add the .exe suffix explicitly
# so the harness and the Makefile agree on the artifact path.
VERSION ?= $(shell git describe --tags --always --dirty 2>nul || echo 0.0.0-dev)
LDFLAGS := -s -w
EXE :=
ifeq ($(OS),Windows_NT)
	EXE := .exe
endif

# go.work pins go=1.25.13; on hosts with older Go (e.g. 1.25.4),
# the default GOTOOLCHAIN=local refuses to build with "go.work
# requires go >= 1.25.13". GOTOOLCHAIN=auto makes Go download
# the pin's toolchain on demand so `make build` works regardless
# of the host Go version. This matches what the build pipeline
# (Makefile + go.work + go versions in go.mod) actually expects.
export GOTOOLCHAIN ?= auto

build: web-frontend-build
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/order/cmd/order.Version=$(VERSION)" -o bin/order$(EXE) ./cmd/order
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/payment/cmd/payment.Version=$(VERSION)" -o bin/payment$(EXE) ./cmd/payment
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/inventory/cmd/inventory.Version=$(VERSION)" -o bin/inventory$(EXE) ./cmd/inventory
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/saga/cmd/saga.Version=$(VERSION)" -o bin/saga$(EXE) ./cmd/saga
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/web/cmd/web.Version=$(VERSION)" -o bin/web$(EXE) ./cmd/web

# `make build` runs the SvelteKit build first so a fresh
# checkout produces a binary that serves the real SPA, not the
# "SPA not built yet" placeholder. Set `SPA_BUILD_SKIP=1` to
# skip the npm step (e.g. in CI containers without Node.js).
#
# `make web-build` rebuilds only the web binary (still pulls
# in the SvelteKit build via the dependency).
web-build: web-frontend-build
	go build -ldflags="$(LDFLAGS) -X github.com/t0pm1x/orderflow/services/web/cmd/web.Version=$(VERSION)" -o bin/web$(EXE) ./cmd/web

# `make build-web` skips the SvelteKit build — for fast iteration
# on the Go BFF only when the SPA assets don't need to change.
build-web:
	go build -ldflags="$(LDFLAGS)" -o bin/web ./cmd/web

# `make web-frontend-install` runs `npm ci` in services/web/frontend/.
# One-time per fresh checkout / dependency change.
web-frontend-install:
	+$(MAKE) -C services/web frontend-install

# `make web-frontend-build` runs `npm run build` (Vite + SvelteKit)
# producing frontend/dist/{index.html,_app/,favicon.svg}. The
# Go binary embeds those files at compile time.
web-frontend-build:
	+$(MAKE) -C services/web frontend-build

# Workspace modules (mirrors go.work `use` block). Each is `cd`'d
# into before `go test ./...` because the workspace root has no
# go.mod — `./...` at the root fails with "directory prefix . does
# not contain modules listed in go.work".
WORKSPACE_MODULES = pkg/platform pkg/outbox pkg/consumer pkg/platform/instrumentation/kafkaprop \
                    services/order services/payment services/inventory services/saga services/web \
                    cmd/order cmd/payment cmd/inventory cmd/saga cmd/web \
                    tests

test:
	@for m in $(WORKSPACE_MODULES); do \
		echo "==> testing $$m"; \
		(cd "$$m" && go test -short ./...) || exit 1; \
	done

lint:
	@for m in $(WORKSPACE_MODULES); do \
		echo "==> linting $$m"; \
		(cd "$$m" && golangci-lint run) || exit 1; \
	done

run:
	@echo "Pick a service: make run-order / run-payment / run-inventory / run-saga"

run-order:
	go run ./cmd/order

run-payment:
	go run ./cmd/payment

run-inventory:
	go run ./cmd/inventory

run-saga:
	go run ./cmd/saga

run-web:
	go run ./cmd/web

clean:
	rm -rf bin/
ifeq ($(OS),Windows_NT)
	@powershell -NoProfile -Command "Get-ChildItem -Path cmd,services -Recurse -Include *.exe -ErrorAction SilentlyContinue | Where-Object { $$_.DirectoryName -notmatch '\\(internal\\|.git)' } | Remove-Item -Force"
else
	find cmd -maxdepth 2 -name '*.exe' -delete
	find services -path '*/bin' -prune -o -name '*.exe' -print -delete
endif

tidy:
	@for m in $(WORKSPACE_MODULES); do \
		echo "==> tidying $$m"; \
		(cd "$$m" && go mod tidy) || exit 1; \
	done

# --- kind cluster (local k8s dev) ---
KIND         := kind
KIND_CLUSTER := orderflow
KIND_IMAGE   := kindest/node:v1.30.0

.PHONY: kind-up kind-down kind-load kind-status

kind-up:
	@echo ">> ensuring kind is installed"
	@command -v $(KIND) >/dev/null 2>&1 || { echo "ERROR: 'kind' not found in PATH. Install: winget install Kubernetes.kind (Windows) or brew install kind (macOS)"; exit 1; }
	$(KIND) create cluster --name $(KIND_CLUSTER) --config deploy/kind/kind.yaml --image $(KIND_IMAGE)

kind-down:
	$(KIND) delete cluster --name $(KIND_CLUSTER)

kind-load:
	$(KIND) load docker-image --name $(KIND_CLUSTER) ghcr.io/t0pm1x/orderflow-order:dev
	$(KIND) load docker-image --name $(KIND_CLUSTER) ghcr.io/t0pm1x/orderflow-payment:dev
	$(KIND) load docker-image --name $(KIND_CLUSTER) ghcr.io/t0pm1x/orderflow-inventory:dev
	$(KIND) load docker-image --name $(KIND_CLUSTER) ghcr.io/t0pm1x/orderflow-saga:dev

kind-status:
	$(KIND) get clusters

# --- load test (k6-driven, 100 RPS for 60s) ---
.PHONY: load

load:
	go test ./tests/load/... -v -timeout 10m

# --- E2E suite aggregate ---
.PHONY: e2e e2e-happy e2e-compensation e2e-chaos

e2e: e2e-happy e2e-compensation e2e-chaos

e2e-happy:
	go test ./tests/e2e/... -run TestE2E_OrderReachesConfirmed -timeout 5m -v

e2e-compensation:
	go test ./tests/e2e/... -run TestE2E_Compensation -timeout 5m -v

e2e-chaos:
	go test ./tests/chaos/... -timeout 10m -v

# --- k8s smoke test (kind cluster) ---
# KIND_SKIP=0 explicitly re-enables the test even if the caller exported
# KIND_SKIP=1 in their shell. CI sets KIND_SKIP=0 to opt-in.
KIND_SMOKE := KIND_SKIP=0 go test ./tests/k8s/... -v -timeout 15m

.PHONY: smoke-k8s

smoke-k8s:
	$(KIND_SMOKE)

# --- record demo as asciinema cast ---
.PHONY: record

record:
	bash docs/demo/record.sh

# --- pre-push verification (runs what CI runs, locally) ---
.PHONY: verify

verify: tidy build test lint
	@echo "==> verify complete"
