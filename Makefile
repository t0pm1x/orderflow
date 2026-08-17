.PHONY: build test lint run clean tidy

# Version baked into each binary's main.Version at build time.
# git describe falls back to a dev tag if not on a tagged commit.
VERSION ?= $(shell git describe --tags --always --dirty 2>nul || echo 0.0.0-dev)
LDFLAGS := -s -w -X main.Version=$(VERSION)

build:
	go build -ldflags="$(LDFLAGS)" -o bin/order ./cmd/order
	go build -ldflags="$(LDFLAGS)" -o bin/payment ./cmd/payment
	go build -ldflags="$(LDFLAGS)" -o bin/inventory ./cmd/inventory
	go build -ldflags="$(LDFLAGS)" -o bin/saga ./cmd/saga

# Workspace modules (mirrors go.work `use` block). Each is `cd`'d
# into before `go test ./...` because the workspace root has no
# go.mod — `./...` at the root fails with "directory prefix . does
# not contain modules listed in go.work".
WORKSPACE_MODULES = pkg/platform pkg/outbox pkg/consumer pkg/platform/instrumentation/kafkaprop \
                    services/order services/payment services/inventory services/saga \
                    cmd/order cmd/payment cmd/inventory cmd/saga \
                    tests

test:
	@for m in $(WORKSPACE_MODULES); do \
		echo "==> testing $$m"; \
		(cd "$$m" && go test ./...) || exit 1; \
	done

lint:
	golangci-lint run

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

clean:
	rm -rf bin/

tidy:
	go mod tidy

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
	go test ./tests/e2e/... -run TestE2E_HappyPath -timeout 5m -v

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
