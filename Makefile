.PHONY: build test lint run clean tidy

build:
	go build -o bin/order ./cmd/order
	go build -o bin/payment ./cmd/payment
	go build -o bin/inventory ./cmd/inventory
	go build -o bin/saga ./cmd/saga

test:
	go test ./...

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
