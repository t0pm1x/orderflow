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
