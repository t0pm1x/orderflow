#!/bin/bash
set -e

BROKER="${KAFKA_BROKER:-redpanda:9092}"

echo "Waiting for Redpanda at $BROKER..."
for i in {1..30}; do
    if rpk cluster health -X brokers="$BROKER" 2>/dev/null; then
        break
    fi
    sleep 2
done

echo "Creating topics..."

rpk topic create order-events \
    --brokers "$BROKER" \
    --partitions 3 \
    --replicas 1 \
    --if-not-exists

rpk topic create payment-events \
    --brokers "$BROKER" \
    --partitions 3 \
    --replicas 1 \
    --if-not-exists

rpk topic create inventory-events \
    --brokers "$BROKER" \
    --partitions 3 \
    --replicas 1 \
    --if-not-exists

rpk topic create orderflow-dlq \
    --brokers "$BROKER" \
    --partitions 1 \
    --replicas 1 \
    --if-not-exists

echo "Topics created:"
rpk topic list -X brokers="$BROKER"
