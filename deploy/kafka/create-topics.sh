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

topic_exists() {
    rpk topic list -X brokers="$BROKER" 2>/dev/null | grep -q "^$1 "
}

for t in order-events payment-events inventory-events; do
    if topic_exists "$t"; then
        echo "topic $t already exists, skipping"
    else
        rpk topic create "$t" --brokers "$BROKER" --partitions 3 --replicas 1
    fi
done

# Per-topic DLQ topics (audit CONSUMER-2). The consumer-side DLQ
# (pkg/consumer/kafka_dlq.go) writes to <topic>.DLQ; without these
# pre-created, the poller blocks forever on the DLQ send when
# auto-create is disabled and races the broker's auto-create
# latency when it is enabled (mirroring the OBX-004 root cause).
for t in order-events.DLQ payment-events.DLQ inventory-events.DLQ; do
    if topic_exists "$t"; then
        echo "topic $t already exists, skipping"
    else
        rpk topic create "$t" --brokers "$BROKER" --partitions 1 --replicas 1
    fi
done

if topic_exists orderflow-dlq; then
    echo "topic orderflow-dlq already exists, skipping"
else
    rpk topic create orderflow-dlq --brokers "$BROKER" --partitions 1 --replicas 1
fi

echo "Topics:"
rpk topic list -X brokers="$BROKER"
