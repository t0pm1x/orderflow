// Package harness — Kafka topic pre-creation helper.
//
// The orderflow services all publish/consume against three fixed
// topics (order-events, payment-events, inventory-events). The
// testcontainer Kafka image (confluentinc/confluent-local:7.5.0)
// enables auto.create.topics.enable=true, but the first publish
// against a topic that doesn't exist yet races the auto-create:
// the producer receives `UNKNOWN_TOPIC_OR_PARTITION` for ~hundreds
// of milliseconds before the topic becomes available. The order
// service's outbox poller has a hard MaxAttempts budget of 5
// attempts × 100ms interval = 500ms; if auto-create doesn't finish
// within that window the row is DLQ'd and the chain stalls because
// the saga never receives the OrderCreated event.
//
// preCreateKafkaTopics blocks until every topic exists on the
// broker, so by the time the spawned service binaries boot the
// poller's first publish lands on a topic that's already there.
//
// The implementation uses franz-go's raw CreateTopics request
// (kmsg) via kgo.Client.Request so we don't pull in a separate
// kadm dependency. ALREADY_EXISTS responses are tolerated.
package harness

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// preCreateKafkaTopics creates topics on the testcontainer broker
// before any service binary boots. t.Fatal on any unrecoverable
// error so the caller does not need to check. Idempotent —
// ALREADY_EXISTS is treated as success.
func preCreateKafkaTopics(ctx context.Context, t *testing.T, brokers []string, topics []string) {
	t.Helper()
	if len(brokers) == 0 || len(topics) == 0 {
		return
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("harness: kafka client for topic pre-create: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range topics {
		tc := kmsg.NewCreateTopicsRequestTopic()
		tc.Topic = topic
		tc.NumPartitions = 1
		tc.ReplicationFactor = 1
		req.Topics = append(req.Topics, tc)
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("harness: CreateTopics request: %v", err)
	}
	for _, r := range resp.Topics {
		// ErrorCode 36 is TOPIC_ALREADY_EXISTS; tolerate it so
		// re-runs of the test in the same broker don't fail.
		if r.ErrorCode != 0 && r.ErrorCode != 36 {
			msg := "<no message>"
			if r.ErrorMessage != nil {
				msg = *r.ErrorMessage
			}
			t.Fatalf("harness: create topic %s: errorCode=%d errorMessage=%s",
				r.Topic, r.ErrorCode, msg)
		}
	}
}
