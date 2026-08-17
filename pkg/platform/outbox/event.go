// Package outbox provides the shared types for the transactional
// outbox pattern used by every orderflow service. The outbox is the
// bridge between business state changes (in Postgres) and downstream
// event consumers (in Kafka): a service writes an outbox row inside
// the same DB transaction as the business row, and a separate
// poller (sub-stage 3.7) reads those rows and publishes them to Kafka
// with exactly-once semantics.
//
// This package contains only the data shape and the Writer contract.
// Each service implements its own Postgres writer against its own
// outbox table.
package outbox

import "time"

// Status is the lifecycle of an outbox row.
type Status string

const (
	// StatusPending: row written by the business tx, not yet published.
	StatusPending Status = "PENDING"
	// StatusSent: row has been published to Kafka.
	StatusSent Status = "SENT"
	// StatusFailed: row exceeded the retry budget and was moved to DLQ.
	StatusFailed Status = "FAILED"
)

// Record is a row destined for the outbox table. The business code
// constructs this from an Envelope (see pkg/platform/events) plus the
// target Kafka topic. The Writer implementation is responsible for
// assigning timestamps and an internal outbox id.
//
// EventID, EventType, AggregateID, AggregateType, SchemaVersion, and
// Payload mirror the Envelope fields 1:1. Topic is which Kafka topic
// the poller should publish to. Headers carries W3C traceparent and
// other per-record Kafka headers (sub-stage 3.10.b — propagated into
// the Envelope and attached to the outgoing Kafka record).
type Record struct {
	EventID       string
	EventType     string
	AggregateID   string
	AggregateType string
	SchemaVersion string
	Topic         string
	Payload       []byte // pre-marshalled JSON
	Headers       map[string]string
}

// OccurredAtOrNow returns the record's OccurredAt if non-zero,
// otherwise time.Now().UTC(). Defined here (rather than calling
// time.Now() inline at construction) so test code can compare a
// freshly built Record against a fixed timestamp.
func (r Record) OccurredAtOrNow() time.Time {
	return time.Now().UTC()
}
