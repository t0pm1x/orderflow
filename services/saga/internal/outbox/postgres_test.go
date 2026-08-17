// Package outbox: PGWriter tests use a fake DBTX so we can assert
// the SQL shape without spinning up Postgres. The fake matches the
// shape used in services/{order,payment,inventory}/internal/outbox.
package outbox

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

type fakeDBTX struct {
	calls []fakeCall
	err   error
}

type fakeCall struct {
	sql  string
	args []any
}

func (f *fakeDBTX) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	return pgconn.CommandTag{}, f.err
}

func (f *fakeDBTX) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	return nil
}

func argsEqual(a, b any) bool {
	if ab, ok := a.([]byte); ok {
		if bb, ok := b.([]byte); ok {
			return bytes.Equal(ab, bb)
		}
	}
	return a == b
}

// TestPGWriter_Append_EmitsInsert verifies the writer emits a single
// INSERT against the saga_outbox table, threading event_id,
// aggregate_id, aggregate_type, event_type, payload, and headers
// through as positional args in that exact order. schema_version is
// fixed at 1 in v0.5.0 (see 0002_saga_outbox.sql); status starts at
// PENDING.
func TestPGWriter_Append_EmitsInsert(t *testing.T) {
	db := &fakeDBTX{}
	w := NewPGWriter()

	err := w.Append(context.Background(), db, outbox.Record{
		EventID:       "evt-1",
		EventType:     "StockReserveRequested",
		AggregateID:   "ord-1",
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       []byte(`{"sku":"A","qty":2}`),
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(db.calls) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(db.calls))
	}
	c := db.calls[0]
	want := []any{
		"evt-1",
		"StockReserveRequested",
		"ord-1",
		"Order",
		[]byte(`{"sku":"A","qty":2}`),
		[]byte(`{}`),
	}
	if len(c.args) != len(want) {
		t.Fatalf("arg count: got %d, want %d", len(c.args), len(want))
	}
	for i := range want {
		if !argsEqual(c.args[i], want[i]) {
			t.Errorf("arg[%d]: got %v, want %v", i, c.args[i], want[i])
		}
	}
}

// TestPGWriter_Append_PropagatesExecError: an Exec failure must
// bubble up so the caller's tx rolls back.
func TestPGWriter_Append_PropagatesExecError(t *testing.T) {
	db := &fakeDBTX{err: pgErr("deadlock")}
	w := NewPGWriter()
	err := w.Append(context.Background(), db, outbox.Record{
		EventID:   "e",
		EventType: "StockReserveRequested",
		Topic:     "order-events",
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

type pgErr string

func (e pgErr) Error() string { return string(e) }

// TestPGWriter_Append_PassesHeaders verifies that non-empty Headers
// are JSON-marshalled and threaded into the 6th positional arg.
// Headers is what the poller reads to attach tracecontext to the
// outgoing Kafka record (sub-stage 3.10.b).
func TestPGWriter_Append_PassesHeaders(t *testing.T) {
	db := &fakeDBTX{}
	w := NewPGWriter()

	err := w.Append(context.Background(), db, outbox.Record{
		EventID:       "evt-1",
		EventType:     "PaymentRequested",
		AggregateID:   "ord-1",
		AggregateType: "Order",
		Topic:         "order-events",
		Payload:       []byte(`{}`),
		Headers:       map[string]string{"traceparent": "00-aaa-bbb-01"},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, ok := db.calls[0].args[5].([]byte)
	if !ok {
		t.Fatalf("headers arg not []byte: %T", db.calls[0].args[5])
	}
	if !bytes.Contains(got, []byte("traceparent")) {
		t.Errorf("headers did not contain traceparent: %s", got)
	}
}
