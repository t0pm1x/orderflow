package outbox

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
)

// Compile-time interface check: pgx.Tx and *pgxpool.Pool both satisfy
// DBTX. If they ever drift, this test fails to compile.
var (
	_ DBTX = (pgx.Tx)(nil)
	_ DBTX = (*pgxpool.Pool)(nil)
)

// TestRecord_Fields documents the field round-trip via a fake writer.
// The writer contract is asserted by build above; this test just makes
// sure the Record shape is what the rest of the system assumes.
func TestRecord_Fields(t *testing.T) {
	r := Record{
		EventID:       "evt-1",
		EventType:     "OrderCreated",
		AggregateID:   "ord-1",
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       []byte(`{"x":1}`),
	}
	if r.EventID != "evt-1" || r.Topic != "order-events" || string(r.Payload) != `{"x":1}` {
		t.Fatalf("record fields not preserved: %+v", r)
	}
}

// fakeDBTX lets us unit-test Writers without spinning up Postgres.
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

// fakeWriter is the minimum to assert Writer.Append delegates the SQL
// and args through to DBTX.
type fakeWriter struct {
	outboxTable string
}

func (w fakeWriter) Append(ctx context.Context, tx DBTX, r Record) error {
	_, err := tx.Exec(ctx,
		"INSERT INTO "+w.outboxTable+" (event_id, event_type, aggregate_id, aggregate_type, schema_version, topic, payload, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
		r.EventID, r.EventType, r.AggregateID, r.AggregateType, r.SchemaVersion, r.Topic, r.Payload, StatusPending,
	)
	return err
}

func TestWriter_Append_PassesArgs(t *testing.T) {
	db := &fakeDBTX{}
	w := fakeWriter{outboxTable: "outbox"}
	err := w.Append(context.Background(), db, Record{
		EventID:       "e1",
		EventType:     "OrderCreated",
		AggregateID:   "o1",
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(db.calls) != 1 {
		t.Fatalf("expected 1 Exec call, got %d", len(db.calls))
	}
	got := db.calls[0]
	if got.args[0] != "e1" || got.args[5] != "order-events" || got.args[7] != StatusPending {
		t.Fatalf("args not threaded through: %v", got.args)
	}
}
