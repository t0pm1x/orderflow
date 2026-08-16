package outbox

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/t0pm1x/orderflow/platform/outbox"
)

// fakeDBTX is the same shape used in pkg/platform/outbox tests.
// Kept local so this package's tests don't depend on internals.
type fakeDBTX struct {
	calls []fakeCall
	err   error
}

type fakeCall struct {
	sql  string
	args []any
}

func (f *fakeDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	return pgconn.CommandTag{}, f.err
}

func (f *fakeDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
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

func TestPGWriter_Append_EmitsInsert(t *testing.T) {
	db := &fakeDBTX{}
	w := NewPGWriter()

	err := w.Append(context.Background(), db, outbox.Record{
		EventID:       "evt-1",
		EventType:     "OrderCreated",
		AggregateID:   "ord-1",
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         "order-events",
		Payload:       []byte(`{"x":1}`),
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
		"OrderCreated",
		"ord-1",
		"Order",
		"1.0",
		"order-events",
		[]byte(`{"x":1}`),
		outbox.StatusPending,
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

func TestPGWriter_Append_PropagatesExecError(t *testing.T) {
	db := &fakeDBTX{err: pgErr("deadlock")}
	w := NewPGWriter()
	err := w.Append(context.Background(), db, outbox.Record{
		EventID:   "e",
		EventType: "OrderCreated",
		Topic:     "order-events",
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

type pgErr string

func (e pgErr) Error() string { return string(e) }
