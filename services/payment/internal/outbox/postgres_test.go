package outbox

import (
	"bytes"
	"context"
	"testing"

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

func (f *fakeDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, fakeCall{sql: sql, args: args})
	return pgconn.CommandTag{}, f.err
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
		EventType:     "PaymentCompleted",
		AggregateID:   "pay-1",
		AggregateType: "Payment",
		SchemaVersion: "1.0",
		Topic:         "payment-events",
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
		"PaymentCompleted",
		"pay-1",
		"Payment",
		"1.0",
		"payment-events",
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
		EventType: "PaymentCompleted",
		Topic:     "payment-events",
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

type pgErr string

func (e pgErr) Error() string { return string(e) }
