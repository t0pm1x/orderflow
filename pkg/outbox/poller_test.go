package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

type fakeSource struct {
	mu         sync.Mutex
	pending    []outbox.Record
	sent       []string
	failed     []string
	fetchErr   error
	markErr    error
	dbAttempts map[string]int
}

// RunInTx simulates FOR UPDATE SKIP LOCKED for unit tests: while fn
// is running, the locked rows are removed from the visible pending
// list so a concurrent RunInTx (in the same fake) would skip them.
// In a single-goroutine test the lock is released when fn returns.
func (f *fakeSource) RunInTx(_ context.Context, limit int, fn func(_ pgx.Tx, _ []outbox.Record) error) error {
	f.mu.Lock()
	if f.fetchErr != nil {
		f.mu.Unlock()
		return f.fetchErr
	}
	n := len(f.pending)
	if n > limit {
		n = limit
	}
	batch := make([]outbox.Record, n)
	copy(batch, f.pending[:n])
	// Simulate the row lock by removing the batch from pending until
	// fn returns.
	f.pending = f.pending[n:]
	f.mu.Unlock()

	err := fn(nil, batch)

	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		// commit: drop the batch (already removed)
		return nil
	}
	// rollback: put the batch back at the head of pending so the
	// next poll re-fetches them.
	f.pending = append(batch, f.pending...)
	return err
}

func (f *fakeSource) MarkSentTx(_ context.Context, _ pgx.Tx, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markErr != nil {
		return f.markErr
	}
	f.sent = append(f.sent, ids...)
	return nil
}

func (f *fakeSource) MarkFailedTx(_ context.Context, _ pgx.Tx, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, ids...)
	return nil
}

// AttemptsOfTx reports the attempts counter for the given event_ids
// from the fake's pending slice (which doubles as a state machine
// for tests that need it). Tests that exercise DLQ thresholds should
// pre-seed attempts via the AttemptsOfTx response map.
func (f *fakeSource) AttemptsOfTx(_ context.Context, _ pgx.Tx, ids []string) (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dbAttempts == nil {
		f.dbAttempts = map[string]int{}
	}
	out := make(map[string]int, len(ids))
	for _, id := range ids {
		out[id] = f.dbAttempts[id]
	}
	return out, nil
}

type fakePublisher struct {
	mu        sync.Mutex
	calls     int32
	batches   [][]string
	errByCall map[int]error
	alwaysErr error
}

func (f *fakePublisher) Publish(_ context.Context, recs []outbox.Record) error {
	idx := int(atomic.AddInt32(&f.calls, 1)) - 1
	if f.alwaysErr != nil {
		return f.alwaysErr
	}
	if err, ok := f.errByCall[idx]; ok && err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, len(recs))
	for i, r := range recs {
		ids[i] = r.EventID
	}
	f.batches = append(f.batches, ids)
	return nil
}

type fakeDLQ struct {
	mu      sync.Mutex
	sent    []string
	reasons []string
}

func (d *fakeDLQ) Send(_ context.Context, r outbox.Record, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sent = append(d.sent, r.EventID)
	d.reasons = append(d.reasons, reason)
	return nil
}

func rec(id string) outbox.Record {
	return outbox.Record{
		EventID:       id,
		EventType:     "T",
		AggregateID:   "agg-" + id,
		AggregateType: "Agg",
		SchemaVersion: "1.0",
		Topic:         "topic",
		Payload:       []byte(`{}`),
	}
}

func TestPoller_PollsAndPublishesOnce(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1"), rec("e2")}}
	pub := &fakePublisher{}
	p := New(PollerConfig{Table: "t", BatchSize: 10, Interval: 10 * time.Millisecond}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	// pending is drained when RunInTx commits; sent/failed are not
	// populated by the fake because the poller no longer calls
	// MarkSent/MarkFailed outside the fn callback.
	if len(src.pending) != 0 {
		t.Errorf("pending after success: got %d want 0", len(src.pending))
	}
	if len(pub.batches) != 1 {
		t.Errorf("publish calls: got %d want 1", len(pub.batches))
	}
	// ADVERSARIAL: the poller MUST transition rows to SENT so the
	// next poll doesn't re-publish them. Without MarkSentTx,
	// the same event is published to Kafka on every iteration
	// forever — the row stays PENDING in the DB.
	if got := len(src.sent); got != 2 {
		t.Errorf("MarkSentTx calls: got %d want 2 (e1, e2). Without this, every event is re-published on every poll.", got)
	}
}

func TestPoller_RetriesOnPublishError(t *testing.T) {
	// MaxAttempts high enough that the 80ms test window cannot
	// trigger the FAILED/DLQ transition; this asserts the under-cap
	// rollback path. After cap, the row leaves pending (see
	// TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown).
	src := &fakeSource{pending: []outbox.Record{rec("e1")}}
	pub := &fakePublisher{alwaysErr: errors.New("kafka down")}
	p := New(PollerConfig{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 1000}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(src.pending) != 1 {
		t.Errorf("pending on persistent error: got %d want 1 (rolled back under cap)", len(src.pending))
	}
	if v, ok := p.attempts.Load("e1"); !ok || *v.(*int32) < 1 {
		t.Errorf("attempts counter not incremented")
	}
}

func TestPoller_RoutesToDLQAfterMaxAttempts(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1")}}
	pub := &fakePublisher{errByCall: map[int]error{0: errors.New("kafka down"), 1: errors.New("kafka down"), 2: errors.New("kafka down")}}
	dlq := &fakeDLQ{}
	p := New(PollerConfig{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 3}, src, pub, dlq, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(dlq.sent) != 1 || dlq.sent[0] != "e1" {
		t.Errorf("dlq: got %v want [e1]", dlq.sent)
	}
	if len(src.failed) != 1 || src.failed[0] != "e1" {
		t.Errorf("failed: got %v want [e1]", src.failed)
	}
	if len(src.pending) != 0 {
		t.Errorf("pending after DLQ: got %d want 0", len(src.pending))
	}
}

// TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown is the v1.1.3
// regression net: before the fix, the poller rolled back the
// MarkFailedTx inside the same tx as the publish failure, so the
// row stayed PENDING and the in-memory attempts counter kept
// incrementing on each poll — the DLQ.Send fired once every 3
// polls (~33 fires in 500ms). With the fix, MarkFailedTx is
// committed (status='FAILED') and the row stops being re-fetched,
// so the DLQ sees exactly one entry.
func TestPoller_DoesNotDoubleDLQOnPersistentBrokerDown(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1")}}
	pub := &fakePublisher{alwaysErr: errors.New("kafka down")}
	dlq := &fakeDLQ{}
	p := New(PollerConfig{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 3}, src, pub, dlq, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(dlq.sent) != 1 {
		t.Errorf("dlq.sent: got %d want 1 (pre-fix: ~33 fires because rollback-undid MarkFailedTx and the row kept re-fetching)", len(dlq.sent))
	}
	if len(src.failed) != 1 {
		t.Errorf("MarkFailedTx calls: got %d want 1", len(src.failed))
	}
}

func TestPoller_StopExitsCleanly(t *testing.T) {
	src := &fakeSource{}
	pub := &fakePublisher{}
	p := New(PollerConfig{Table: "t", Interval: 50 * time.Millisecond}, src, pub, nil, nil)

	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Stop()
	}()
	if err := p.Run(context.Background()); err != nil {
		t.Errorf("Run after Stop: %v", err)
	}
}

func TestPoller_ContextCancelExitsCleanly(t *testing.T) {
	src := &fakeSource{}
	pub := &fakePublisher{}
	p := New(PollerConfig{Table: "t", Interval: 50 * time.Millisecond}, src, pub, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := p.Run(ctx); err != nil {
		t.Errorf("Run after ctx cancel: %v", err)
	}
}

func TestPoller_BatchSizeRespected(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1"), rec("e2"), rec("e3"), rec("e4"), rec("e5")}}
	pub := &fakePublisher{}
	p := New(PollerConfig{Table: "t", BatchSize: 2, Interval: 5 * time.Millisecond}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	// 5 rows, batch=2 → at least 3 Publish calls (last batch may be
	// partial). We assert ≥3 without pinning the exact count, since
	// timing decides when the last 1-row batch lands.
	if got := int(atomic.LoadInt32(&pub.calls)); got < 3 {
		t.Errorf("publish calls: got %d want ≥3", got)
	}
}

// TestPoller_DBQueriesAttemptsForDLQ is the v1.1.4 regression net:
// the poller's DLQ-budget decision must read attempts from the DB
// row (Source.AttemptsOfTx), not rely solely on the in-memory
// sync.Map. Simulating a "fresh pod, but the DB row is already
// past MaxAttempts because a previous pod started the budget"
// scenario: pre-seed dbAttempts[e1]=MaxAttempts-1; with one
// publish failure the row crosses the threshold on the very first
// poll in the new pod, not after MaxAttempts new failures.
func TestPoller_DBQueriesAttemptsForDLQ(t *testing.T) {
	src := &fakeSource{
		pending:    []outbox.Record{rec("e1")},
		dbAttempts: map[string]int{"e1": 4}, // pre-seed: previous pod almost crossed
	}
	pub := &fakePublisher{alwaysErr: errors.New("kafka down")}
	dlq := &fakeDLQ{}
	// MaxAttempts=5; e1 starts with dbAttempts=4, so the FIRST
	// observed failure increments to 5 and DLQ-fires.
	p := New(PollerConfig{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 5}, src, pub, dlq, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(dlq.sent) != 1 || dlq.sent[0] != "e1" {
		t.Errorf("dlq: got %v want [e1] (DB seed attempts=4 + 1 publish failure must cross MaxAttempts=5)", dlq.sent)
	}
	if len(src.failed) != 1 {
		t.Errorf("MarkFailedTx calls: got %d want 1", len(src.failed))
	}
}
