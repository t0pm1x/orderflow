package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

type fakeSource struct {
	mu       sync.Mutex
	pending  []outbox.Record
	sent     []string
	failed   []string
	fetchErr error
	markErr  error
}

func (f *fakeSource) FetchPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	n := len(f.pending)
	if n > limit {
		n = limit
	}
	out := make([]outbox.Record, n)
	copy(out, f.pending[:n])
	return out, nil
}

func (f *fakeSource) MarkSent(ctx context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markErr != nil {
		return f.markErr
	}
	f.sent = append(f.sent, ids...)
	// drop the marked ones from pending
	keep := f.pending[:0]
	for _, r := range f.pending {
		found := false
		for _, id := range ids {
			if r.EventID == id {
				found = true
				break
			}
		}
		if !found {
			keep = append(keep, r)
		}
	}
	f.pending = keep
	return nil
}

func (f *fakeSource) MarkFailed(ctx context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, ids...)
	keep := f.pending[:0]
	for _, r := range f.pending {
		found := false
		for _, id := range ids {
			if r.EventID == id {
				found = true
				break
			}
		}
		if !found {
			keep = append(keep, r)
		}
	}
	f.pending = keep
	return nil
}

type fakePublisher struct {
	mu        sync.Mutex
	calls     int32
	batches   [][]string
	errByCall map[int]error
	alwaysErr error
}

func (f *fakePublisher) Publish(ctx context.Context, recs []outbox.Record) error {
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

func (d *fakeDLQ) Send(ctx context.Context, r outbox.Record, reason string) error {
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
	p := New(Config{Table: "t", BatchSize: 10, Interval: 10 * time.Millisecond}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(src.sent) != 2 {
		t.Errorf("sent: got %d want 2", len(src.sent))
	}
	if len(pub.batches) != 1 {
		t.Errorf("publish calls: got %d want 1", len(pub.batches))
	}
	if len(src.pending) != 0 {
		t.Errorf("pending after success: got %d want 0", len(src.pending))
	}
}

func TestPoller_RetriesOnPublishError(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1")}}
	pub := &fakePublisher{alwaysErr: errors.New("kafka down")}
	p := New(Config{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 10}, src, pub, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(src.sent) != 0 {
		t.Errorf("sent on persistent error: got %d want 0", len(src.sent))
	}
	if v, ok := p.attempts.Load("e1"); !ok || *v.(*int32) < 1 {
		t.Errorf("attempts counter not incremented")
	}
}

func TestPoller_RoutesToDLQAfterMaxAttempts(t *testing.T) {
	src := &fakeSource{pending: []outbox.Record{rec("e1")}}
	pub := &fakePublisher{errByCall: map[int]error{0: errors.New("kafka down"), 1: errors.New("kafka down"), 2: errors.New("kafka down")}}
	dlq := &fakeDLQ{}
	p := New(Config{Table: "t", BatchSize: 10, Interval: 5 * time.Millisecond, MaxAttempts: 3}, src, pub, dlq, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(dlq.sent) != 1 || dlq.sent[0] != "e1" {
		t.Errorf("dlq: got %v want [e1]", dlq.sent)
	}
	if len(src.failed) != 1 {
		t.Errorf("failed: got %d want 1", len(src.failed))
	}
	if len(src.pending) != 0 {
		t.Errorf("pending: got %d want 0", len(src.pending))
	}
}

func TestPoller_StopExitsCleanly(t *testing.T) {
	src := &fakeSource{}
	pub := &fakePublisher{}
	p := New(Config{Table: "t", Interval: 50 * time.Millisecond}, src, pub, nil, nil)

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
	p := New(Config{Table: "t", Interval: 50 * time.Millisecond}, src, pub, nil, nil)

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
	p := New(Config{Table: "t", BatchSize: 2, Interval: 5 * time.Millisecond}, src, pub, nil, nil)

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
