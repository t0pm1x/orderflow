package consumer

import (
	"context"
	"sync"
	"time"
)

// InMemoryDeduper is the default Deduper for tests. Production
// uses a Redis- or Postgres-backed one (see 3.8.b follow-up).
type InMemoryDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewInMemoryDeduper constructs an empty deduper.
func NewInMemoryDeduper() *InMemoryDeduper {
	return &InMemoryDeduper{seen: map[string]struct{}{}}
}

// Seen reports whether eventID has been processed.
func (d *InMemoryDeduper) Seen(_ context.Context, eventID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[eventID]
	return ok, nil
}

// Mark records eventID as processed.
func (d *InMemoryDeduper) Mark(_ context.Context, eventID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[eventID] = struct{}{}
	return nil
}

// NoopDeduper returns Seen=false for every call. Use it when you
// don't want dedup (e.g. for handlers that are themselves
// idempotent at the DB layer).
type NoopDeduper struct{}

// Seen always reports false.
func (NoopDeduper) Seen(context.Context, string) (bool, error) { return false, nil }

// Mark is a no-op.
func (NoopDeduper) Mark(context.Context, string) error { return nil }

// compile-time interface checks.
var (
	_ Deduper = (*InMemoryDeduper)(nil)
	_ Deduper = NoopDeduper{}
)

// suppress unused-time-import warning when this file is the only
// thing imported in a tiny build.
var _ = time.Second
