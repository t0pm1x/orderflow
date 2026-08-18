package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestStore returns a Store backed by miniredis.
func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewStore(client), s
}

// TestStore_Begin_InFlightReturnsErrInFlight: when the same
// Idempotency-Key is claimed by an in-flight call (key value is
// the literal "in-flight" marker), Begin must return *ErrInFlight
// — NOT the placeholder as a cached response body. The middleware
// translates ErrInFlight into HTTP 409 Conflict so concurrent
// retries don't echo back the placeholder string.
func TestStore_Begin_InFlightReturnsErrInFlight(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if _, err := store.Begin(ctx, "k1"); err != nil {
		t.Fatalf("first Begin: %v", err)
	}
	_, err := store.Begin(ctx, "k1")
	var inFlight *ErrInFlight
	if !errors.As(err, &inFlight) {
		t.Fatalf("second Begin: got %v, want *ErrInFlight", err)
	}
}

// TestStore_Begin_CompletedReturnsErrDuplicateWithBody: after
// Complete() runs, a subsequent Begin returns ErrDuplicate whose
// CachedResponse is the body Complete wrote — never the in-flight
// placeholder.
func TestStore_Begin_CompletedReturnsErrDuplicateWithBody(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	res, err := store.Begin(ctx, "k1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	body := []byte(`{"status":"ok","payment_id":"p-1"}`)
	if err := store.Complete(ctx, res, body); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	_, err = store.Begin(ctx, "k1")
	var dup *ErrDuplicate
	if !errors.As(err, &dup) {
		t.Fatalf("after Complete: got %v, want *ErrDuplicate", err)
	}
	if string(dup.CachedResponse) != string(body) {
		t.Errorf("cached body: got %q want %q", dup.CachedResponse, body)
	}
}

// TestStore_ReleaseAllowsRetry: Release removes the marker so the
// next Begin succeeds (used by the middleware on 5xx so the client
// can retry the same Idempotency-Key).
func TestStore_ReleaseAllowsRetry(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	res, err := store.Begin(ctx, "k1")
	if err != nil {
		t.Fatalf("first Begin: %v", err)
	}
	if err := store.Release(ctx, res); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := store.Begin(ctx, "k1"); err != nil {
		t.Errorf("after Release: Begin must succeed; got %v", err)
	}
}

// suppress unused warning for time when this file is the only
// thing imported in a tiny build.
var _ = time.Second
