package idempotency

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// newTestClient returns a Redis client pointing at localhost:6379.
// Tests that require Redis should t.Skip when it is unreachable so
// `go test ./...` works in environments without Redis (CI without
// service containers, local machines that haven't started docker).
//
// Run integration tests with: go test ./internal/idempotency/...
// Skip them (the default) with: go test -short ./...
func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("redis integration test (skipped in -short mode)")
	}
	return redis.NewClient(&redis.Options{Addr: "localhost:6379"})
}

func TestStore_BeginAndComplete(t *testing.T) {
	client := newTestClient(t)
	defer client.Close()

	s := NewStore(client)
	ctx := context.Background()

	res, err := s.Begin(ctx, "test-key-1")
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}

	// First Begin on a new key succeeds.
	if res == nil {
		t.Fatal("expected non-nil reservation")
	}

	// Complete stores the response under the key.
	if err := s.Complete(ctx, res, []byte(`{"status":"ok"}`)); err != nil {
		t.Fatal(err)
	}

	// Second Begin on the same key returns ErrDuplicate with cached body.
	_, err = s.Begin(ctx, "test-key-1")
	dup, ok := IsDuplicate(err)
	if !ok {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
	if len(dup.CachedResponse) == 0 {
		t.Error("expected cached response to be non-empty")
	}
}
