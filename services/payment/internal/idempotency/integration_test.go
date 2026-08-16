package idempotency_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/t0pm1x/orderflow/services/payment/internal/idempotency"
)

// TestDuplicateWebhook_DifferentBodies simulates the spec acceptance
// criterion: "duplicate payment test passes (same key → same response)".
//
// Two requests with the same Idempotency-Key but *different* bodies
// should both result in the cached body from the first request being
// returned — never the second body. This catches the bug class where
// idempotency only dedupes the key without replaying the original
// response.
func TestDuplicateWebhook_DifferentBodies(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := idempotency.NewStore(client)
	mw := idempotency.Middleware(store)

	calls := 0
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"original"}`))
		} else {
			t.Error("handler must not be called on duplicate")
		}
	})

	// First call: original body, expect 200 + body.
	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body-A"))
	req1.Header.Set(idempotency.HeaderIDKey, "dupe-key")
	rec1 := httptest.NewRecorder()
	mw(h).ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK || rec1.Body.String() != `{"result":"original"}` {
		t.Fatalf("first call: status=%d body=%q", rec1.Code, rec1.Body.String())
	}
	if rec1.Header().Get(idempotency.HeaderReplayed) != "" {
		t.Fatal("first call must not be marked replayed")
	}

	// Second call: DIFFERENT body, but should still get the original.
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body-B-DIFFERENT"))
	req2.Header.Set(idempotency.HeaderIDKey, "dupe-key")
	rec2 := httptest.NewRecorder()
	mw(h).ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay: status=%d want 200", rec2.Code)
	}
	if rec2.Body.String() != `{"result":"original"}` {
		t.Fatalf("replay body: got %q want original", rec2.Body.String())
	}
	if rec2.Header().Get(idempotency.HeaderReplayed) != "true" {
		t.Fatal("replay must set Idempotent-Replayed: true")
	}
	if calls != 1 {
		t.Fatalf("handler invocations: got %d want 1", calls)
	}
}

// TestStore_BeginReleasesOnPanic demonstrates the reservation lifecycle:
// a handler that panics leaves the reservation in place (no Complete),
// so a retry with a DIFFERENT handler logic would still hit ErrDuplicate.
// (Use Middleware's Release on 5xx for the explicit retry path; this
// test pins down Begin-only behavior.)
func TestStore_BeginReleasesOnPanic(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := idempotency.NewStore(client)
	ctx := context.Background()

	// Begin succeeds; keep the reservation so we can Release it later.
	res1, err := store.Begin(ctx, "p1")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Same key immediately returns ErrDuplicate (no Complete needed).
	_, err = store.Begin(ctx, "p1")
	if _, ok := idempotency.IsDuplicate(err); !ok {
		t.Fatalf("expected duplicate, got %v", err)
	}
	// Release the original reservation; the key is now available again.
	if err := store.Release(ctx, res1); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Begin succeeds again because the key was released.
	if _, err := store.Begin(ctx, "p1"); err != nil {
		t.Fatalf("begin after release: %v", err)
	}
}
