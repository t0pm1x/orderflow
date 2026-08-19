package handlers

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestNewIdempotencyToken pins the wire-level contract: 16 bytes
// of crypto/rand, base64.RawURLEncoding => 22-char URL-safe
// opaque string, never empty. The token is the cache key for the
// replay guard; collisions must be astronomically unlikely
// (2^128 keyspace) and the format must be safe to embed in form
// hidden fields without escaping.
func TestNewIdempotencyToken(t *testing.T) {
	tok := newIdempotencyToken()
	if tok == "" {
		t.Fatal("token must not be empty")
	}
	if len(tok) != 22 {
		t.Errorf("token length: got %d want 22 (16 bytes base64.RawURLEncoding)", len(tok))
	}
	if strings.ContainsAny(tok, "+/=") {
		t.Errorf("token must be base64.RawURLEncoding (URL-safe, no padding): got %q", tok)
	}
	a := newIdempotencyToken()
	b := newIdempotencyToken()
	if a == b {
		t.Errorf("two consecutive tokens must differ: both %q", a)
	}
}

// TestReplayCache_FirstCall_NotReplay pins the cold-path contract:
// a token we've never seen is recorded and NOT reported as a
// replay.
func TestReplayCache_FirstCall_NotReplay(t *testing.T) {
	c := newReplayCache()
	if c.check("token-A") {
		t.Error("first call must not report replay")
	}
}

// TestReplayCache_SecondCall_Replay pins the warm-path contract:
// the second observation of the same token within the TTL is
// reported as a replay so the BFF returns 409.
func TestReplayCache_SecondCall_Replay(t *testing.T) {
	c := newReplayCache()
	if c.check("token-A") {
		t.Fatal("first call must not report replay")
	}
	if !c.check("token-A") {
		t.Error("second call must report replay")
	}
}

// TestReplayCache_DistinctTokens_Independent pins the negative
// case: token-A being replayed does not affect the first
// observation of unrelated token-B.
func TestReplayCache_DistinctTokens_Independent(t *testing.T) {
	c := newReplayCache()
	_ = c.check("token-A")
	_ = c.check("token-A")
	if c.check("token-B") {
		t.Error("token-B must not be a replay on first call")
	}
}

// TestReplayCache_TTLExpiry pins the TTL contract: once a token
// is older than the replay window (5 min), it must be treated as
// fresh again. This bounds replay-protection false-positives
// against an honest user clicking "back" and resubmitting an old
// form. We manipulate the clock via the package-private setter
// (only available in this internal-test package).
func TestReplayCache_TTLExpiry(t *testing.T) {
	c := newReplayCache()
	_ = c.check("token-A")
	c.lastSeen["token-A"] = time.Now().Add(-10 * time.Minute)
	if c.check("token-A") {
		t.Error("token aged past TTL must not report replay")
	}
}

// TestReplayCache_GCAt_TrimsStale pins the opportunistic GC
// contract: when the map crosses the 1024-entry watermark, entries
// older than the TTL are dropped to bound memory under sustained
// load.
func TestReplayCache_GCAt_TrimsStale(t *testing.T) {
	c := newReplayCache()
	c.lastSeen["stale-1"] = time.Now().Add(-10 * time.Minute)
	c.lastSeen["stale-2"] = time.Now().Add(-10 * time.Minute)
	c.lastSeen["fresh-1"] = time.Now()
	c.lastSeen["fresh-2"] = time.Now()
	// Bump size past the watermark with unique fresh entries.
	for i := 0; i < 1024; i++ {
		c.lastSeen[fmt.Sprintf("fresh-fill-%04d", i)] = time.Now()
	}
	// Trigger another check — first call records + GC runs.
	_ = c.check("trigger")
	if _, ok := c.lastSeen["stale-1"]; ok {
		t.Error("stale-1 must be evicted by opportunistic GC")
	}
	if _, ok := c.lastSeen["stale-2"]; ok {
		t.Error("stale-2 must be evicted by opportunistic GC")
	}
	if _, ok := c.lastSeen["fresh-1"]; !ok {
		t.Error("fresh-1 must survive GC")
	}
}
