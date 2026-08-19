package handlers

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"
)

// replayWindow is the lifetime of a recorded token. After this
// elapsed time a repeat observation is no longer treated as a
// replay — this bounds false-positive 409s for honest users
// who click "back" and resubmit an old form.
const replayWindow = 5 * time.Minute

// replayCacheMax is the soft size watermark that triggers the
// opportunistic GC pass. The GC pass runs on every check call
// (cheap), so this is a memory ceiling more than a perf knob.
const replayCacheMax = 1024

// newIdempotencyToken returns a 22-character URL-safe opaque
// nonce (16 bytes of crypto/rand, base64.RawURLEncoding). It is
// the per-form-render key embedded in hidden inputs and looked
// up in the replay cache.
func newIdempotencyToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// replayCache records which idempotency tokens the BFF has
// observed during this process lifetime. The TTL (replayWindow)
// bounds the false-positive rate for old forms; the soft cap
// (replayCacheMax) bounds memory under sustained load via
// opportunistic GC.
//
// Not safe to copy after first use (the mutex must not be
// copied). Construction is via newReplayCache().
type replayCache struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
}

// newReplayCache returns an initialized cache ready to accept
// the first check call.
func newReplayCache() *replayCache {
	return &replayCache{lastSeen: make(map[string]time.Time)}
}

// check records token as observed and returns true iff this is
// NOT the first observation within replayWindow. First-time
// observations return false so the caller proceeds with the
// mutation; replays return true so the caller returns 409.
//
// On every call the cache also runs an opportunistic GC pass if
// the entry count has crossed replayCacheMax, removing any
// entries older than replayWindow. The GC cost is amortized
// because the soft cap is much larger than the typical
// burst (a single user session is <10 tokens).
func (c *replayCache) check(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if last, ok := c.lastSeen[token]; ok {
		if now.Sub(last) < replayWindow {
			return true
		}
	}
	c.lastSeen[token] = now
	if len(c.lastSeen) > replayCacheMax {
		cutoff := now.Add(-replayWindow)
		for k, v := range c.lastSeen {
			if v.Before(cutoff) {
				delete(c.lastSeen, k)
			}
		}
	}
	return false
}