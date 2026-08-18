package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRedisDeduper_RoundTrip: Seen → false, Mark → true, TTL set.
func TestRedisDeduper_RoundTrip(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	d := NewRedisDeduper(client, "", 5*time.Minute)
	ctx := context.Background()

	if seen, err := d.Seen(ctx, "e1"); err != nil || seen {
		t.Fatalf("expected unseen/no-err; got seen=%v err=%v", seen, err)
	}
	if err := d.Mark(ctx, "e1"); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if seen, err := d.Seen(ctx, "e1"); err != nil || !seen {
		t.Fatalf("expected seen/no-err; got seen=%v err=%v", seen, err)
	}

	// Different event_id must remain unseen.
	if seen, _ := d.Seen(ctx, "e2"); seen {
		t.Fatal("isolated key must not collide with other event_ids")
	}
}

// TestRedisDeduper_TTLExpiry: when Redis evicts the key (miniredis
// honours TTLs), Seen reports false again — operator must extend
// the TTL past Kafka's retention to keep dedup effective.
func TestRedisDeduper_TTLExpiry(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	d := NewRedisDeduper(client, "", 50*time.Millisecond)
	ctx := context.Background()
	if err := d.Mark(ctx, "e1"); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	s.FastForward(100 * time.Millisecond)
	if seen, _ := d.Seen(ctx, "e1"); seen {
		t.Fatal("key must expire past TTL")
	}
}

// TestRedisDeduper_PrefixIsolation: events with the same id but
// different prefixes must not collide (lets multiple services share
// one Redis without cross-talk).
func TestRedisDeduper_PrefixIsolation(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	dA := NewRedisDeduper(client, "svc-a:", time.Hour)
	dB := NewRedisDeduper(client, "svc-b:", time.Hour)
	ctx := context.Background()

	if err := dA.Mark(ctx, "shared-id"); err != nil {
		t.Fatalf("Mark A: %v", err)
	}
	if seen, _ := dB.Seen(ctx, "shared-id"); seen {
		t.Fatal("different prefix must isolate keys")
	}
}
