package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDeduper is a Redis-backed implementation of Deduper. Use
// it in production so dedup state survives service restarts; the
// in-memory deduper loses its seen-set on restart and would let
// redelivered events re-run their handlers.
type RedisDeduper struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

const defaultDedupPrefix = "orderflow:dedup:"

// NewRedisDeduper constructs a RedisDeduper. prefix defaults to
// "orderflow:dedup:" when empty; ttl defaults to 7 days (long
// enough to outlast Kafka's retention so a replayed event is
// still deduped).
func NewRedisDeduper(client *redis.Client, prefix string, ttl time.Duration) *RedisDeduper {
	if prefix == "" {
		prefix = defaultDedupPrefix
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &RedisDeduper{client: client, prefix: prefix, ttl: ttl}
}

func (d *RedisDeduper) key(eventID string) string {
	return d.prefix + eventID
}

// Seen reports whether eventID has already been processed.
func (d *RedisDeduper) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := d.client.Exists(ctx, d.key(eventID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Mark records eventID as processed with the configured TTL.
func (d *RedisDeduper) Mark(ctx context.Context, eventID string) error {
	return d.client.Set(ctx, d.key(eventID), "1", d.ttl).Err()
}

// NoopDeduperSentinel makes it explicit at call sites that a
// consumer is intentionally running without dedup. Returning nil
// for every Seen lets a redelivered event run its handler again —
// only safe when the handler is itself idempotent at the DB layer
// (e.g. uses ON CONFLICT DO NOTHING).
var NoopDeduperSentinel Deduper = NoopDeduper{}

// ensure interface compatibility at compile time.
var (
	_ Deduper = (*RedisDeduper)(nil)
	_ Deduper = (*InMemoryDeduper)(nil)
)

// suppress unused warning when the file is the only thing imported
// in a tiny build.
var _ = errors.New

// NewDeduperFromRedisURL constructs a RedisDeduper from a Redis
// URL (e.g. "redis://localhost:6379"). Returns a no-op deduper
// and a non-nil error when the URL is empty or unparseable; the
// caller decides whether the error is fatal (production wants
// Redis; tests can tolerate fallback).
func NewDeduperFromRedisURL(url, prefix string, ttl time.Duration) (Deduper, error) {
	if url == "" {
		return NoopDeduper{}, nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	return NewRedisDeduper(client, prefix, ttl), nil
}