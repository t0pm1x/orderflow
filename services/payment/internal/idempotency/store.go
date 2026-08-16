// Package idempotency provides Redis-backed dedupe of webhook events
// by Idempotency-Key header. Duplicate requests return the cached response.
package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ErrDuplicate is returned when an Idempotency-Key has already been processed.
type ErrDuplicate struct {
	CachedResponse []byte
}

func (e *ErrDuplicate) Error() string {
	return "idempotency: duplicate request"
}

// Store is a Redis-backed idempotency cache.
type Store struct {
	client *redis.Client
	ttl    time.Duration
}

// NewStore constructs a Store backed by client with a 24h TTL on keys.
func NewStore(client *redis.Client) *Store {
	return &Store{client: client, ttl: 24 * time.Hour}
}

const keyPrefix = "payment:idem:"

func (s *Store) key(idempotencyKey string) string {
	return keyPrefix + idempotencyKey
}

// Reservation represents a successful claim of an idempotency key.
// The caller must call Complete() to cache the response or Release()
// on failure to allow a retry.
type Reservation struct {
	Key   string
	Token string
}

// Begin atomically claims idempotencyKey. On success, returns a
// Reservation the caller uses to Complete() or Release(). If the key
// has already been processed, returns *ErrDuplicate carrying the
// cached response.
func (s *Store) Begin(ctx context.Context, idempotencyKey string) (*Reservation, error) {
	key := s.key(idempotencyKey)
	// SETNX with TTL — if key exists, returns 0 (duplicate); else returns 1 (reserved)
	ok, err := s.client.SetNX(ctx, key, "in-flight", s.ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		// Already exists — fetch cached response
		cached, err := s.client.Get(ctx, key).Bytes()
		if err == redis.Nil {
			// Race: key expired between SETNX and GET. Retry once.
			return s.Begin(ctx, idempotencyKey)
		}
		if err != nil {
			return nil, err
		}
		return nil, &ErrDuplicate{CachedResponse: cached}
	}
	return &Reservation{Key: idempotencyKey, Token: uuid.NewString()}, nil
}

// Complete stores the raw response body bytes under the reservation
// key, replacing the in-flight marker. The bytes are what will be
// returned on duplicate replay (see ErrDuplicate.CachedResponse), so
// the caller is responsible for any encoding (e.g. JSON) before
// passing them in.
func (s *Store) Complete(ctx context.Context, res *Reservation, body []byte) error {
	return s.client.Set(ctx, s.key(res.Key), body, s.ttl).Err()
}

// Release deletes the reservation key (use on errors so a retry can succeed).
func (s *Store) Release(ctx context.Context, res *Reservation) error {
	return s.client.Del(ctx, s.key(res.Key)).Err()
}

// IsDuplicate reports whether err is an *ErrDuplicate and, if so,
// returns it.
func IsDuplicate(err error) (*ErrDuplicate, bool) {
	var dup *ErrDuplicate
	if errors.As(err, &dup) {
		return dup, true
	}
	return nil, false
}
