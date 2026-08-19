package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// publishTracer is the package-local tracer for the per-batch span
// the poller opens around each pub.Publish call. The span is
// inherited by KafkaPublisher.recordToEnvelope, which lifts it into
// the Envelope's TraceID/SpanID fields (sub-stage 3.10.b).
var publishTracer = otel.Tracer("github.com/t0pm1x/orderflow/outbox")

// PollerConfig tunes the Poller.
type PollerConfig struct {
	// Table is the outbox table name. Used only for logging/metrics
	// labels; Source implementations already know their table.
	Table string

	// BatchSize is the max rows fetched per poll. Default 100.
	BatchSize int

	// Interval is the sleep between empty polls. Default 100ms.
	Interval time.Duration

	// MaxAttempts is the cap on Publish retries before the row is
	// moved to DLQ. Default 5.
	MaxAttempts int
}

// applyDefaults returns a copy of c with zero values replaced by
// the documented defaults.
func (c PollerConfig) applyDefaults() PollerConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Interval <= 0 {
		c.Interval = 100 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	return c
}

// ErrSourceClosed is returned by Poller.Run when the source has been
// closed mid-poll. The poller treats this as a normal shutdown signal.
var ErrSourceClosed = errors.New("outbox: source closed")

// Poller drives one service's outbox table. Construct it once at
// startup and call Run() in a goroutine; Stop() shuts it down.
type Poller struct {
	cfg       PollerConfig
	src       Source
	pub       Publisher
	dlq       DLQ
	metrics   Metrics
	attempts  sync.Map // event_id -> int (atomic-stored via *int32). Deprecated: superseded by source-row attempts; kept for the v1.1.2 attempt-counter fast path while DB-attempts propagates to all 5 services.
	stopCh    chan struct{}
	stopped   atomic.Bool
	runningCh chan struct{}
}

// New constructs a Poller. dlq may be nil (then MaxAttempts>0 is
// ignored — failed rows stay PENDING forever). metrics may be nil
// and defaults to NoopMetrics.
func New(cfg PollerConfig, src Source, pub Publisher, dlq DLQ, metrics Metrics) *Poller {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &Poller{
		cfg:       cfg.applyDefaults(),
		src:       src,
		pub:       pub,
		dlq:       dlq,
		metrics:   metrics,
		stopCh:    make(chan struct{}),
		runningCh: make(chan struct{}),
	}
}

// Stop signals Run to exit at the next iteration boundary.
func (p *Poller) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		close(p.stopCh)
	}
}

// Run polls Source, publishes, and marks rows sent until Stop is
// called or ctx is cancelled. Returns nil on clean shutdown.
//
// One Run loop = one outbox table. Each service starts its own.
//
// Concurrency: src.RunInTx acquires FOR UPDATE SKIP LOCKED on the
// fetched rows for the duration of the publish-and-mark sequence.
// Two replicas running this loop will each see a disjoint batch of
// rows because locked rows are skipped by the other transaction.
// This is the production concurrency contract; the test fake
// implements the same invariant by holding pending rows while fn
// runs.
//
// Failure handling (v1.1.3 fix):
//
//   - publish succeeded → MarkSentTx, COMMIT.
//   - publish failed, every row still under retry budget → no
//     MarkFailedTx, ROLLBACK so rows stay PENDING for the next poll.
//   - publish failed, at least one row past MaxAttempts →
//     MarkFailedTx (status='FAILED' so the row stops being
//     re-fetched) plus DLQ.Send, COMMIT so the FAILED transition
//     sticks. Pre-v1.1.3 this branch also called ROLLBACK which
//     undid the MarkFailedTx, leaving the row PENDING forever and
//     causing DLQ.Send to fire on every subsequent poll.
func (p *Poller) Run(ctx context.Context) error {
	close(p.runningCh)
	defer p.resetAttemptsForTest()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.stopCh:
			return nil
		default:
		}

		start := time.Now()
		err := p.src.RunInTx(ctx, p.cfg.BatchSize, func(tx pgx.Tx, recs []outbox.Record) error {
			p.metrics.ObservePoll(ctx, len(recs), time.Since(start), nil)
			if len(recs) == 0 {
				return errEmptyBatch
			}

			// Read the DB-authoritative retry budget inside the
			// locked tx. v1.1.4 fix: the in-memory sync.Map is
			// still used as a fast-path cache, but the DB column
			// is the source of truth so a pod restart that
			// wipes p.attempts doesn't silently re-DLQ a row
			// that had already crossed the threshold (or skip
			// DLQ-ing a row that has reset budget because of a
			// mid-cycle MarkFailedTx commit fail).
			ids := make([]string, len(recs))
			for i, r := range recs {
				ids[i] = r.EventID
			}
			dbAttempts, dbErr := p.src.AttemptsOfTx(ctx, tx, ids)
			if dbErr != nil {
				return dbErr // rollback
			}

			if err := p.publishBatch(ctx, recs); err != nil {
				p.metrics.ObservePublish(ctx, len(recs), err)
				// For rows past MaxAttempts, MarkFailedTx sets
				// status='FAILED' so the row stops being re-fetched.
				// For rows still under the cap, MarkFailedTx is
				// skipped and we ROLLBACK so they stay PENDING and
				// get re-tried on the next poll.
				//
				// Returning nil here commits the FAILED transition;
				// returning the err rolls back. handlePublishFailure
				// decides per row.
				committable := p.handlePublishFailure(ctx, tx, recs, err, dbAttempts)
				if committable {
					return nil // commit: FAILED rows now terminal; no re-fetch
				}
				return err // rollback: rows stay PENDING, retried next poll
			}
			p.metrics.ObservePublish(ctx, len(recs), nil)

			// Mark the rows SENT inside the same tx so the row's
			// status flip and the row's lock release commit
			// atomically. Without this call, the next poll re-fetches
			// the same rows and re-publishes them — an infinite
			// duplicate loop. (Regression introduced by the v1.1.0-pre
			// refactor that moved MarkSent from a post-loop call
			// site into the RunInTx closure without ever wiring the
			// new MarkSentTx call.) Re-uses `ids` from above.
			if err := p.src.MarkSentTx(ctx, tx, ids); err != nil {
				p.metrics.ObservePublish(ctx, len(recs), err)
				return err // triggers rollback; rows stay PENDING
			}
			for _, r := range recs {
				p.attempts.Delete(r.EventID)
			}
			return nil
		})
		if err != nil && !errors.Is(err, errEmptyBatch) {
			p.metrics.ObservePoll(ctx, 0, time.Since(start), err)
			if !p.sleep(ctx) {
				return nil
			}
			continue
		}
		if !p.sleep(ctx) {
			return nil
		}
	}
}

// errEmptyBatch signals an empty fetch so the poller's outer loop
// can sleep without rolling back the tx.
var errEmptyBatch = errors.New("outbox: empty batch")

// resetAttemptsForTest is exposed for tests; in production it's a
// no-op via defer.
func (p *Poller) resetAttemptsForTest() {}

// publishBatch opens an "outbox.publish" span on ctx and calls
// Publisher.Publish. The span's context is inherited by
// KafkaPublisher.recordToEnvelope (sub-stage 3.10.b) which lifts
// it into each emitted Envelope's TraceID/SpanID.
func (p *Poller) publishBatch(ctx context.Context, recs []outbox.Record) error {
	ctx, span := publishTracer.Start(ctx, "outbox.publish")
	defer span.End()
	return p.pub.Publish(ctx, recs)
}

// sleep waits cfg.Interval or until ctx is cancelled / stop is called.
// Returns false if the poller should exit.
func (p *Poller) sleep(ctx context.Context) bool {
	t := time.NewTimer(p.cfg.Interval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	case <-t.C:
		return true
	}
}

// handlePublishFailure bumps the per-event attempt counter and, on
// MaxAttempts exceeded, routes the row to the DLQ and marks it
// FAILED so the next poll skips it. Rows still under the cap stay
// PENDING — they're re-fetched on the next poll because the outer
// RunInTx rolls back when committable=false.
//
// The retry-budget source of truth is the DB column (`attempts`),
// not the in-memory sync.Map. The closure pre-reads dbAttempts via
// Source.AttemptsOfTx inside the locked tx; the in-memory cache is
// a fast path that mirrors the DB value within a single pod's
// lifetime but does not survive a restart. Taking max(in-memory,
// DB) makes the cache safe to repopulate from zero without under-
// counting.
//
// DLQ transitions are recorded via MarkFailedTx inside the locked
// tx so the row's status flips to FAILED atomically with the row
// lock release. The caller (Run) commits the tx when this function
// returns true so the FAILED transition persists across restarts.
//
// Returns true when at least one row crossed the MaxAttempts
// threshold and was marked FAILED + DLQ'd. The caller should commit
// the tx in that case. Returns false when every row is still under
// the retry budget (no MarkFailedTx was called); the caller rolls
// back so the rows stay PENDING and a subsequent poll retries them.
func (p *Poller) handlePublishFailure(ctx context.Context, tx pgx.Tx, recs []outbox.Record, cause error, dbAttempts map[string]int) bool {
	anyCrossed := false
	for _, r := range recs {
		memCur := p.loadAttempts(r.EventID)
		dbCur := dbAttempts[r.EventID] // 0 when the row has no DB attempts yet
		cur := memCur
		if dbCur > cur {
			cur = dbCur
		}
		next := cur + 1
		p.storeAttempts(r.EventID, next)
		if next >= p.cfg.MaxAttempts {
			anyCrossed = true
			if p.dlq != nil {
				_ = p.dlq.Send(ctx, r, cause.Error())
				p.metrics.ObserveDLQ(ctx, r, cause.Error())
				// MarkFailedTx sets status='FAILED' on the source
				// row AND increments attempts via the inlined
				// "attempts = attempts + 1" SQL. The commit-
				// after-this-path ensures both transitions are
				// durable so the row stops being re-fetched and
				// the next pod sees the actual attempt count.
				if err := p.src.MarkFailedTx(ctx, tx, []string{r.EventID}); err != nil {
					p.metrics.ObservePublish(ctx, len(recs), err)
				}
				p.attempts.Delete(r.EventID)
			}
			// Without a DLQ we leave the row PENDING; it will keep
			// being re-attempted on every poll, which is the
			// pre-DLQ behavior. Operators see the lag grow.
		}
	}
	return anyCrossed
}

func (p *Poller) loadAttempts(id string) int {
	if v, ok := p.attempts.Load(id); ok {
		return int(*v.(*int32))
	}
	return 0
}

func (p *Poller) storeAttempts(id string, n int) {
	v := int32(n)
	p.attempts.Store(id, &v)
}
