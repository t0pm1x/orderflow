package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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

	// Interval is the base sleep between empty polls. Default 100ms.
	// Used as the base of the exponential backoff (OBX-004): on a
	// failing iteration the actual sleep grows as
	// min(Interval * 2^consecutiveFailures, MaxInterval) ± jitter.
	Interval time.Duration

	// MaxAttempts is the poison-message cap. A row is DLQ'd only
	// when both MaxAttempts AND MaxRetryAge have been exceeded
	// (OBX-004). 0 is treated as the default (5).
	MaxAttempts int

	// MaxRetryAge is the infrastructure-outage cap. Combined with
	// MaxAttempts: a row must be both past the attempt budget AND
	// the time budget before it is moved to the DLQ. 0 disables
	// the cap (DLQ on MaxAttempts alone). Production should set
	// this explicitly (recommended: 15 minutes).
	MaxRetryAge time.Duration

	// MaxInterval caps the exponential backoff between polls.
	// 0 disables the cap. Production should set this explicitly
	// (recommended: 5s).
	MaxInterval time.Duration

	// JitterFraction is the ± fraction of full-jitter applied to
	// the exponential backoff sleep. 0 = no jitter (deterministic,
	// for tests). Production should set this explicitly
	// (recommended: 0.2).
	JitterFraction float64
}

// applyDefaults returns a copy of c with zero values replaced by
// the documented defaults. MaxRetryAge/MaxInterval/JitterFraction
// are NOT defaulted here because 0 is a meaningful value
// (disabled). Production main.go sets them explicitly.
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
	cfg                 PollerConfig
	src                 Source
	pub                 Publisher
	dlq                 DLQ
	metrics             Metrics
	attempts            sync.Map // event_id -> *int32 (in-memory attempt counter fast-path)
	firstSeen           sync.Map // event_id -> time.Time (when the row was first observed; used for MaxRetryAge)
	consecutiveFailures int      // failed-iteration counter for OBX-004 exponential backoff
	rng                 *rand.Rand
	rngMu               sync.Mutex
	stopCh              chan struct{}
	stopped             atomic.Bool
	runningCh           chan struct{}
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
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
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
		// OBS-9: refresh the outbox_pending_events /
		// outbox_failed_events gauges before the per-cycle work so a
		// dashboard scrape mid-cycle still sees a recent snapshot.
		// A Lag error must not abort the poll (the cycle is more
		// important than the gauge), so the error is logged via
		// ObservePoll's outcome="lag_error" path and otherwise
		// swallowed — the next cycle will retry.
		if pending, failed, lagErr := p.src.Lag(ctx); lagErr != nil {
			p.metrics.ObservePoll(ctx, 0, time.Since(start), lagErr)
		} else {
			p.metrics.ObserveLag(ctx, pending, failed)
		}
		// terminalIDs holds the event_ids that reached a terminal
		// state in this iteration (SENT or FAILED). On a successful
		// commit, Run() resets the in-memory attempt counters for
		// these ids — this must run AFTER the tx commits so a
		// rollback preserves the budget (OBX-003: a tx that
		// "commits a rollback" must not reset the counter).
		//
		// NOT every event in the batch has a terminal transition:
		// an under-cap publish failure leaves rows PENDING, and
		// their counters MUST survive for the next poll's budget
		// decision.
		//
		// failedIDs captures every event_id whose publish attempt
		// failed in this iteration. BumpAttempts is called for each
		// AFTER RunInTx returns (see post-RunInTx block below) so
		// the locked-tx's pool connection is released first. Calling
		// BumpAttempts inside the closure contends with the tx for
		// a pool slot and, when the outer ctx is short (tests) or
		// the pool is saturated, exhausts the ctx budget and
		// prevents MarkFailedTx from completing.
		var terminalIDs []string
		var failedIDs []string
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
				// Capture the ids so BumpAttempts can run them
				// after RunInTx returns (avoids contending with
				// the locked tx for a pool slot — see the
				// failedIDs comment in Run).
				idsList := make([]string, len(recs))
				for i, r := range recs {
					idsList[i] = r.EventID
				}
				failedIDs = idsList
				// For rows past MaxAttempts AND MaxRetryAge,
				// MarkFailedTx sets status='FAILED' so the row
				// stops being re-fetched. For rows still under
				// either cap, or when DLQ.Send / MarkFailedTx
				// itself errors, we return the error so the tx
				// rolls back and rows stay PENDING.
				failed := p.handlePublishFailure(ctx, tx, recs, err, dbAttempts)
				if failed.err != nil {
					return failed.err // rollback: rows stay PENDING, retried next poll
				}
				if len(failed.terminalIDs) == 0 {
					// No terminal transition (every row still
					// under MaxAttempts / MaxRetryAge, or no DLQ
					// configured). Roll back so the row stays
					// PENDING for the next poll; the in-memory
					// counter is preserved.
					return errUnderCap
				}
				terminalIDs = failed.terminalIDs
				return nil // commit: FAILED rows now terminal; no re-fetch
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
			terminalIDs = ids
			return nil
		})
		// OBX-001 (bump-on-failure): run the autonomous
		// BumpAttempts UPDATE for every event that hit a publish
		// failure this iteration. Done OUTSIDE RunInTx so the
		// locked tx's pool connection is released first; see the
		// failedIDs comment in Run and handlePublishFailure's
		// own comment block. A short bounded ctx (bumpBudget)
		// caps the wait — the bump is best-effort and must not
		// block the publish-failure path on a saturated pool.
		if len(failedIDs) > 0 {
			bumpCtx, bumpCancel := context.WithTimeout(ctx, bumpBudget)
			if bumpErr := p.src.BumpAttempts(bumpCtx, failedIDs, ""); bumpErr != nil {
				p.metrics.ObservePublish(ctx, len(failedIDs), bumpErr)
				// Continue — the budget-bump failure must not
				// mask the DLQ eligibility decision.
			}
			bumpCancel()
		}
		// Post-commit cleanup (OBX-003): only events that reached a
		// terminal state (SENT or FAILED) have their in-memory
		// counters reset. Under-cap publish failures keep the row
		// PENDING, and its counter MUST survive for the next
		// poll's budget decision. On a rollback we must preserve
		// every counter so the next poll retries with the same
		// budget.
		if err == nil {
			for _, id := range terminalIDs {
				p.attempts.Delete(id)
				p.firstSeen.Delete(id)
			}
		}
		if err != nil && !errors.Is(err, errEmptyBatch) {
			p.metrics.ObservePoll(ctx, 0, time.Since(start), err)
			p.consecutiveFailures++
			if !p.sleep(ctx) {
				return nil
			}
			continue
		}
		p.consecutiveFailures = 0
		if !p.sleep(ctx) {
			return nil
		}
	}
}

// errEmptyBatch signals an empty fetch so the poller's outer loop
// can sleep without rolling back the tx.
var errEmptyBatch = errors.New("outbox: empty batch")

// errUnderCap signals a publish failure where every row in the batch
// is still under the DLQ thresholds (no MarkFailedTx call). The
// closure returns this to force a rollback so the row stays PENDING
// for the next poll. The Run loop treats it like any other rollback
// error for metrics purposes but does NOT increment consecutive
// failure counter (this is the under-cap recovery path, not an
// outage).
var errUnderCap = errors.New("outbox: publish failed, rows under cap")

// bumpBudget caps the time the autonomous BumpAttempts call will
// wait for a pool slot. The call runs inside the locked-tx closure,
// competing with the tx for a pool connection; if the pool is
// saturated we want the bump to fail fast rather than block the
// publish-failure path until the outer ctx times out.
const bumpBudget = 2 * time.Second

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

// sleep waits the computed backoff duration or until ctx is
// cancelled / stop is called. Returns false if the poller should
// exit. The backoff is min(cfg.Interval * 2^consecutiveFailures,
// cfg.MaxInterval) ± jitter (OBX-004). On a successful iteration
// the caller resets consecutiveFailures to 0 so the next sleep is
// just cfg.Interval.
func (p *Poller) sleep(ctx context.Context) bool {
	dur := p.computeBackoff()
	t := time.NewTimer(dur)
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

// computeBackoff returns the next inter-poll sleep duration:
// min(Interval * 2^consecutiveFailures, MaxInterval) ± jitter.
// JitterFraction=0 yields a deterministic duration (tests).
// JitterFraction=0.2 yields ±20% (production recommended).
func (p *Poller) computeBackoff() time.Duration {
	if p.consecutiveFailures <= 0 {
		return p.cfg.Interval
	}
	// Guard against overflow: cap the shift at 30 (≈ 1B × Interval).
	shift := p.consecutiveFailures
	if shift > 30 {
		shift = 30
	}
	base := time.Duration(int64(p.cfg.Interval) << uint(shift))
	if p.cfg.MaxInterval > 0 && base > p.cfg.MaxInterval {
		base = p.cfg.MaxInterval
	}
	if p.cfg.JitterFraction > 0 {
		p.rngMu.Lock()
		f := 1.0 + (p.rng.Float64()*2-1)*p.cfg.JitterFraction
		p.rngMu.Unlock()
		base = time.Duration(float64(base) * f)
	}
	return base
}

// handlePublishFailure bumps the per-event attempt counter and, when
// the row crosses both MaxAttempts AND MaxRetryAge, routes it to
// the DLQ and marks it FAILED so the next poll skips it. Rows
// still under either cap stay PENDING — they're re-fetched on the
// next poll because the outer RunInTx rolls back on the returned
// error.
//
// Failure-handling invariants (v1.1.5 E2E chain repair):
//
//   - DLQ.Send error (OBX-002): the DLQ write MUST succeed before
//     MarkFailedTx is called; otherwise the row is excluded from
//     future fetches AND not in the DLQ — silent data loss. On
//     error the row stays PENDING and the returned error triggers
//     a rollback so a subsequent DLQ success can retry.
//
//   - MarkFailedTx error (OBX-003): the row's terminal transition
//     MUST be durable. If the UPDATE fails inside the locked tx,
//     the tx rolls back, the row stays PENDING, and the in-memory
//     attempt counter is preserved (Delete is moved to the caller
//     in Run so it only runs on commit). The next poll retries
//     the same DLQ transition with the same budget — no
//     double-firing, no silent reset.
//
//   - DB attempts for under-cap failures (OBX-001): the per-row
//     `attempts` counter is incremented via an autonomous UPDATE
//     (Source.BumpAttempts) on every failure, not only at the
//     terminal transition. This makes the retry budget survive
//     pod restarts for rows still under MaxAttempts (the v1.1.4
//     claim was inert pre-fix because MarkFailedTx was the only
//     writer of `attempts`).
//
// Returns the list of events that reached a terminal FAILED state
// (caller resets their in-memory counters post-commit). A non-nil
// error indicates the tx should roll back so the row stays
// PENDING with its counter preserved.
type publishFailureResult struct {
	terminalIDs []string
	err         error
}

func (p *Poller) handlePublishFailure(ctx context.Context, tx pgx.Tx, recs []outbox.Record, cause error, dbAttempts map[string]int) publishFailureResult {
	var result publishFailureResult
	// OBX-001 (bump-on-failure) is implemented in Run(), NOT here:
	// Source.BumpAttempts is an autonomous (non-tx) UPDATE that must
	// commit independently of the rollback path. Calling it inside
	// the locked tx would contend with the tx for a pool slot,
	// risk a 5s+ wait when the outer ctx is short (the publish
	// failure path exhausts the ctx budget before MarkFailedTx
	// runs), and would also roll back with the tx — defeating
	// the purpose of a non-tx counter bump.
	//
	// Run() invokes BumpAttempts post-RunInTx with the captured
	// failedIDs, so the locked tx's pool connection is released
	// first. For under-cap failures (rows stay PENDING), the
	// UPDATE matches and increments `attempts`. For cross-cap
	// failures (MarkFailedTx transitioned status to FAILED), the
	// WHERE status='PENDING' guard skips the bump — which is
	// correct because the row is now terminal.
	for _, r := range recs {
		memCur := p.loadAttempts(r.EventID)
		dbCur := dbAttempts[r.EventID] // 0 when the row has no DB attempts yet
		cur := memCur
		if dbCur > cur {
			cur = dbCur
		}
		next := cur + 1
		p.storeAttempts(r.EventID, next)
		p.touchFirstSeen(r.EventID)

		if next < p.cfg.MaxAttempts {
			continue
		}
		if !p.exceedsRetryAge(r.EventID) {
			continue
		}
		if p.dlq == nil {
			// Without a DLQ we leave the row PENDING; it will keep
			// being re-attempted on every poll, which is the
			// pre-DLQ behavior. Operators see the lag grow.
			continue
		}
		// OBX-002: if DLQ.Send fails, do NOT mark the row FAILED.
		// Returning the error here rolls back the tx and leaves
		// the row PENDING; the next poll retries the DLQ write.
		if err := p.dlq.Send(ctx, r, cause.Error()); err != nil {
			result.err = fmt.Errorf("dlq send %s: %w", r.EventID, err)
			return result
		}
		p.metrics.ObserveDLQ(ctx, r, cause.Error())
		// OBX-003: propagate MarkFailedTx error so the tx rolls
		// back if the terminal transition itself fails. The
		// in-memory attempts Delete is done in Run() AFTER the
		// tx commits successfully (see post-commit cleanup).
		if err := p.src.MarkFailedTx(ctx, tx, []string{r.EventID}); err != nil {
			p.metrics.ObservePublish(ctx, len(recs), err)
			result.err = fmt.Errorf("mark failed %s: %w", r.EventID, err)
			return result
		}
		result.terminalIDs = append(result.terminalIDs, r.EventID)
	}
	return result
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

// touchFirstSeen records the first time the poller observed this
// event_id (used by OBX-004's MaxRetryAge cap). Returns the
// existing time if already recorded.
func (p *Poller) touchFirstSeen(id string) time.Time {
	if v, ok := p.firstSeen.Load(id); ok {
		return v.(time.Time)
	}
	now := time.Now()
	actual, _ := p.firstSeen.LoadOrStore(id, now)
	return actual.(time.Time)
}

// exceedsRetryAge returns true when the row's age (since first
// observed by this pod) has crossed the configured MaxRetryAge. The
// per-pod firstSeen map is wiped on restart; for cross-restart
// accuracy the row's created_at should be used. For now this is the
// per-pod clock used to bound a Kafka outage's visibility window.
func (p *Poller) exceedsRetryAge(id string) bool {
	if p.cfg.MaxRetryAge <= 0 {
		return true // disabled → fall through to attempt-only DLQ
	}
	first, ok := p.firstSeen.Load(id)
	if !ok {
		return true
	}
	return time.Since(first.(time.Time)) >= p.cfg.MaxRetryAge
}
