package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// Config tunes the Poller.
type Config struct {
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
func (c Config) applyDefaults() Config {
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
	cfg       Config
	src       Source
	pub       Publisher
	dlq       DLQ
	metrics   Metrics
	attempts  sync.Map // event_id -> int (atomic-stored via *int32)
	stopCh    chan struct{}
	stopped   atomic.Bool
	runningCh chan struct{}
}

// New constructs a Poller. dlq may be nil (then MaxAttempts>0 is
// ignored — failed rows stay PENDING forever). metrics may be nil
// and defaults to NoopMetrics.
func New(cfg Config, src Source, pub Publisher, dlq DLQ, metrics Metrics) *Poller {
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
		recs, err := p.src.FetchPending(ctx, p.cfg.BatchSize)
		p.metrics.ObservePoll(ctx, len(recs), time.Since(start), err)
		if err != nil {
			// Transient error: back off and retry.
			if !p.sleep(ctx) {
				return nil
			}
			continue
		}
		if len(recs) == 0 {
			if !p.sleep(ctx) {
				return nil
			}
			continue
		}

		if err := p.pub.Publish(ctx, recs); err != nil {
			p.metrics.ObservePublish(ctx, len(recs), err)
			p.handlePublishFailure(ctx, recs, err)
			if !p.sleep(ctx) {
				return nil
			}
			continue
		}
		p.metrics.ObservePublish(ctx, len(recs), nil)

		ids := make([]string, len(recs))
		for i, r := range recs {
			ids[i] = r.EventID
		}
		if err := p.src.MarkSent(ctx, ids); err != nil {
			// MarkSent failure: rows stay PENDING and will be
			// re-published on the next poll. At-least-once.
			p.metrics.ObservePublish(ctx, len(recs), err)
		}
		for _, r := range recs {
			p.attempts.Delete(r.EventID)
		}
	}
}

// resetAttemptsForTest is exposed for tests; in production it's a
// no-op via defer.
func (p *Poller) resetAttemptsForTest() {}

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
// MaxAttempts exceeded, routes the row to the DLQ. Rows still under
// the cap stay PENDING.
func (p *Poller) handlePublishFailure(ctx context.Context, recs []outbox.Record, cause error) {
	for _, r := range recs {
		cur := p.loadAttempts(r.EventID)
		next := cur + 1
		p.storeAttempts(r.EventID, next)
		if next >= p.cfg.MaxAttempts {
			if p.dlq != nil {
				_ = p.dlq.Send(ctx, r, cause.Error())
				p.metrics.ObserveDLQ(ctx, r, cause.Error())
				_ = p.src.MarkFailed(ctx, []string{r.EventID})
				p.attempts.Delete(r.EventID)
			}
			// Without a DLQ we leave the row PENDING; it will keep
			// being re-attempted on every poll, which is the
			// pre-DLQ behavior. Operators see the lag grow.
		}
	}
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
