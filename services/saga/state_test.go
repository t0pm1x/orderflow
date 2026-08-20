package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestNewSaga_StateInitiated pins the initial state.
func TestNewSaga_StateInitiated(t *testing.T) {
	s := New("o1")
	if s.State != StateInitiated {
		t.Errorf("initial state: got %q want %q", s.State, StateInitiated)
	}
	if s.OrderID != "o1" {
		t.Errorf("OrderID: got %q want o1", s.OrderID)
	}
}

func TestSaga_HappyPath_StockReservedThenPaymentCompleted(t *testing.T) {
	s := New("o1")
	if next, err := s.Handle("StockReserved"); err != nil || next != StateStockReserved {
		t.Fatalf("StockReserved: got (%q, %v) want (%q, nil)", next, err, StateStockReserved)
	}
	if next, err := s.Handle("PaymentCompleted"); err != nil || next != StateCompleted {
		t.Fatalf("PaymentCompleted: got (%q, %v) want (%q, nil)", next, err, StateCompleted)
	}
	if !s.State.IsTerminal() {
		t.Error("Completed must be terminal")
	}
}

func TestSaga_PaymentFailedCompensates(t *testing.T) {
	s := New("o1")
	_, _ = s.Handle("StockReserved")
	if next, err := s.Handle("PaymentFailed"); err != nil || next != StateCompensated {
		t.Fatalf("PaymentFailed: got (%q, %v) want (%q, nil)", next, err, StateCompensated)
	}
}

func TestSaga_StockReservationFailedCompensates(t *testing.T) {
	s := New("o1")
	if next, err := s.Handle("StockReservationFailed"); err != nil || next != StateCompensated {
		t.Fatalf("StockReservationFailed: got (%q, %v) want (%q, nil)", next, err, StateCompensated)
	}
}

func TestSaga_InvalidTransitionRejected(t *testing.T) {
	s := New("o1")
	if _, err := s.Handle("PaymentCompleted"); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestSaga_TerminalRejectsFurtherEvents(t *testing.T) {
	s := New("o1")
	_, _ = s.Handle("StockReserved")
	_, _ = s.Handle("PaymentCompleted") // → completed
	if _, err := s.Handle("StockReserved"); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition from terminal, got %v", err)
	}
}

func TestSaga_CanTransition(t *testing.T) {
	s := New("o1")
	if !s.CanTransition("StockReserved") {
		t.Error("initiated must accept StockReserved")
	}
	if s.CanTransition("PaymentCompleted") {
		t.Error("initiated must NOT accept PaymentCompleted")
	}
}

func TestCompensate_AllRunAndSetTerminal(t *testing.T) {
	s := &Saga{OrderID: "o1", State: StateStockReserved}
	calls := 0
	cs := []Compensator{
		func(_ context.Context, _ *Saga) error { calls++; return nil },
		func(_ context.Context, _ *Saga) error { calls++; return errors.New("boom") },
		func(_ context.Context, _ *Saga) error { calls++; return nil },
	}
	err := Compensate(context.Background(), s, cs)
	if err == nil || err.Error() != "boom" {
		t.Errorf("Compensate error: got %v want 'boom'", err)
	}
	if calls != 3 {
		t.Errorf("compensator calls: got %d want 3", calls)
	}
	if s.State != StateCompensated {
		t.Errorf("state after Compensate: got %q want %q", s.State, StateCompensated)
	}
}

func TestCompensate_Idempotent(t *testing.T) {
	s := &Saga{OrderID: "o1", State: StateCompensated}
	err := Compensate(context.Background(), s, nil)
	if !errors.Is(err, ErrAlreadyCompensated) {
		t.Errorf("idempotent Compensate: got %v want ErrAlreadyCompensated", err)
	}
}

func TestCompensators_NilGuards(t *testing.T) {
	if err := ReleaseStockCompensator(nil)(nil, &Saga{OrderID: "x"}); err == nil {
		t.Error("ReleaseStockCompensator(nil) must return error")
	}
	if err := RefundPaymentCompensator(nil)(nil, &Saga{OrderID: "x"}); err == nil {
		t.Error("RefundPaymentCompensator(nil) must return error")
	}
}

func TestWatchdog_RegisterDeregisterExpire(t *testing.T) {
	w := NewWatchdog(50 * time.Millisecond)
	var mu sync.Mutex
	var expired []string
	done := make(chan struct{})
	go func() {
		w.Run(context.Background(), func(id string) {
			mu.Lock()
			expired = append(expired, id)
			mu.Unlock()
		})
		close(done)
	}()
	w.Register("o1")
	time.Sleep(120 * time.Millisecond)
	mu.Lock()
	got := append([]string(nil), expired...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "o1" {
		t.Errorf("expired: got %v want [o1]", got)
	}
	w.Stop()
	<-done
}

// TestWatchdog_StopIsIdempotent is the reviewer-found regression
// net for the double-close panic in Watchdog.Stop. Pre-fix the
// channel was closed unconditionally; a second Stop() call from a
// deferred shutdown path panicked with "close of closed channel".
// The fix wraps the close in sync.Once; calling Stop N times from
// any number of goroutines must be safe.
func TestWatchdog_StopIsIdempotent(t *testing.T) {
	w := NewWatchdog(time.Second)
	done := make(chan struct{})
	go func() {
		w.Run(context.Background(), func(string) {})
		close(done)
	}()
	// Fire Stop from 8 goroutines concurrently + 2 sequential calls.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Stop()
		}()
	}
	w.Stop()
	w.Stop()
	wg.Wait()
	<-done
}
