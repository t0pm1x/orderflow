package model

import (
	"testing"
	"time"
)

func TestStock_CanReserve(t *testing.T) {
	s := &Stock{SKU: "A", Available: 10, Reserved: 0, Version: 1}
	if !s.CanReserve(5) {
		t.Error("should be able to reserve 5")
	}
	if s.CanReserve(15) {
		t.Error("should not be able to reserve 15")
	}
}

func TestStock_Reserve(t *testing.T) {
	s := &Stock{SKU: "A", Available: 10, Reserved: 0, Version: 1}
	if err := s.Reserve(3); err != nil {
		t.Fatal(err)
	}
	if s.Available != 7 || s.Reserved != 3 {
		t.Errorf("expected 7/3, got %d/%d", s.Available, s.Reserved)
	}
	if s.Version != 2 {
		t.Errorf("expected version 2, got %d", s.Version)
	}
}

func TestStock_Reserve_Insufficient(t *testing.T) {
	s := &Stock{SKU: "A", Available: 2}
	err := s.Reserve(5)
	if err != ErrInsufficientStock {
		t.Errorf("expected ErrInsufficientStock, got %v", err)
	}
}

func TestStock_Release(t *testing.T) {
	s := &Stock{SKU: "A", Available: 7, Reserved: 3, Version: 1}
	s.Release(3)
	if s.Available != 10 || s.Reserved != 0 {
		t.Errorf("expected 10/0, got %d/%d", s.Available, s.Reserved)
	}
}

func TestReservation_Expired(t *testing.T) {
	r := &Reservation{ExpiresAt: time.Now().Add(-time.Hour)}
	if !r.Expired() {
		t.Error("should be expired")
	}
	r2 := &Reservation{ExpiresAt: time.Now().Add(time.Hour)}
	if r2.Expired() {
		t.Error("should not be expired")
	}
}
