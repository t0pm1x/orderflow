package provider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCharge_Success(t *testing.T) {
	r, err := Charge(context.Background(), "p1", 1000, "1234")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "succeeded" {
		t.Errorf("expected succeeded, got %s", r.Status)
	}
}

func TestCharge_Declined(t *testing.T) {
	r, err := Charge(context.Background(), "p1", 1000, "0001")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "failed" || r.ErrorCode != "card_declined" {
		t.Errorf("expected failed/card_declined, got %s/%s", r.Status, r.ErrorCode)
	}
}

func TestCharge_InsufficientFunds(t *testing.T) {
	r, _ := Charge(context.Background(), "p1", 1000, "0002")
	if r.ErrorCode != "insufficient_funds" {
		t.Errorf("expected insufficient_funds, got %s", r.ErrorCode)
	}
}

func TestCharge_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := Charge(ctx, "p1", 1000, "0003")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestCharge_InvalidCard(t *testing.T) {
	_, err := Charge(context.Background(), "p1", 1000, "12")
	if err == nil {
		t.Error("expected error for short card")
	}
}

func TestRefund_AlwaysSucceeds(t *testing.T) {
	r, err := Refund(context.Background(), "p1", 1000)
	if err != nil || r.Status != "succeeded" {
		t.Errorf("refund should always succeed, got %v %v", r, err)
	}
}
