package types

import (
	"testing"
)

func TestMoney_Cents(t *testing.T) {
	m := NewMoneyFromCents(1999)
	if m.Cents() != 1999 {
		t.Errorf("expected 1999 cents, got %d", m.Cents())
	}
}

func TestMoney_String(t *testing.T) {
	cases := []struct {
		m    Money
		want string
	}{
		{NewMoneyFromCents(199), "$1.99"},
		{NewMoneyFromCents(100), "$1.00"},
		{NewMoneyFromCents(0), "$0.00"},
	}
	for _, c := range cases {
		if got := c.m.String(); got != c.want {
			t.Errorf("%d cents: got %q, want %q", c.m, got, c.want)
		}
	}
}

func TestMoney_FromMajor(t *testing.T) {
	m := NewMoneyFromMajor(19.99)
	if m.Cents() != 1999 {
		t.Errorf("expected 1999 cents, got %d", m.Cents())
	}
}
