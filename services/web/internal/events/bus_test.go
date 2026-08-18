package events_test

import "testing"
import "github.com/t0pm1x/orderflow/services/web/internal/events"

func TestBus_StubConstruct(t *testing.T) {
	b := events.NewBus()
	if b == nil { t.Fatal("nil bus") }
}
