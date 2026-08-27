package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/t0pm1x/orderflow/platform/outbox"
)

// fakeRepo is an in-memory webhook.Repository. No DB required: the
// handler's contract is "look up (or auto-create), classify, update +
// emit", all of which is observable through the map and the captured
// events.
type fakeRepo struct {
	payments  map[string]*Payment
	events    []outbox.Record
	upsertErr error
}

func newFakeRepo(payments ...*Payment) *fakeRepo {
	f := &fakeRepo{payments: make(map[string]*Payment, len(payments))}
	for _, p := range payments {
		f.payments[p.ID] = p
	}
	return f
}

// UpsertFromWebhook is the new entry point: creates the payments
// row from the webhook payload when missing (the saga's
// PaymentRequested consumer may not have run yet for a manual
// webhook fire) and transitions the status with the terminal-state
// guard. Returns (true, nil) on a real transition, (false, nil)
// when the row's current status is already terminal.
func (f *fakeRepo) UpsertFromWebhook(_ context.Context, paymentID, orderID string, amountCents int64, lastFour string, to PaymentStatus, events ...outbox.Record) (bool, error) {
	if f.upsertErr != nil {
		return false, f.upsertErr
	}
	p, ok := f.payments[paymentID]
	if !ok {
		// Auto-create from webhook payload (mock-provider semantics).
		p = &Payment{
			ID:          paymentID,
			OrderID:     orderID,
			AmountCents: amountCents,
			LastFour:    lastFour,
			Status:      "",
		}
		f.payments[paymentID] = p
	}
	if p.Status == StatusCaptured || p.Status == StatusFailed {
		// Already terminal — same-status replay and opposite-
		// terminal flip both short-circuit here. Mirrors the
		// production PGRepo's terminal-state guard.
		return false, nil
	}
	p.Status = to
	f.events = append(f.events, events...)
	return true, nil
}

const (
	testPaymentID = "2e4f8a1c-5b7d-4e9a-8c2f-1a6b3e7d9c4f"
	testOrderID   = "0fa1b8e2-7c14-4d39-9b1e-3f8c0a7b2d5e"
)

func testPayment() *Payment {
	return &Payment{
		ID:          testPaymentID,
		OrderID:     testOrderID,
		AmountCents: 3998,
		Status:      "",
	}
}

// post drives the request through Routes() so route registration is
// covered too, not just the handler func.
func post(t *testing.T, repo Repository, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/payments/webhook", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	NewHandler(repo, nil).Routes().ServeHTTP(rec, req)
	return rec
}

func decodeCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return body.Code
}

// TestWebhook_AutoCreatesRow_FromPayload is F-008 root-cause fix:
// pre-fix, the webhook returned 404 if no payments row existed. The
// SPA's "Force succeed/fail" buttons then failed with PAYMENT_NOT_FOUND
// whenever the saga's PaymentRequested consumer hadn't run yet.
// Now the handler auto-creates the row from the webhook payload
// (mock-provider semantics) and the webhook chain completes.
func TestWebhook_AutoCreatesRow_FromPayload(t *testing.T) {
	repo := newFakeRepo() // EMPTY — no payments row pre-existing
	const newID = "11111111-2222-3333-4444-555555555555"
	const newOrderID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	rec := post(t, repo, fmt.Sprintf(
		`{"payment_id":%q,"order_id":%q,"last_four":"4242","amount_cents":1999,"status":"success"}`,
		newID, newOrderID))

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}
	p, ok := repo.payments[newID]
	if !ok {
		t.Fatalf("payment row was NOT auto-created")
	}
	if p.Status != StatusCaptured {
		t.Errorf("auto-created row status: got %q want %q", p.Status, StatusCaptured)
	}
	if p.OrderID != newOrderID {
		t.Errorf("auto-created row order_id: got %q want %q", p.OrderID, newOrderID)
	}
	if len(repo.events) != 1 {
		t.Fatalf("events: got %d want 1", len(repo.events))
	}
	if repo.events[0].EventType != "PaymentCompleted" {
		t.Errorf("event type: got %q want PaymentCompleted", repo.events[0].EventType)
	}
}

func TestWebhook_Success_CapturesAndEmitsPaymentCompleted(t *testing.T) {
	repo := newFakeRepo(testPayment())

	rec := post(t, repo, fmt.Sprintf(
		`{"payment_id":%q,"order_id":%q,"last_four":"4242000000000000","amount_cents":3998,"status":"success"}`,
		testPaymentID, testOrderID))

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := repo.payments[testPaymentID].Status; got != StatusCaptured {
		t.Errorf("payment status: got %q want %q", got, StatusCaptured)
	}
	if len(repo.events) != 1 {
		t.Fatalf("events: got %d want 1", len(repo.events))
	}
	ev := repo.events[0]
	if ev.EventType != "PaymentCompleted" {
		t.Errorf("event type: got %q want PaymentCompleted", ev.EventType)
	}
	if ev.Topic != TopicPaymentEvents {
		t.Errorf("topic: got %q want %q", ev.Topic, TopicPaymentEvents)
	}
	if ev.AggregateID != testOrderID || ev.AggregateType != "Order" {
		t.Errorf("aggregate: got (%q, %q) want (%q, %q)", ev.AggregateID, ev.AggregateType, testOrderID, "Order")
	}
	if ev.EventID == "" || ev.SchemaVersion != SchemaVersion {
		t.Errorf("envelope: got (event_id %q, schema %q) want (non-empty, %q)", ev.EventID, ev.SchemaVersion, SchemaVersion)
	}
	var payload PaymentCompletedPayload
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.PaymentID != testPaymentID || payload.OrderID != testOrderID {
		t.Errorf("payload: got %+v want payment %q order %q", payload, testPaymentID, testOrderID)
	}
}

// "succeeded" is the spelling in api/openapi.yaml; both must work.
func TestWebhook_SucceededSpellingAccepted(t *testing.T) {
	repo := newFakeRepo(testPayment())
	rec := post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"succeeded"}`, testPaymentID))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := repo.payments[testPaymentID].Status; got != StatusCaptured {
		t.Errorf("payment status: got %q want %q", got, StatusCaptured)
	}
}

func TestWebhook_Failed_MapsLastFourToErrorCode(t *testing.T) {
	tests := []struct {
		lastFour string
		want     string
	}{
		{"4242000000000001", "card_declined"},
		{"4242000000000002", "insufficient_funds"},
		{"4242000000009999", "network_error"},
		{"", "network_error"},
	}
	for _, tc := range tests {
		t.Run(tc.want+"/"+tc.lastFour, func(t *testing.T) {
			repo := newFakeRepo(testPayment())
			rec := post(t, repo, fmt.Sprintf(
				`{"payment_id":%q,"last_four":%q,"status":"failed"}`, testPaymentID, tc.lastFour))

			if rec.Code != http.StatusOK {
				t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
			}
			if got := repo.payments[testPaymentID].Status; got != StatusFailed {
				t.Errorf("payment status: got %q want %q", got, StatusFailed)
			}
			if len(repo.events) != 1 {
				t.Fatalf("events: got %d want 1", len(repo.events))
			}
			if repo.events[0].EventType != "PaymentFailed" {
				t.Errorf("event type: got %q want PaymentFailed", repo.events[0].EventType)
			}
			var payload PaymentFailedPayload
			if err := json.Unmarshal(repo.events[0].Payload, &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if payload.ErrorCode != tc.want {
				t.Errorf("error_code: got %q want %q", payload.ErrorCode, tc.want)
			}
		})
	}
}

func TestWebhook_ExplicitErrorCodeWins(t *testing.T) {
	repo := newFakeRepo(testPayment())
	rec := post(t, repo, fmt.Sprintf(
		`{"payment_id":%q,"last_four":"4242000000000001","status":"failed","error_code":"provider_timeout"}`,
		testPaymentID))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var payload PaymentFailedPayload
	if err := json.Unmarshal(repo.events[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.ErrorCode != "provider_timeout" {
		t.Errorf("error_code: got %q want provider_timeout", payload.ErrorCode)
	}
}

func TestWebhook_InvalidStatus_400(t *testing.T) {
	repo := newFakeRepo(testPayment())
	rec := post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"pending"}`, testPaymentID))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if code := decodeCode(t, rec); code != "INVALID_STATUS" {
		t.Errorf("error code: got %q want INVALID_STATUS", code)
	}
	if len(repo.events) != 0 {
		t.Errorf("events: got %d want 0 on rejected status", len(repo.events))
	}
}

func TestWebhook_MissingPaymentID_400(t *testing.T) {
	rec := post(t, newFakeRepo(testPayment()), `{"status":"success"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if code := decodeCode(t, rec); code != "VALIDATION" {
		t.Errorf("error code: got %q want VALIDATION", code)
	}
}

func TestWebhook_MalformedJSON_400(t *testing.T) {
	rec := post(t, newFakeRepo(testPayment()), `{"payment_id":`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if code := decodeCode(t, rec); code != "INVALID_PAYLOAD" {
		t.Errorf("error code: got %q want INVALID_PAYLOAD", code)
	}
}

// A transient repo failure must surface as 5xx so the provider (and
// the idempotency middleware, which releases the key on >=500) retries.
func TestWebhook_UpsertError_500(t *testing.T) {
	repo := newFakeRepo(testPayment())
	repo.upsertErr = fmt.Errorf("deadlock detected")
	rec := post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"success"}`, testPaymentID))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d want 500 (body %q)", rec.Code, rec.Body.String())
	}
	if code := decodeCode(t, rec); code != "DB_ERROR" {
		t.Errorf("error code: got %q want DB_ERROR", code)
	}
	if got := repo.payments[testPaymentID].Status; got == StatusCaptured {
		t.Error("payment must not be captured when UpsertFromWebhook fails")
	}
}

func TestRoutes_UnknownPath_404(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/payments/nope", nil)
	rec := httptest.NewRecorder()
	NewHandler(newFakeRepo(), nil).Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status: got %d want 404", rec.Code)
	}
}

// TestWebhook_TerminalGuard_LateFailedAfterCaptured is the
// regression guard for P1-#1 from the v1.1.1 audit: a payment
// already in StatusCaptured must NOT flip to StatusFailed on a
// late-delivered webhook. Pre-fix, the handler unconditionally
// UPDATEd status — letting providers' "failed" retries overwrite
// a confirmed successful-c payment.
func TestWebhook_TerminalGuard_LateFailedAfterCaptured(t *testing.T) {
	repo := newFakeRepo(&Payment{
		ID:          testPaymentID,
		OrderID:     testOrderID,
		AmountCents: 1000,
		Status:      StatusCaptured,
	})

	rec := post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"failed","error_code":"card_declined"}`, testPaymentID))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}

	if got := repo.payments[testPaymentID].Status; got != StatusCaptured {
		t.Errorf("status: got %q want %q (terminal-state guard must reject opposite-terminal flip)", got, StatusCaptured)
	}
	if n := len(repo.events); n != 0 {
		t.Errorf("outbox events: got %d want 0 (no PaymentFailed should be emitted)", n)
	}
}

// TestWebhook_TerminalGuard_LateCapturedAfterFailed: symmetric
// case — payment in StatusFailed, late webhook says succeeded.
// Must NOT flip.
func TestWebhook_TerminalGuard_LateCapturedAfterFailed(t *testing.T) {
	repo := newFakeRepo(&Payment{
		ID:          testPaymentID,
		OrderID:     testOrderID,
		AmountCents: 1000,
		Status:      StatusFailed,
	})

	rec := post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"succeeded"}`, testPaymentID))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", rec.Code)
	}

	if got := repo.payments[testPaymentID].Status; got != StatusFailed {
		t.Errorf("status: got %q want %q (terminal-state guard must reject opposite-terminal flip)", got, StatusFailed)
	}
	if n := len(repo.events); n != 0 {
		t.Errorf("outbox events: got %d want 0", n)
	}
}

// TestWebhook_TerminalGuard_SameStatusReplay: same-status replay
// must NOT emit a duplicate PaymentCompleted / PaymentFailed.
// Idempotent no-op. The first webhook transitions; the second
// is a same-status replay and must not double-emit.
func TestWebhook_TerminalGuard_SameStatusReplay(t *testing.T) {
	repo := newFakeRepo(testPayment())

	// Two identical webhooks — the first transitions; the second
	// is a no-op because the row is already terminal.
	post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"succeeded"}`, testPaymentID))
	if n := len(repo.events); n != 1 {
		t.Fatalf("after first: events = %d want 1", n)
	}
	post(t, repo, fmt.Sprintf(`{"payment_id":%q,"status":"succeeded"}`, testPaymentID))
	if n := len(repo.events); n != 1 {
		t.Errorf("after replay: events = %d want 1 (idempotent no-op must not double-emit)", n)
	}
}
