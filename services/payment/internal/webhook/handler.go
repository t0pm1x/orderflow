package webhook

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	apierrors "github.com/t0pm1x/orderflow/platform/errors"
	"github.com/t0pm1x/orderflow/platform/outbox"

	"github.com/t0pm1x/orderflow/services/payment/internal/idempotency"
)

// TopicPaymentEvents is the Kafka topic every Payment Service event is
// published to. Centralized here so the webhook (3.5.g) and the order /
// saga consumers (3.8) agree on one string.
const TopicPaymentEvents = "payment-events"

// SchemaVersion is the envelope schema version of the events emitted
// by this handler (docs/superpowers/specs/orderflow-events.md).
const SchemaVersion = "1.0"

// ErrPaymentNotFound is what a Repository returns (wrapped) when the
// payment_id in the webhook body has no row. The handler maps it to a
// 404 so a provider replay for an unknown payment is not retried as a
// 5xx forever.
var ErrPaymentNotFound = errors.New("payment not found")

// PaymentStatus is the lifecycle state of a payments row.
type PaymentStatus string

const (
	// StatusCaptured is the payments row state when the provider confirmed the charge.
	StatusCaptured PaymentStatus = "captured"
	// StatusFailed is the payments row state when the provider declined or errored.
	StatusFailed PaymentStatus = "failed"
)

// Payment is the subset of the payments row the webhook needs: enough
// to build the outbox event (order id is the saga's aggregate id) and
// to report the resulting state.
type Payment struct {
	ID          string        `json:"id"`
	OrderID     string        `json:"order_id"`
	AmountCents int64         `json:"amount_cents"`
	Status      PaymentStatus `json:"status"`
}

// Repository is the payment data access seam.
//
// UpdateStatus must write the payments row and every outbox Record in
// a single DB transaction: either the new status and the event commit
// together, or neither does. Implementations:
//   - repository.PGRepo — pgxpool + pgx.BeginFunc (production).
//   - fakeRepo in handler_test.go — in-memory (handler unit tests).
type Repository interface {
	Get(id string) (*Payment, error)
	UpdateStatus(id string, status PaymentStatus, events ...outbox.Record) error
	// UpdateStatusFromNonTerminal transitions the payment to `to`
	// only if its current status is non-terminal (i.e. NOT in
	// {captured, failed}). Returns (true, nil) on transition with
	// outbox rows emitted, (false, nil) when the current status is
	// already terminal (no-op, no outbox emission), and (false,
	// ErrPaymentNotFound) when the row vanished.
	//
	// The handler uses this to enforce terminal-state guards: a
	// late webhook that flips a captured payment to failed (or
	// vice versa) is silently dropped, and a same-status replay
	// is also a no-op so we don't emit duplicate PaymentCompleted
	// / PaymentFailed events.
	UpdateStatusFromNonTerminal(id string, to PaymentStatus, events ...outbox.Record) (bool, error)
}

// PaymentCompletedPayload is the body of a PaymentCompleted event.
type PaymentCompletedPayload struct {
	PaymentID string `json:"payment_id"`
	OrderID   string `json:"order_id"`
}

// PaymentFailedPayload is the body of a PaymentFailed event.
type PaymentFailedPayload struct {
	PaymentID string `json:"payment_id"`
	OrderID   string `json:"order_id"`
	ErrorCode string `json:"error_code"`
}

// webhookRequest is the provider callback body. Status accepts both
// "succeeded" (api/openapi.yaml PaymentWebhook) and "success" (what
// the mock provider posts) so either spelling works. ErrorCode is
// optional: when absent it is derived from LastFour using the same
// table as internal/provider.Charge.
type webhookRequest struct {
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	LastFour    string `json:"last_four"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
	ErrorCode   string `json:"error_code"`
}

// Handler serves the provider callback endpoint.
type Handler struct {
	repo   Repository
	idem   *idempotency.Store
	logger *slog.Logger
}

// NewHandler constructs a Handler. idemStore may be nil (no Redis
// configured): the route is then served without the idempotency
// middleware, which is safe because the saga drives state from outbox
// events rather than from the HTTP response.
func NewHandler(repo Repository, idemStore *idempotency.Store) *Handler {
	return NewHandlerWithLogger(repo, idemStore, slog.Default())
}

// NewHandlerWithLogger constructs a Handler with an explicit logger.
// Used by tests to capture log output.
func NewHandlerWithLogger(repo Repository, idemStore *idempotency.Store, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{repo: repo, idem: idemStore, logger: logger}
}

// Routes returns the chi router for the Payment Service.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	if h.idem != nil {
		r.With(idempotency.Middleware(h.idem)).Post("/v1/payments/webhook", h.webhook)
		return r
	}
	r.Post("/v1/payments/webhook", h.webhook)
	return r
}

func (h *Handler) webhook(w http.ResponseWriter, r *http.Request) {
	var req webhookRequest
	// Cap the request body at 64 KiB. Webhook payloads are JSON
	// with a handful of fields (payment_id, status, last_four,
	// error_code); 64 KiB is generous and prevents OOM via
	// giant-body DoS.
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierrors.WriteError(w, apierrors.ErrInvalidPayload)
		return
	}
	if req.PaymentID == "" {
		apierrors.WriteError(w, &apierrors.APIError{
			Status:  http.StatusBadRequest,
			Code:    "VALIDATION",
			Message: "payment_id required",
		})
		return
	}

	newStatus, eventType, ok := classify(req.Status)
	if !ok {
		apierrors.WriteError(w, &apierrors.APIError{
			Status:  http.StatusBadRequest,
			Code:    "INVALID_STATUS",
			Message: "status must be succeeded|success|failed",
		})
		return
	}

	p, err := h.repo.Get(req.PaymentID)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			apierrors.WriteError(w, &apierrors.APIError{
				Status:  http.StatusNotFound,
				Code:    "PAYMENT_NOT_FOUND",
				Message: err.Error(),
				Cause:   err,
			})
			return
		}
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "GET_FAILED", err.Error(), err))
		return
	}

	ev, buildErr := buildRecord(p, eventType, errorCode(req))
	if buildErr != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "EVENT_BUILD_FAILED", buildErr.Error(), buildErr))
		return
	}

	// Terminal-state guard: a payment already in a terminal
	// status (captured or failed) must NOT be flipped to the
	// other terminal by a late-delivered webhook. The repo
	// transition only fires when the current status is non-terminal,
	// so a same-status replay also no-ops (the row's status
	// already matches). Either way, no outbox row is emitted
	// for the no-op path.
	advanced, err := h.repo.UpdateStatusFromNonTerminal(p.ID, newStatus, ev)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			apierrors.WriteError(w, &apierrors.APIError{
				Status:  http.StatusNotFound,
				Code:    "PAYMENT_NOT_FOUND",
				Message: err.Error(),
				Cause:   err,
			})
			return
		}
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "DB_ERROR", err.Error(), err))
		return
	}
	if !advanced {
		// Idempotent replay: payment is already in a terminal state
		// different from the requested one (or the row's status
		// doesn't match — but allowedFrom covers both terminals, so
		// this branch is a true terminal-mismatch). Return 200 so the
		// provider doesn't retry forever; log for observability.
		h.logger.Info("webhook: payment already in terminal state, ignoring late update",
			"payment_id", p.ID, "current_status", p.Status, "requested_status", newStatus)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":     "ok",
		"payment_id": p.ID,
		"state":      string(newStatus),
	})
}

// classify maps the provider status string onto the payments row state
// and the event type to emit. ok is false for anything unrecognized.
func classify(status string) (newStatus PaymentStatus, eventType string, ok bool) {
	switch status {
	case "succeeded", "success":
		return StatusCaptured, "PaymentCompleted", true
	case "failed":
		return StatusFailed, "PaymentFailed", true
	default:
		return "", "", false
	}
}

// errorCode returns the failure reason for a PaymentFailed payload.
// An explicit error_code in the body wins; otherwise the card's last 4
// digits are mapped with the same table internal/provider.Charge uses
// so the mock's decline reasons survive the round trip.
func errorCode(req webhookRequest) string {
	if req.ErrorCode != "" {
		return req.ErrorCode
	}
	if len(req.LastFour) < 4 {
		return "network_error"
	}
	switch req.LastFour[len(req.LastFour)-4:] {
	case "0001":
		return "card_declined"
	case "0002":
		return "insufficient_funds"
	default:
		return "network_error"
	}
}

// buildRecord constructs the outbox Record for eventType. The
// aggregate is the Order (not the Payment): the saga and the Order
// Service key their state off order_id.
func buildRecord(p *Payment, eventType, reason string) (outbox.Record, error) {
	var (
		body []byte
		err  error
	)
	if eventType == "PaymentFailed" {
		body, err = json.Marshal(PaymentFailedPayload{
			PaymentID: p.ID,
			OrderID:   p.OrderID,
			ErrorCode: reason,
		})
	} else {
		body, err = json.Marshal(PaymentCompletedPayload{
			PaymentID: p.ID,
			OrderID:   p.OrderID,
		})
	}
	if err != nil {
		return outbox.Record{}, err
	}
	return outbox.Record{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		AggregateID:   p.OrderID,
		AggregateType: "Order",
		SchemaVersion: SchemaVersion,
		Topic:         TopicPaymentEvents,
		Payload:       body,
	}, nil
}
