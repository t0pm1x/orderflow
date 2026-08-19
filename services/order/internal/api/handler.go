package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	apierrors "github.com/t0pm1x/orderflow/platform/errors"
	"github.com/t0pm1x/orderflow/platform/outbox"
	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// TopicOrderEvents is the Kafka topic that all Order Service events
// are published to. Centralized here so producer (3.4.d) and consumer
// (3.8) agree.
const TopicOrderEvents = "order-events"

// OrderCreatedPayload is the body of an OrderCreated event.
// Matches docs/superpowers/specs/orderflow-events.md.
type OrderCreatedPayload struct {
	OrderID    string             `json:"order_id"`
	CustomerID string             `json:"customer_id"`
	Items      []domain.OrderItem `json:"items"`
	TotalCents int64              `json:"total_cents"`
	State      string             `json:"state"`
	// LastFour is the opaque last-four-digits string the client
	// passed on the submit request (POST /v1/orders). The saga
	// forwards it on the downstream PaymentRequested event so the
	// payment provider can pick a success/decline branch on
	// determinism rather than orderID-derived coincidence. Empty
	// when the submit body did not include a payment block (the
	// historical v1.x behavior — keep working without forcing
	// callers to send payment info).
	LastFour string `json:"last_four,omitempty"`
}

// Repository is the data access interface for Orders.
// Insert must persist both the Order row and any outbox records
// within a single DB transaction. Implementations:
//   - 3.4.c mockRepo: stores the order in memory; ignores events
//     (preserved for unit tests of the handler).
//   - 3.4.d+ PGRepo:  tx-wraps INSERT INTO orders + Append on the
//     outbox writer, commits atomically.
//
// All methods take a context so the HTTP request deadline reaches
// the DB driver — without it, a client that cancels mid-write
// would still see the order appear in the database.
//
// Cancel performs a state transition to StateCancelled (terminal),
// emits an OrderCancelled outbox event atomically with the state
// update, and returns errNotFound when the order is unknown or
// already terminal (confirmed/cancelled/failed).
type Repository interface {
	Insert(ctx context.Context, o *domain.Order, events ...outbox.Record) error
	Get(ctx context.Context, id types.OrderID) (*domain.Order, error)
	List(ctx context.Context, state domain.OrderState, limit int) ([]*domain.Order, error)
	Cancel(ctx context.Context, id types.OrderID) error
}

// Handler serves the Order Service REST routes. It validates input,
// calls the Repository to insert orders (tx-wrapped with outbox
// writes via Repository.Insert) and reads them back.
type Handler struct {
	repo Repository
}

// NewHandler constructs a Handler backed by the given Repository.
func NewHandler(repo Repository) *Handler {
	return &Handler{repo: repo}
}

// Routes returns the chi router for the Order Service.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Post("/v1/orders", h.submit)
	r.Get("/v1/orders/{id}", h.get)
	r.Get("/v1/orders", h.list)
	r.Delete("/v1/orders/{id}", h.cancel)
	return r
}

type submitRequest struct {
	CustomerID string             `json:"customer_id"`
	Items      []domain.OrderItem `json:"items"`
	// Payment is the client-supplied payment hint. Today only
	// LastFour is forwarded — it lets the mock payment provider
	// (services/payment/internal/provider/provider.go) pick a
	// deterministic success/decline branch on cards ending in
	// 0001, instead of the orderID-derived coincidence the v1.x
	// handlers relied on. Optional; an empty Payment block is a
	// no-op so existing callers keep working.
	Payment *submitRequestPayment `json:"payment,omitempty"`
}

// submitRequestPayment is the slice of the submit body that is
// relayed onto the OrderCreated event. Modeled as its own struct
// (rather than inlining fields) so a future addition like
// "stripe_token" can land without churning submitRequest's JSON
// shape or its existing callers' omitempty behavior.
type submitRequestPayment struct {
	LastFour string `json:"last_four"`
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierrors.WriteError(w, apierrors.ErrInvalidPayload)
		return
	}
	if req.CustomerID == "" || len(req.Items) == 0 {
		apierrors.WriteError(w, &apierrors.APIError{
			Status:  http.StatusBadRequest,
			Code:    "VALIDATION",
			Message: "customer_id and items required",
		})
		return
	}

	custID, err := parseCustomerID(req.CustomerID)
	if err != nil {
		apierrors.WriteError(w, err)
		return
	}

	o := domain.NewOrder(custID, req.Items)
	if req.Payment != nil {
		o.LastFour = req.Payment.LastFour
	}

	rec, buildErr := buildOrderCreatedRecord(o)
	if buildErr != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "EVENT_BUILD_FAILED", buildErr.Error(), buildErr))
		return
	}

	if err := h.repo.Insert(r.Context(), o, rec); err != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "INSERT_FAILED", err.Error(), err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(o)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, parseErr := parseOrderID(idStr)
	if parseErr != nil {
		apierrors.WriteError(w, parseErr)
		return
	}

	o, err := h.repo.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, errNotFound) {
			apierrors.WriteError(w, apierrors.ErrNotFound)
			return
		}
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "GET_FAILED", err.Error(), err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(o)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	state := domain.OrderState(r.URL.Query().Get("state"))
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	items, err := h.repo.List(r.Context(), state, limit)
	if err != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "LIST_FAILED", err.Error(), err))
		return
	}
	next := ""
	if len(items) == limit {
		next = items[len(items)-1].ID.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Items      []*domain.Order `json:"items"`
		NextCursor string          `json:"next_cursor,omitempty"`
	}{Items: items, NextCursor: next})
}

// cancel handles DELETE /v1/orders/{id}. Idempotent: a missing or
// already-terminal order returns 404 (so callers can distinguish
// "I don't know this id" from "cancelled"), and a successful cancel
// returns 204. The Repository writes the state transition and the
// OrderCancelled outbox row in a single transaction so a downstream
// consumer (inventory) is guaranteed to see the same event.
func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, parseErr := parseOrderID(idStr)
	if parseErr != nil {
		apierrors.WriteError(w, parseErr)
		return
	}
	if err := h.repo.Cancel(r.Context(), id); err != nil {
		if errors.Is(err, errNotFound) {
			apierrors.WriteError(w, apierrors.ErrNotFound)
			return
		}
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "CANCEL_FAILED", err.Error(), err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// buildOrderCreatedRecord constructs an outbox.Record for an
// OrderCreated event from a freshly built Order. The record is what
// the poller (3.7) will read and publish to Kafka.
func buildOrderCreatedRecord(o *domain.Order) (outbox.Record, error) {
	payload := OrderCreatedPayload{
		OrderID:    o.ID.String(),
		CustomerID: o.CustomerID.String(),
		Items:      o.Items,
		TotalCents: int64(o.TotalCents),
		State:      string(o.State),
		LastFour:   o.LastFour,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return outbox.Record{}, err
	}
	return outbox.Record{
		EventID:       uuid.NewString(),
		EventType:     "OrderCreated",
		AggregateID:   o.ID.String(),
		AggregateType: "Order",
		SchemaVersion: "1.0",
		Topic:         TopicOrderEvents,
		Payload:       body,
	}, nil
}

var errNotFound = errors.New("not found")

func parseOrderID(s string) (types.OrderID, *apierrors.APIError) {
	u, err := uuid.Parse(s)
	if err != nil {
		return types.OrderID{}, &apierrors.APIError{
			Status:  http.StatusBadRequest,
			Code:    "INVALID_ID",
			Message: "invalid order id",
		}
	}
	return types.OrderID(u), nil
}

func parseCustomerID(s string) (types.CustomerID, *apierrors.APIError) {
	u, err := uuid.Parse(s)
	if err != nil {
		return types.CustomerID{}, &apierrors.APIError{
			Status:  http.StatusBadRequest,
			Code:    "INVALID_ID",
			Message: "invalid customer id",
		}
	}
	return types.CustomerID(u), nil
}
