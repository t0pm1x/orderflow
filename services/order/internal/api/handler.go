package api

import (
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
}

// Repository is the data access interface for Orders.
// Insert must persist both the Order row and any outbox records
// within a single DB transaction. Implementations:
//   - 3.4.c mockRepo: stores the order in memory; ignores events
//     (preserved for unit tests of the handler).
//   - 3.4.d+ PGRepo:  tx-wraps INSERT INTO orders + Append on the
//     outbox writer, commits atomically.
type Repository interface {
	Insert(o *domain.Order, events ...outbox.Record) error
	Get(id types.OrderID) (*domain.Order, error)
	List(state domain.OrderState, limit int) ([]*domain.Order, error)
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
	return r
}

type submitRequest struct {
	CustomerID string             `json:"customer_id"`
	Items      []domain.OrderItem `json:"items"`
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

	rec, buildErr := buildOrderCreatedRecord(o)
	if buildErr != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "EVENT_BUILD_FAILED", buildErr.Error(), buildErr))
		return
	}

	if err := h.repo.Insert(o, rec); err != nil {
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

	o, err := h.repo.Get(id)
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
	items, err := h.repo.List(state, limit)
	if err != nil {
		apierrors.WriteError(w, apierrors.Wrap(http.StatusInternalServerError, "LIST_FAILED", err.Error(), err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"items":    items,
		"has_more": len(items) == limit,
	})
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
