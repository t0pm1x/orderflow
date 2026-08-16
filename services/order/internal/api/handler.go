package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	apierrors "github.com/t0pm1x/orderflow/platform/errors"
	"github.com/t0pm1x/orderflow/platform/types"

	"github.com/t0pm1x/orderflow/services/order/internal/domain"
)

// Repository is the data access interface for Orders.
// Implemented in 3.4.d (outbox); for now, in-memory mock for tests.
type Repository interface {
	Insert(o *domain.Order) error
	Get(id types.OrderID) (*domain.Order, error)
	List(state domain.OrderState, limit int) ([]*domain.Order, error)
}

type Handler struct {
	repo Repository
}

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
	if err := h.repo.Insert(o); err != nil {
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
