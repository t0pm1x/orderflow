package backend

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// OrderClient talks to the Order Service.
type OrderClient interface {
	List(ctx context.Context, state OrderState, limit int) (*OrderList, error)
	Get(ctx context.Context, id string) (*Order, error)
	Submit(ctx context.Context, in OrderSubmit) (*Order, error)
	Cancel(ctx context.Context, id string) error
}

// PaymentClient talks to the Payment Service.
type PaymentClient interface {
	FireWebhook(ctx context.Context, w PaymentWebhook) error
}

// InventoryClient talks to the Inventory Service.
type InventoryClient interface {
	ListStock(ctx context.Context) ([]StockItem, error)
}

// HTTPClient implements all three clients against the configured
// upstream URLs. Safe for concurrent use.
type HTTPClient struct {
	orderURL     string
	paymentURL   string
	inventoryURL string
	http         *http.Client
}

// New constructs an HTTPClient. baseOrderURL/Payment/Inventory are
// full base URLs (no trailing slash). hc may be nil (defaults to
// http.Client{Timeout: 10s}).
func New(hc *http.Client, orderURL, paymentURL, inventoryURL string) *HTTPClient {
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPClient{
		orderURL:     strings.TrimRight(orderURL, "/"),
		paymentURL:   strings.TrimRight(paymentURL, "/"),
		inventoryURL: strings.TrimRight(inventoryURL, "/"),
		http:         hc,
	}
}
