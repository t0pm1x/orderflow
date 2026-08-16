module github.com/t0pm1x/orderflow/services/inventory

go 1.25.13

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.10.0
	github.com/pressly/goose/v3 v3.27.3
	github.com/redis/go-redis/v9 v9.7.0
	github.com/t0pm1x/orderflow/platform v0.0.0
	github.com/twmb/franz-go v1.21.6
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.70.0
)

replace github.com/t0pm1x/orderflow/platform => ../../pkg/platform
