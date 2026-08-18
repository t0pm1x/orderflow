module github.com/t0pm1x/orderflow/cmd/web

go 1.25.13

require github.com/t0pm1x/orderflow/services/web v0.0.0

require (
	github.com/go-chi/chi/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
)

replace github.com/t0pm1x/orderflow/services/web => ../../services/web
