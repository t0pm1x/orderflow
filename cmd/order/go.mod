module github.com/t0pm1x/orderflow/cmd/order

go 1.25.13

require (
	github.com/t0pm1x/orderflow/outbox v0.0.0
	github.com/t0pm1x/orderflow/platform v0.0.0
	github.com/t0pm1x/orderflow/services/order v0.0.0
)

replace (
	github.com/t0pm1x/orderflow/outbox => ../../pkg/outbox
	github.com/t0pm1x/orderflow/platform => ../../pkg/platform
	github.com/t0pm1x/orderflow/services/order => ../../services/order
)