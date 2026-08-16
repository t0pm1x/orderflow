module github.com/t0pm1x/orderflow/services/saga

go 1.25.13

replace (
	github.com/t0pm1x/orderflow/consumer => ../../pkg/consumer
	github.com/t0pm1x/orderflow/outbox => ../../pkg/outbox
	github.com/t0pm1x/orderflow/platform => ../../pkg/platform
)
