module github.com/t0pm1x/orderflow/services/web

go 1.25.13

replace github.com/t0pm1x/orderflow/consumer => ../../pkg/consumer

replace github.com/t0pm1x/orderflow/kafkaprop => ../../pkg/platform/instrumentation/kafkaprop

replace github.com/t0pm1x/orderflow/platform => ../../pkg/platform

require github.com/google/uuid v1.6.0
