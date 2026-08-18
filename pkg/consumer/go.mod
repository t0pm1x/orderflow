module github.com/t0pm1x/orderflow/consumer

go 1.25.13

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/redis/go-redis/v9 v9.22.0
	github.com/t0pm1x/orderflow/kafkaprop v0.0.0-00010101000000-000000000000
	github.com/t0pm1x/orderflow/platform v0.0.0
	github.com/twmb/franz-go v1.21.6
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/sdk v1.45.0
	go.opentelemetry.io/otel/trace v1.45.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/t0pm1x/orderflow/kafkaprop => ../platform/instrumentation/kafkaprop

replace github.com/t0pm1x/orderflow/platform => ../platform
