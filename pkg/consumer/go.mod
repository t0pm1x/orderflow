module github.com/t0pm1x/orderflow/consumer

go 1.25.13

require (
	github.com/t0pm1x/orderflow/platform v0.0.0
	github.com/twmb/franz-go v1.21.6
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/t0pm1x/orderflow/platform => ../platform
