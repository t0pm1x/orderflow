// orderflow Payment Service — webhook ingestor for mock provider, consumer
// for PaymentRequested events.
package main

import (
	"fmt"

	"github.com/t0pm1x/orderflow/platform"
)

var version = "0.0.0-dev"

func main() {
	logger := platform.NewLogger()
	logger.Info("orderflow-payment starting", "version", version)

	// TODO: wire DB, Redis, Kafka client, mock provider, webhook handler
	fmt.Printf("orderflow-payment version %s\n", version)
}