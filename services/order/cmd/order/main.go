// orderflow Order Service — receives POST /v1/orders, emits OrderCreated.
package main

import (
	"fmt"
	"os"

	"github.com/t0pm1x/orderflow/platform"
)

var version = "0.0.0-dev"

func main() {
	logger := platform.NewLogger()
	logger.Info("orderflow-order starting", "version", version)

	// TODO: wire DB pool, Kafka client, outbox poller, REST API
	// See sub-stages 3.4.b (domain), 3.4.c (REST), 3.4.d (outbox)
	_ = os.Getenv

	fmt.Printf("orderflow-order version %s\n", version)
}
