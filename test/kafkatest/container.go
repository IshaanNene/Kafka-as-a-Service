//go:build integration

package kafkatest

import (
	"context"
	"os"
	"testing"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// DefaultImage is used when KAAS_KAFKA_IMAGE is unset. CI sets it per
// matrix entry (Kafka 3.9, 4.2, 4.4).
const DefaultImage = "apache/kafka:4.2.0"

// Real starts a single-node KRaft broker in a container and returns its
// seed addresses. The container is terminated when the test ends.
func Real(t testing.TB) []string {
	t.Helper()
	img := os.Getenv("KAAS_KAFKA_IMAGE")
	if img == "" {
		img = DefaultImage
	}
	ctx := context.Background()
	c, err := tckafka.Run(ctx, img, tckafka.WithClusterID("kaas-test"))
	if err != nil {
		t.Fatalf("start %s: %v", img, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}
	return brokers
}
