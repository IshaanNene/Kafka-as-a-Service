// Package kafkatest is the shared test harness: an in-memory kfake cluster
// for fast unit tests, and a Testcontainers-backed real broker for
// integration tests (build tag "integration").
package kafkatest

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Fake starts an in-memory cluster and returns a connected admin client.
// Both are closed when the test ends.
func Fake(t testing.TB, opts ...kfake.Opt) (*kfake.Cluster, *kadm.Client) {
	t.Helper()
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return c, Admin(t, c.ListenAddrs()...)
}

// Admin returns an admin client for the given seed brokers, closed at test end.
func Admin(t testing.TB, seeds ...string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatalf("kgo: %v", err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}
