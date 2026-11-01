package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestDelete(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders", "audit", "keep"))
	ctx := context.Background()
	res, err := Delete(ctx, kadm.NewClient(cl), []string{"orders", "audit", "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if res["orders"] != nil || res["audit"] != nil || !errors.Is(res["nope"], kerr.UnknownTopicOrPartition) {
		t.Errorf("results = %v", res)
	}
	// A fresh client: kgo caches metadata for a few seconds.
	td, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if td.Has("orders") || td.Has("audit") || !td.Has("keep") {
		t.Errorf("left %v", td.Names())
	}
}
