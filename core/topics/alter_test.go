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

func TestGrow(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	count := func() int {
		td, err := adm.ListTopics(ctx, "orders")
		if err != nil {
			t.Fatal(err)
		}
		return len(td["orders"].Partitions)
	}

	if err := Grow(ctx, adm, "orders", 5, true); err != nil || count() != 3 {
		t.Fatalf("dry run: err %v, %d partitions; want nil and 3", err, count())
	}
	if err := Grow(ctx, adm, "orders", 5, false); err != nil || count() != 5 {
		t.Fatalf("grow: err %v, %d partitions; want nil and 5", err, count())
	}
	if err := Grow(ctx, adm, "orders", 2, false); !errors.Is(err, kerr.InvalidPartitions) {
		t.Errorf("shrink: err %v, want INVALID_PARTITIONS", err)
	}
	if err := Grow(ctx, adm, "nope", 4, false); !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Errorf("unknown topic: err %v, want UNKNOWN_TOPIC_OR_PARTITION", err)
	}
}
