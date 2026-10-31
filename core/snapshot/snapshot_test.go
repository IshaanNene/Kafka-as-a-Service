package snapshot_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestTake(t *testing.T) {
	_, adm := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders", "payments"))

	tk := snapshot.NewTaker(adm)
	s, err := tk.Take(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Generation != 1 {
		t.Errorf("generation = %d, want 1", s.Generation)
	}
	if len(s.Brokers) != 3 {
		t.Errorf("brokers = %d, want 3", len(s.Brokers))
	}
	if got := s.PartitionCount(); got != 12 {
		t.Errorf("partitions = %d, want 12", got)
	}
	orders, ok := s.Topic("orders")
	if !ok {
		t.Fatal("orders missing")
	}
	for i, p := range orders.Partitions {
		if p.ID != int32(i) {
			t.Errorf("partition %d out of order: id %d", i, p.ID)
		}
	}
	if _, ok := s.Topic("nope"); ok {
		t.Error("found nonexistent topic")
	}

	s2, err := tk.Take(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s2.Generation != 2 {
		t.Errorf("second generation = %d, want 2", s2.Generation)
	}
}

func TestUnderReplicated(t *testing.T) {
	p := snapshot.Partition{Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}}
	if !p.UnderReplicated() {
		t.Error("want under-replicated")
	}
	p.ISR = append(p.ISR, 3)
	if p.UnderReplicated() {
		t.Error("want fully replicated")
	}
}
