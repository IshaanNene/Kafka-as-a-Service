package groups

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestFetch(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders", "payments"))
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := range 40 {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: int32(i % 2), Value: []byte("o")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	empty, err := Fetch(ctx, adm)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Groups) != 0 {
		t.Fatalf("groups before any commit = %+v", empty.Groups)
	}

	commit := func(group string, at int64) {
		t.Helper()
		var os kadm.Offsets
		os.Add(kadm.Offset{Topic: "orders", Partition: 0, At: at, LeaderEpoch: -1})
		os.Add(kadm.Offset{Topic: "orders", Partition: 1, At: at, LeaderEpoch: -1})
		if err := adm.CommitAllOffsets(ctx, group, os); err != nil {
			t.Fatal(err)
		}
	}
	commit("billing", 5)
	commit("audit", 20)

	first, err := Fetch(ctx, adm)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Groups) != 2 || first.Groups[0].Name != "audit" || first.Groups[1].Name != "billing" {
		t.Fatalf("groups = %+v, want audit then billing", first.Groups)
	}
	audit, billing := first.Groups[0], first.Groups[1]
	if audit.Lag != 0 || audit.Committed != 40 || audit.Partitions != 2 {
		t.Errorf("audit = %+v, want caught up with 40 committed over 2 partitions", audit)
	}
	if billing.Lag != 30 || len(billing.Topics) != 1 || billing.Topics[0] != "orders" || billing.TopicLag["orders"] != 30 {
		t.Errorf("billing = %+v, want 30 behind on orders", billing)
	}

	commit("billing", 15)
	second, err := Fetch(ctx, adm)
	if err != nil {
		t.Fatal(err)
	}
	second.At = first.At.Add(2 * time.Second)
	if r, ok := Rate(first, second, "billing"); !ok || r != 10 {
		t.Errorf("billing rate = %v %v, want 20 records over 2s", r, ok)
	}
	if r, ok := Rate(first, second, "audit"); !ok || r != 0 {
		t.Errorf("audit rate = %v %v, want 0", r, ok)
	}
	if _, ok := Rate(empty, second, "billing"); ok {
		t.Error("a group missing from the first sample has no rate")
	}
}

func TestRateResetIsZero(t *testing.T) {
	at := time.Now()
	prev := Sample{At: at, Groups: []Group{{Name: "g", Committed: 100}}}
	cur := Sample{At: at.Add(time.Second), Groups: []Group{{Name: "g", Committed: 10}}}
	if r, ok := Rate(prev, cur, "g"); !ok || r != 0 {
		t.Errorf("rate after a reset = %v %v, want 0", r, ok)
	}
	if _, ok := Rate(cur, prev, "g"); ok {
		t.Error("samples out of order have no rate")
	}
}
