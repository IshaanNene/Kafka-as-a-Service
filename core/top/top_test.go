package top

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestRates(t *testing.T) {
	at := time.Now()
	a, b := Key{"orders", 0}, Key{"orders", 1}
	prev := Sample{At: at, End: map[Key]int64{a: 100, b: 50}, Size: map[Key]int64{a: 1000},
		Read: map[string]map[Key]int64{"billing": {a: 90}}}
	cur := Sample{At: at.Add(2 * time.Second), End: map[Key]int64{a: 140, b: 50, {"audit", 0}: 7},
		Size: map[Key]int64{a: 1400, b: 10}, Leader: map[Key]int32{a: 2},
		Read: map[string]map[Key]int64{"billing": {a: 110}, "fresh": {a: 140}}}
	rows := Rates(prev, cur)
	if len(rows) != 3 || rows[0].Topic != "audit" {
		t.Fatalf("rows = %+v, want audit first then orders 0, 1", rows)
	}
	audit, r0, r1 := rows[0], rows[1], rows[2]
	if audit.In != -1 || audit.Out != -1 {
		t.Errorf("new partition has unknown rates: %+v", audit)
	}
	if r0.In != 20 || r0.Bytes != 200 || r0.Out != 10 || r0.Leader != 2 || r0.Size != 1400 {
		t.Errorf("orders 0 = %+v, want 20 in, 10 out (the new group ignored), 200 B/s", r0)
	}
	if r1.In != 0 || r1.Bytes != -1 || r1.Out != 0 || r1.Size != 10 || r1.Leader != -1 {
		t.Errorf("orders 1 = %+v, want idle with unknown growth", r1)
	}
}

func TestTake(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(2, "orders"))
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	for range 20 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	md, err := adm.Metadata(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	leaders := map[Key]int32{}
	for _, p := range md.Topics["orders"].Partitions {
		leaders[Key{"orders", p.Partition}] = p.Leader
	}
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: "orders", Partition: 0, At: 3, LeaderEpoch: -1})
	if err := adm.CommitAllOffsets(ctx, "billing", os); err != nil {
		t.Fatal(err)
	}
	s, err := Take(ctx, adm, []string{"orders"}, leaders)
	if err != nil {
		t.Fatal(err)
	}
	var end, size int64
	for k, e := range s.End {
		end += e
		size += s.Size[k]
	}
	if end != 20 || size <= 0 || s.Read["billing"][Key{"orders", 0}] != 3 {
		t.Errorf("sample = %+v, want 20 records, leader sizes, billing at 3", s)
	}
}
