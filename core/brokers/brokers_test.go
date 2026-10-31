package brokers

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestSummarize(t *testing.T) {
	s := &snapshot.Snapshot{
		Controller: 2,
		Brokers:    []snapshot.Broker{{ID: 1, Host: "k1", Port: 9092, Rack: "a"}, {ID: 2, Host: "k2", Port: 9092}, {ID: 3, Host: "k3", Port: 9092}},
		Topics: []snapshot.Topic{
			{Name: "orders", Partitions: []snapshot.Partition{
				{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
				{ID: 1, Leader: 1, Replicas: []int32{1, 3}, ISR: []int32{1}},
				{ID: 2, Leader: 2, Replicas: []int32{2, 4}, ISR: []int32{2}, Offline: []int32{4}},
			}},
			{Name: "audit", Partitions: []snapshot.Partition{
				{ID: 0, Leader: 3, Replicas: []int32{3, 1}, ISR: []int32{3, 1}},
			}},
		},
	}
	bs := Summarize(s, map[int32]int64{1: 100, 2: 50})
	if len(bs) != 4 {
		t.Fatalf("brokers = %+v, want 1-3 and the offline 4", bs)
	}
	b1, b2, b3, b4 := bs[0], bs[1], bs[2], bs[3]
	if !b1.Online || b1.Leaders != 2 || b1.Replicas != 3 || b1.Behind != 0 || b1.Disk != 100 || b1.Rack != "a" {
		t.Errorf("broker 1 = %+v", b1)
	}
	if b1.Topics[0].Name != "orders" || len(b1.Topics[0].Leads) != 2 || b1.Topics[1].Name != "audit" || b1.Topics[1].Follows != 1 {
		t.Errorf("broker 1 topics = %+v, want orders (leads 2) then audit (follows 1)", b1.Topics)
	}
	if !b2.Controller || b1.Controller {
		t.Error("broker 2 is the controller")
	}
	if b3.Behind != 1 || b3.Disk != -1 {
		t.Errorf("broker 3 = %+v, want 1 replica behind and unknown disk", b3)
	}
	if b4.Online || b4.Replicas != 1 || b4.Behind != 1 || b4.Host != "" {
		t.Errorf("broker 4 = %+v, want offline with one replica behind", b4)
	}
	// 4 leaders over 3 online brokers; broker 1 leads 2.
	if got := Skew(bs); got != 1.5 {
		t.Errorf("skew = %v, want 1.5", got)
	}
	if Skew(nil) != 0 {
		t.Error("no brokers, no skew")
	}
}

func TestDisk(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	ctx := context.Background()
	for range 30 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	disk, err := Disk(ctx, kadm.NewClient(cl))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, n := range disk {
		total += n
	}
	if len(disk) != 3 || total <= 0 {
		t.Errorf("disk = %v, want all 3 brokers and the bytes just produced", disk)
	}
}
