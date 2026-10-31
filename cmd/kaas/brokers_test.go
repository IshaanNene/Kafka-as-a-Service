package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestBrokersCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders"), kfake.SeedTopics(3, "payments"))
	brokersFlag := strings.Join(c.ListenAddrs(), ",")

	plain := runKaas(t, "brokers", "-b", brokersFlag)
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "ID") || strings.Count(plain, "controller") != 1 {
		t.Errorf("plain output, one line per broker and one controller:\n%s", plain)
	}

	var rows []brokerJSON
	if err := json.Unmarshal([]byte(runKaas(t, "brokers", "-b", brokersFlag, "-o", "json")), &rows); err != nil {
		t.Fatal(err)
	}
	leaders, replicas := 0, 0
	for _, r := range rows {
		leaders += r.Leaders
		replicas += r.Replicas
		if !r.Online || r.DiskBytes == nil || r.Host == "" {
			t.Errorf("broker %+v: want online with a host and known disk", r)
		}
	}
	if len(rows) != 3 || leaders != 9 || replicas < 9 {
		t.Errorf("3 brokers leading 9 partitions, got %d brokers, %d leaders, %d replicas", len(rows), leaders, replicas)
	}
}

func TestBrokersScreen(t *testing.T) {
	s := &snapshot.Snapshot{
		ClusterID: "5L6g3nShQ", Controller: 1,
		Brokers: []snapshot.Broker{{ID: 1, Host: "k1", Port: 9092}, {ID: 2, Host: "k2", Port: 9092}},
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		s.Topics = append(s.Topics, snapshot.Topic{Name: name, Partitions: []snapshot.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 3}, ISR: []int32{1}},
			{ID: 1, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
			{ID: 2, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		}})
	}
	s.Topics = append(s.Topics, snapshot.Topic{Name: "__consumer_offsets", Internal: true, Partitions: []snapshot.Partition{
		{ID: 0, Leader: 2, Replicas: []int32{2}, ISR: []int32{2}},
	}})
	bl := brokerList{snap: s, brokers: brokers.Summarize(s, nil), top: 2}
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	out := kit.Join(bl.screen().Render(k, 100))
	for _, want := range []string{"broker 3", "offline", "+ 2 more", "internal", "__consumer_offsets", "p0-2", "4 behind", "controller", "skew 1.85x"} {
		if !strings.Contains(out, want) {
			t.Errorf("screen missing %q:\n%s", want, out)
		}
	}
	bl.top = 0
	if out := kit.Join(bl.screen().Render(k, 100)); strings.Contains(out, "p0-2") {
		t.Errorf("--top 0 should hide topics:\n%s", out)
	}
}

func TestRanges(t *testing.T) {
	for want, ids := range map[string][]int32{"": nil, "p0": {0}, "p0-2 p5": {0, 1, 2, 5}, "p1 p3-4": {1, 3, 4}} {
		if got := ranges(ids); got != want {
			t.Errorf("ranges(%v) = %q, want %q", ids, got, want)
		}
	}
}
