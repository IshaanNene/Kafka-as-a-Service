package engine

import (
	"fmt"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

func part(id, leader int32, replicas, isr []int32) snapshot.Partition {
	return snapshot.Partition{ID: id, Leader: leader, Replicas: replicas, ISR: isr}
}

func base() *snapshot.Snapshot {
	r := []int32{1, 2, 3}
	return &snapshot.Snapshot{
		Controller: 1,
		Brokers:    []snapshot.Broker{{ID: 1}, {ID: 2}, {ID: 3}},
		Topics: []snapshot.Topic{
			{Name: "orders", Partitions: []snapshot.Partition{part(0, 1, r, r), part(1, 2, r, r)}},
			{Name: "payments", Partitions: []snapshot.Partition{part(0, 3, r, r)}},
		},
	}
}

func kinds(evs []Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.String())
	}
	return out
}

func TestDiff(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(s *snapshot.Snapshot)
		want   []string
	}{
		{"no change", func(*snapshot.Snapshot) {}, nil},
		{"broker added and removed", func(s *snapshot.Snapshot) {
			s.Brokers = []snapshot.Broker{{ID: 1}, {ID: 2}, {ID: 4}}
		}, []string{"BrokerAdded broker 4", "BrokerRemoved broker 3"}},
		{"controller moved", func(s *snapshot.Snapshot) { s.Controller = 2 },
			[]string{"ControllerMoved 1 → 2"}},
		{"topic created", func(s *snapshot.Snapshot) {
			s.Topics = append(s.Topics, snapshot.Topic{Name: "refunds", Partitions: make([]snapshot.Partition, 4)})
		}, []string{"TopicCreated refunds (4 partitions)"}},
		{"topic created in the middle", func(s *snapshot.Snapshot) {
			s.Topics = slices.Insert(s.Topics, 1, snapshot.Topic{Name: "outbox"})
		}, []string{"TopicCreated outbox (0 partitions)"}},
		{"topic deleted", func(s *snapshot.Snapshot) { s.Topics = s.Topics[1:] },
			[]string{"TopicDeleted orders"}},
		{"partitions added", func(s *snapshot.Snapshot) {
			r := []int32{1, 2, 3}
			s.Topics[1].Partitions = append(s.Topics[1].Partitions, part(1, 1, r, r), part(2, 2, r, r))
		}, []string{"PartitionsAdded payments +2"}},
		{"leader moved", func(s *snapshot.Snapshot) { s.Topics[0].Partitions[1].Leader = 3 },
			[]string{"LeaderMoved orders/1 2 → 3"}},
		{"partition offline", func(s *snapshot.Snapshot) { s.Topics[1].Partitions[0].Leader = -1 },
			[]string{"PartitionOffline payments/0 (was leader 3)"}},
		{"isr shrunk", func(s *snapshot.Snapshot) { s.Topics[0].Partitions[0].ISR = []int32{1} },
			[]string{"ISRShrunk orders/0 -[2 3]"}},
		{"isr swapped", func(s *snapshot.Snapshot) { s.Topics[0].Partitions[0].ISR = []int32{1, 2, 4} },
			[]string{"ISRShrunk orders/0 -[3]", "ISRExpanded orders/0 +[4]"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next := base()
			tt.mutate(next)
			got := kinds(Diff(base(), next))
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestDiffNilIsBaseline(t *testing.T) {
	if evs := Diff(nil, base()); evs != nil {
		t.Errorf("Diff(nil, s) = %v, want nil", evs)
	}
}

func TestDiffISRExpandedRoundTrip(t *testing.T) {
	shrunk := base()
	shrunk.Topics[0].Partitions[0].ISR = []int32{1}
	got := kinds(Diff(shrunk, base()))
	want := []string{"ISRExpanded orders/0 +[2 3]"}
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// genSnapshot draws a random, well-formed snapshot: sorted unique topics,
// partitions numbered 0..n-1, leaders and ISRs drawn from the brokers.
func genSnapshot(t *rapid.T) *snapshot.Snapshot {
	brokers := rapid.SliceOfNDistinct(rapid.Int32Range(1, 6), 1, 6, rapid.ID[int32]).Draw(t, "brokers")
	slices.Sort(brokers)
	s := &snapshot.Snapshot{Controller: rapid.SampledFrom(brokers).Draw(t, "controller")}
	for _, b := range brokers {
		s.Brokers = append(s.Brokers, snapshot.Broker{ID: b})
	}
	names := rapid.SliceOfNDistinct(rapid.IntRange(0, 8), 0, 6, rapid.ID[int]).Draw(t, "topics")
	slices.Sort(names)
	for _, n := range names {
		tp := snapshot.Topic{Name: fmt.Sprintf("t%d", n)}
		for p := range rapid.IntRange(0, 4).Draw(t, "partitions") {
			leader := rapid.SampledFrom(append([]int32{-1}, brokers...)).Draw(t, "leader")
			isr := rapid.SliceOfDistinct(rapid.SampledFrom(brokers), rapid.ID[int32]).Draw(t, "isr")
			tp.Partitions = append(tp.Partitions, part(int32(p), leader, brokers, isr))
		}
		s.Topics = append(s.Topics, tp)
	}
	return s
}

func TestDiffProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, b := genSnapshot(t), genSnapshot(t)

		if evs := Diff(a, a); len(evs) != 0 {
			t.Fatalf("Diff(a, a) = %v, want none", evs)
		}
		if !slices.EqualFunc(Diff(a, b), Diff(a, b), func(x, y Event) bool { return x.String() == y.String() }) {
			t.Fatal("Diff is not deterministic")
		}

		// Every created topic exists only in b, every deleted one only in a.
		for _, e := range Diff(a, b) {
			_, inA := a.Topic(e.Topic)
			_, inB := b.Topic(e.Topic)
			switch e.Kind {
			case TopicCreated:
				if inA || !inB {
					t.Fatalf("%v: inA=%v inB=%v", e, inA, inB)
				}
			case TopicDeleted:
				if !inA || inB {
					t.Fatalf("%v: inA=%v inB=%v", e, inA, inB)
				}
			}
		}
	})
}

func BenchmarkDiff100kPartitions(b *testing.B) {
	r := []int32{1, 2, 3}
	mk := func() *snapshot.Snapshot {
		s := &snapshot.Snapshot{Controller: 1, Brokers: []snapshot.Broker{{ID: 1}, {ID: 2}, {ID: 3}}}
		for t := range 1000 {
			tp := snapshot.Topic{Name: fmt.Sprintf("topic-%04d", t)}
			for p := range 100 {
				tp.Partitions = append(tp.Partitions, part(int32(p), int32(p%3+1), r, r))
			}
			s.Topics = append(s.Topics, tp)
		}
		return s
	}
	prev, next := mk(), mk()
	// A rolling restart's worth of churn: 1% of leaders move.
	for t := 0; t < 1000; t += 10 {
		next.Topics[t].Partitions[0].Leader = 3
	}
	b.ReportAllocs()
	for b.Loop() {
		Diff(prev, next)
	}
}
