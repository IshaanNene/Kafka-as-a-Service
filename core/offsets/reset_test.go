package offsets

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestParseTo(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want Target
	}{
		{"earliest", Target{Kind: Earliest}},
		{"Latest", Target{Kind: Latest}},
		{"1043", Target{Kind: Offset, N: 1043}},
		{"+100", Target{Kind: Shift, N: 100}},
		{"-250", Target{Kind: Shift, N: -250}},
		{"2h", Target{Kind: Time, At: now.Add(-2 * time.Hour)}},
		{"1.5d", Target{Kind: Time, At: now.Add(-36 * time.Hour)}},
		{"2026-10-31T09:00:00Z", Target{Kind: Time, At: time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)}},
		{"2026-10-31 09:30", Target{Kind: Time, At: time.Date(2026, 10, 31, 9, 30, 0, 0, time.UTC)}},
		{"2026-10-31", Target{Kind: Time, At: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}},
	} {
		got, err := ParseTo(tc.in, now)
		if err != nil || got.Kind != tc.want.Kind || got.N != tc.want.N || !got.At.Equal(tc.want.At) {
			t.Errorf("ParseTo(%q) = %+v, %v; want %+v", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"", "soon", "+1.5", "2026-11-02", "-2h"} {
		if _, err := ParseTo(bad, now); err == nil {
			t.Errorf("ParseTo(%q) took it", bad)
		}
	}
}

// seed writes n records a minute apart to each partition of orders,
// ending at base, and commits group at committed on each.
func seed(t *testing.T, c *kfake.Cluster, parts int, n int, base time.Time, group string, committed map[int32]int64) {
	t.Helper()
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for p := range int32(parts) {
		for i := range n {
			r := &kgo.Record{Topic: "orders", Partition: p, Value: []byte("v"), Timestamp: base.Add(time.Duration(i-n+1) * time.Minute)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	os := kadm.Offsets{}
	for p, at := range committed {
		os.Add(kadm.Offset{Topic: "orders", Partition: p, At: at, LeaderEpoch: -1})
	}
	if len(os) > 0 {
		if _, err := kadm.NewClient(cl).CommitOffsets(ctx, group, os); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlanReset(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	seed(t, c, 3, 20, base, "billing", map[int32]int64{0: 15, 1: 20})
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	plan := func(to Target) Plan {
		t.Helper()
		p, err := PlanReset(ctx, adm, "billing", []string{"orders"}, to)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Moves) != 3 {
			t.Fatalf("%d moves, want 3", len(p.Moves))
		}
		return p
	}
	type want struct {
		from, to int64
		skip     bool
		clamped  bool
	}
	check := func(name string, p Plan, ws ...want) {
		t.Helper()
		for i, w := range ws {
			m := p.Moves[i]
			if m.From != w.from || m.To != w.to || (m.Skip != "") != w.skip || m.Clamped != w.clamped {
				t.Errorf("%s p%d: %+v, want %+v", name, i, m, w)
			}
		}
	}

	p := plan(Target{Kind: Earliest})
	check("earliest", p, want{15, 0, false, false}, want{20, 0, false, false}, want{-1, 0, false, false})
	if a, ch, s := p.Counts(); a != 1 || ch != 2 || s != 0 {
		t.Errorf("counts = %d %d %d", a, ch, s)
	}
	if re, sk := p.Records(); re != 35 || sk != 0 {
		t.Errorf("records = %d reread, %d skipped", re, sk)
	}
	check("latest", plan(Target{Kind: Latest}), want{15, 20, false, false}, want{20, 20, false, false}, want{-1, 20, false, false})
	check("offset", plan(Target{Kind: Offset, N: 99}), want{15, 20, false, true}, want{20, 20, false, true}, want{-1, 20, false, true})
	check("shift", plan(Target{Kind: Shift, N: -18}), want{15, 0, false, true}, want{20, 2, false, false}, want{-1, -1, true, false})
	// Records are a minute apart ending at base: 5m before base is offset 14.
	check("time", plan(Target{Kind: Time, At: base.Add(-5 * time.Minute)}), want{15, 14, false, false}, want{20, 14, false, false}, want{-1, 14, false, false})
	check("after the last", plan(Target{Kind: Time, At: base.Add(time.Minute)}), want{15, 20, false, false})

	if _, err := PlanReset(ctx, adm, "billing", []string{"nope"}, Target{Kind: Earliest}); !errors.Is(err, ErrNoTopic) {
		t.Errorf("unknown topic: %v", err)
	}
	if got, err := Committed(ctx, adm, "billing"); err != nil || len(got) != 1 || got[0] != "orders" {
		t.Errorf("committed = %v, %v", got, err)
	}
}

func TestApply(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	seed(t, c, 2, 10, time.Now(), "billing", map[int32]int64{0: 10})
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	p, err := PlanReset(ctx, adm, "billing", []string{"orders"}, Target{Kind: Offset, N: 4})
	if err != nil {
		t.Fatal(err)
	}
	res, err := Apply(ctx, adm, p)
	if err != nil || len(res["orders"]) != 2 || res["orders"][0] != nil || res["orders"][1] != nil {
		t.Fatalf("apply = %v, %v", res, err)
	}
	got, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).FetchOffsets(ctx, "billing")
	if err != nil {
		t.Fatal(err)
	}
	for id := range int32(2) {
		if o, _ := got.Lookup("orders", id); o.At != 4 {
			t.Errorf("p%d committed at %d, want 4", id, o.At)
		}
	}
}

func TestPlanActiveGroup(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	member, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	adm := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			member.PollFetches(ctx)
		}
	}()
	for {
		p, err := PlanReset(ctx, adm, "billing", []string{"orders"}, Target{Kind: Earliest})
		if errors.Is(err, ErrActive) {
			if p.Members != 1 || len(p.Moves) != 1 {
				t.Errorf("active plan: %+v", p)
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("the member never showed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
