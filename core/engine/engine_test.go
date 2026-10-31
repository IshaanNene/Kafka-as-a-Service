package engine_test

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// fast keeps tests quick; the budget is generous unless a test overrides it.
var fast = engine.Config{
	MetadataInterval:  50 * time.Millisecond,
	FastInterval:      20 * time.Millisecond,
	SlowInterval:      200 * time.Millisecond,
	RequestsPerSecond: 10_000,
	Burst:             10_000,
}

func start(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// waitFor reads updates until cond accepts the accumulated events and the
// latest snapshot, or fails the test after 5 s.
func waitFor(t *testing.T, sub *engine.Subscription, cond func(*snapshot.Snapshot, []engine.Event) bool) {
	t.Helper()
	var seen []engine.Event
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-sub.Ready():
			u, _ := sub.Next()
			seen = append(seen, u.Events...)
			if u.Snapshot != nil && cond(u.Snapshot, seen) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out; events seen: %v", seen)
		}
	}
}

func has(evs []engine.Event, k engine.Kind, topic string) bool {
	return slices.ContainsFunc(evs, func(e engine.Event) bool { return e.Kind == k && e.Topic == topic })
}

func TestEngineEmitsEvents(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(4, "orders"))
	e := engine.New(cl, fast)
	sub := e.Subscribe()
	start(t, e)
	waitFor(t, sub, func(s *snapshot.Snapshot, _ []engine.Event) bool { return s.PartitionCount() >= 4 })

	if err := c.CreateTopic("payments", 2, nil); err != nil {
		t.Fatal(err)
	}
	from := c.LeaderFor("orders", 0)
	to := (from + 1) % 3
	if err := c.MoveTopicPartition("orders", 0, to); err != nil {
		t.Fatal(err)
	}

	waitFor(t, sub, func(_ *snapshot.Snapshot, evs []engine.Event) bool {
		return has(evs, engine.TopicCreated, "payments") &&
			slices.ContainsFunc(evs, func(e engine.Event) bool {
				return e.Kind == engine.LeaderMoved && e.Topic == "orders" && e.Partition == 0 && e.From == from && e.To == to
			})
	})
}

func TestEngineWatchedOffsets(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(2, "orders"))
	e := engine.New(cl, fast)
	sub := e.Subscribe()
	start(t, e)
	defer e.Watch("orders")()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.DefaultProduceTopic("orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := range 10 {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Value: fmt.Appendf(nil, "%d", i)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, sub, func(s *snapshot.Snapshot, _ []engine.Event) bool {
		tp, ok := s.Topic("orders")
		if !ok || tp.OffsetsAt.IsZero() {
			return false
		}
		var total int64
		for _, p := range tp.Partitions {
			total += p.EndOffset - p.StartOffset
		}
		return total == 10
	})
}

func TestEngineRespectsBudget(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders"))
	var metadata, listOffsets atomic.Int64
	count := func(n *atomic.Int64) func(kmsg.Request) (kmsg.Response, error, bool) {
		return func(kmsg.Request) (kmsg.Response, error, bool) { n.Add(1); return nil, nil, false }
	}
	c.ControlKey(int16(kmsg.Metadata), count(&metadata))
	c.ControlKey(int16(kmsg.ListOffsets), count(&listOffsets))

	cfg := fast
	cfg.FastInterval = 5 * time.Millisecond
	cfg.RequestsPerSecond = 0.001 // no meaningful refill during the test
	cfg.Burst = 3
	e := engine.New(cl, cfg)
	start(t, e)
	for range 100 {
		e.Refresh() // must coalesce, and must not bypass the budget
	}
	time.Sleep(300 * time.Millisecond)

	// Three tokens buy at most three metadata fetches (1 each) and never
	// the offsets for three leaders (2×3). Without the budget,
	// 60 ticks and 100 refreshes would have sent well over 100 requests.
	if got := metadata.Load(); got > 3 {
		t.Errorf("metadata requests = %d, want <= 3", got)
	}
	if got := listOffsets.Load(); got != 0 {
		t.Errorf("list-offsets requests = %d, want 0 (offsets cost more than the budget)", got)
	}
	st := e.Stats()
	if st.Budget.Denied == 0 {
		t.Error("budget denied nothing")
	}
	if st.Generation > 3 {
		t.Errorf("generation = %d, want <= 3: one per paid metadata fetch", st.Generation)
	}
	if e.Latest() == nil {
		t.Error("no snapshot published")
	}
}

func TestSubscriberThatFallsBehind(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "t0"))
	e := engine.New(cl, fast)
	sub := e.Subscribe()
	start(t, e)
	waitFor(t, sub, func(*snapshot.Snapshot, []engine.Event) bool { return true })

	// Create topics without reading; the mailbox must keep only the newest
	// snapshot and accumulate every event.
	for i := 1; i <= 5; i++ {
		if err := c.CreateTopic(fmt.Sprintf("t%d", i), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, sub, func(s *snapshot.Snapshot, evs []engine.Event) bool {
		created := 0
		for _, ev := range evs {
			if ev.Kind == engine.TopicCreated {
				created++
			}
		}
		return len(s.Topics) == 6 && created == 5
	})
}
