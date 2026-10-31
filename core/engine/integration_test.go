//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestEngineRealBroker(t *testing.T) {
	seeds := kafkatest.Real(t)
	cl := kafkatest.Client(t, seeds...)
	e := engine.New(cl, fast)
	sub := e.Subscribe()
	start(t, e)
	defer e.Watch("kaas-engine-it")()
	waitFor(t, sub, func(*snapshot.Snapshot, []engine.Event) bool { return true })

	ctx := context.Background()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 3, 1, nil, "kaas-engine-it"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, sub, func(_ *snapshot.Snapshot, evs []engine.Event) bool {
		return has(evs, engine.TopicCreated, "kaas-engine-it")
	})

	prod := kafkatest.Client(t, seeds...)
	for i := range 25 {
		r := &kgo.Record{Topic: "kaas-engine-it", Value: fmt.Appendf(nil, "%d", i)}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, sub, func(s *snapshot.Snapshot, _ []engine.Event) bool {
		tp, ok := s.Topic("kaas-engine-it")
		if !ok || tp.OffsetsAt.IsZero() {
			return false
		}
		var n int64
		for _, p := range tp.Partitions {
			n += p.EndOffset - p.StartOffset
		}
		return n == 25
	})
}
