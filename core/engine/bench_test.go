package engine_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// bigCluster is the Phase 1 scale target: 100k partitions across 1,000
// topics on 3 brokers. Seeding takes a few seconds, so it is shared.
func bigCluster(b *testing.B) (*kfake.Cluster, []string) {
	b.Helper()
	topics := make([]string, 1000)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic-%04d", i)
	}
	c, _ := kafkatest.Fake(b, kfake.NumBrokers(3), kfake.SeedTopics(100, topics...))
	return c, topics
}

// BenchmarkFetch100kPartitions is one metadata refresh: request, decode and
// build a Snapshot. It bounds how often metadata can refresh at this scale.
func BenchmarkFetch100kPartitions(b *testing.B) {
	c, _ := bigCluster(b)
	cl := kafkatest.Client(b, c.ListenAddrs()...)
	ctx := context.Background()
	var parts int
	b.ReportAllocs()
	for b.Loop() {
		s, err := snapshot.Fetch(ctx, cl)
		if err != nil {
			b.Fatal(err)
		}
		parts = s.PartitionCount()
	}
	if parts != 100_000 {
		b.Fatalf("partitions = %d", parts)
	}
}

// BenchmarkOffsets100kPartitions is one full slow-tier pass: start and end
// offsets for every partition. It reports admin requests per pass, which is
// what the budget charges for.
func BenchmarkOffsets100kPartitions(b *testing.B) {
	c, topics := bigCluster(b)
	var reqs atomic.Int64
	c.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
		reqs.Add(1)
		return nil, nil, false
	})
	adm := kadm.NewClient(kafkatest.Client(b, c.ListenAddrs()...))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := adm.ListStartOffsets(ctx, topics...); err != nil {
			b.Fatal(err)
		}
		if _, err := adm.ListEndOffsets(ctx, topics...); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(reqs.Load())/float64(b.N), "requests/pass")
}
