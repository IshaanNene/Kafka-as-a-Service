//go:build integration

package snapshot_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestTakeRealBroker(t *testing.T) {
	cl := kafkatest.Client(t, kafkatest.Real(t)...)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	if _, err := adm.CreateTopic(ctx, 3, 1, nil, "kaas-it"); err != nil {
		t.Fatal(err)
	}
	s, err := snapshot.NewTaker(cl).Take(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.ClusterID == "" {
		t.Error("empty cluster ID")
	}
	tp, ok := s.Topic("kaas-it")
	if !ok || len(tp.Partitions) != 3 {
		t.Fatalf("kaas-it = %+v, ok=%v", tp, ok)
	}
}
