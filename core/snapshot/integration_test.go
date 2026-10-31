//go:build integration

package snapshot_test

import (
	"context"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestTakeRealBroker(t *testing.T) {
	adm := kafkatest.Admin(t, kafkatest.Real(t)...)
	ctx := context.Background()
	if _, err := adm.CreateTopic(ctx, 3, 1, nil, "kaas-it"); err != nil {
		t.Fatal(err)
	}
	s, err := snapshot.NewTaker(adm).Take(ctx)
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
