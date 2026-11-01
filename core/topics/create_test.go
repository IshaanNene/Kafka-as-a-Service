package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestCreate(t *testing.T) {
	c0, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(1, "orders"))
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	exists := func(name string) bool {
		// A fresh client: kgo caches metadata for a few seconds.
		td, err := kadm.NewClient(kafkatest.Client(t, c0.ListenAddrs()...)).ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return td.Has(name)
	}

	spec := Spec{Name: "payments", Partitions: 6, ReplicationFactor: 2, Configs: map[string]string{"cleanup.policy": "compact"}}
	if c, err := Create(ctx, adm, spec, true); err != nil || c.Partitions != 6 || c.ID != "" || exists("payments") {
		t.Fatalf("dry run = %+v, %v; want 6 partitions, no ID, nothing created", c, err)
	}
	c, err := Create(ctx, adm, spec, false)
	if err != nil || c.Partitions != 6 || c.ReplicationFactor != 2 || len(c.ID) != 22 || !exists("payments") {
		t.Fatalf("create = %+v, %v", c, err)
	}
	got, err := Describe(ctx, adm, []string{"payments"})
	if err != nil || got["payments"].Cleanup != "compact" {
		t.Errorf("configs not applied: %+v, %v", got["payments"], err)
	}

	// Zero takes the broker defaults.
	if c, err := Create(ctx, adm, Spec{Name: "defaults"}, false); err != nil || c.Partitions < 1 || c.ReplicationFactor < 1 {
		t.Errorf("broker defaults = %+v, %v", c, err)
	}
	if _, err := Create(ctx, adm, spec, false); !errors.Is(err, kerr.TopicAlreadyExists) {
		t.Errorf("again: %v, want TOPIC_ALREADY_EXISTS", err)
	}
	if _, err := Create(ctx, adm, Spec{Name: "wide", Partitions: 1, ReplicationFactor: 5}, true); !errors.Is(err, kerr.InvalidReplicationFactor) {
		t.Errorf("RF 5 on 3 brokers: %v, want INVALID_REPLICATION_FACTOR", err)
	}
}

func TestValidName(t *testing.T) {
	for name, ok := range map[string]bool{
		"orders": true, "payments.eu-1_v2": true, "": false, ".": false, "..": false,
		"has space": false, "ünïcode": false, string(make([]byte, 250)): false,
	} {
		if err := ValidName(name); (err == nil) != ok {
			t.Errorf("ValidName(%q) = %v, want ok %v", name, err, ok)
		}
	}
	existing := []string{"payments.eu", "orders"}
	if got := Collides("payments_eu", existing); got != "payments.eu" {
		t.Errorf("Collides = %q, want payments.eu", got)
	}
	if got := Collides("payments.eu", existing); got != "" {
		t.Errorf("a name doesn't collide with itself: %q", got)
	}
}
