package topics

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestDescribe(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	week, compact := "604800000", "compact"
	if _, err := adm.CreateTopic(ctx, 2, 3, map[string]*string{"retention.ms": &week, "cleanup.policy": &compact}, "users"); err != nil {
		t.Fatal(err)
	}
	for range 50 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	got, err := Describe(ctx, adm, []string{"orders", "users"})
	if err != nil {
		t.Fatal(err)
	}
	users := got["users"]
	if users.Retention != 7*24*time.Hour || !users.RetentionOverride || users.Cleanup != "compact" || !users.CleanupOverride {
		t.Errorf("users = %+v, want 7d compact, both set on the topic", users)
	}
	if users.Overrides < 2 {
		t.Errorf("users overrides = %d, want at least 2", users.Overrides)
	}
	orders := got["orders"]
	if orders.RetentionOverride || orders.CleanupOverride || orders.Cleanup != "delete" {
		t.Errorf("orders = %+v, want broker defaults", orders)
	}
	if orders.Disk <= 0 {
		t.Errorf("orders disk = %d, want the bytes just produced", orders.Disk)
	}
	var replicas int64
	for _, parts := range orders.Replicas {
		for _, b := range parts {
			replicas += b
		}
	}
	if replicas != orders.Disk {
		t.Errorf("per-replica bytes sum to %d, topic disk is %d", replicas, orders.Disk)
	}
	if users.Set["cleanup.policy"] != "compact" || users.Set["retention.ms"] != "604800000" {
		t.Errorf("users set configs = %v", users.Set)
	}
}

func TestParseMillis(t *testing.T) {
	for in, want := range map[string]time.Duration{"-1": Forever, "1000": time.Second, "x": 0} {
		if got := parseMillis(in); got != want {
			t.Errorf("parseMillis(%q) = %v, want %v", in, got, want)
		}
	}
}
