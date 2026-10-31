package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func runKaas(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		t.Fatalf("kaas %v: %v", args, err)
	}
	return out.String()
}

func TestSnapshotCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(4, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")

	table := runKaas(t, "snapshot", "-b", brokers)
	if !strings.Contains(table, "3 brokers") || !strings.Contains(table, "orders") {
		t.Errorf("table output missing fields:\n%s", table)
	}

	var s snapshot.Snapshot
	if err := json.Unmarshal([]byte(runKaas(t, "snapshot", "-b", brokers, "-o", "json")), &s); err != nil {
		t.Fatal(err)
	}
	if s.PartitionCount() != 4 {
		t.Errorf("json partitions = %d, want 4", s.PartitionCount())
	}
}
