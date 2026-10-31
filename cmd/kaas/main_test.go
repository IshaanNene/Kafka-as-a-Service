package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
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

func TestWatchCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(2, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = c.CreateTopic("payments", 3, nil)
	}()

	var out bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetArgs([]string{"watch", "-b", brokers, "-o", "json", "-t", "orders",
		"--metadata-interval", "50ms", "--fast-interval", "20ms", "--budget", "1000"})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}

	var sawBaseline, sawCreated, sawRecords bool
	for line := range strings.Lines(out.String()) {
		var l watchLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("bad json line %q: %v", line, err)
		}
		switch {
		case l.Baseline != nil:
			sawBaseline = l.Baseline.Brokers == 3
		case l.Event != nil && l.Event.Kind == engine.TopicCreated && l.Event.Topic == "payments":
			sawCreated = true
		case l.Topic == "orders" && l.Records != nil:
			sawRecords = true
		}
	}
	if !sawBaseline || !sawCreated || !sawRecords {
		t.Errorf("baseline=%v created=%v records=%v; output:\n%s", sawBaseline, sawCreated, sawRecords, out.String())
	}
}
