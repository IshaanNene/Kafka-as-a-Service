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

// TestErrorContract pins the exit codes and the plain and JSON error output
// scripts see when stderr is not a terminal.
func TestErrorContract(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	tests := []struct {
		args []string
		exit int
		want string
	}{
		{[]string{"brokrs"}, exitNotFound, `unknown command "brokrs"`},
		{[]string{"snapshot", "-o", "yaml"}, exitUsage, `--output "yaml": must be one of table, json`},
		{[]string{"watch", "--tpoic", "x"}, exitUsage, "did you mean --topic?"},
		{[]string{"watch", "--budget", "abc"}, exitUsage, "not a valid float64"},
		{[]string{"snapshot", "-b", "127.0.0.1:1"}, exitUnavailable, "cannot reach cluster at 127.0.0.1:1: tcp"},
		{[]string{"watch", "-b", brokers, "-t", "oders"}, exitNotFound, "did you mean orders?"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			root := newRoot()
			root.SetOut(&out)
			root.SetErr(&errOut)
			root.SetArgs(tt.args)
			cmd, err := root.ExecuteC()
			if code := report(detect(&errOut, "amber"), err, wantsJSON(cmd)); code != tt.exit {
				t.Errorf("exit = %d, want %d (err %v)", code, tt.exit, err)
			}
			if !strings.Contains(errOut.String(), tt.want) {
				t.Errorf("stderr = %q, want %q", errOut.String(), tt.want)
			}
		})
	}

	var errOut bytes.Buffer
	root := newRoot()
	root.SetErr(&errOut)
	root.SetArgs([]string{"snapshot", "-b", "127.0.0.1:1", "-o", "json"})
	cmd, err := root.ExecuteC()
	report(detect(&errOut, "amber"), err, wantsJSON(cmd))
	var got struct {
		Error struct {
			Code string
			Exit int
		}
	}
	if err := json.Unmarshal(errOut.Bytes(), &got); err != nil || got.Error.Code != "unavailable" || got.Error.Exit != exitUnavailable {
		t.Errorf("json error = %s (%v)", errOut.String(), err)
	}
}
