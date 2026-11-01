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
		{[]string{"bench", "produce", "-b", brokers, "-t", "oders"}, exitNotFound, "did you mean orders?"},
		{[]string{"bench", "produce", "-b", brokers}, exitUsage, "--topic: required"},
		{[]string{"bench", "produce", "-t", "orders", "--compression", "brotli"}, exitUsage, `--compression "brotli": must be one of`},
		{[]string{"bench", "consume", "-b", brokers, "-t", "orders"}, exitUsage, "has no records to read yet"},
		{[]string{"bench", "consume", "-t", "orders", "--fetch-size", "lots"}, exitUsage, `--fetch-size "lots": want a size like 64KB`},
		{[]string{"bench", "consume", "-t", "orders", "--fetch-size", "100B"}, exitUsage, "must be at least 1KB"},
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

// TestWatchViewRates checks the live view samples records/sec from end
// offsets only when the topic's offsets were actually re-read.
func TestWatchViewRates(t *testing.T) {
	t0 := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)
	snap := func(at time.Time, end int64) *snapshot.Snapshot {
		return &snapshot.Snapshot{Topics: []snapshot.Topic{{Name: "orders", OffsetsAt: at,
			Partitions: []snapshot.Partition{{ID: 0, StartOffset: 10, EndOffset: end}}}}}
	}
	v := newWatchView(surface{width: 100, height: 30}, []string{"orders"}, engine.Config{})
	v.update(engine.Update{Snapshot: snap(t0, 100)}, engine.Stats{}, t0)
	v.update(engine.Update{Snapshot: snap(t0.Add(2*time.Second), 1100)}, engine.Stats{}, t0)
	v.update(engine.Update{Snapshot: snap(t0.Add(2*time.Second), 1100), Events: []engine.Event{{Kind: engine.TopicCreated, Topic: "x", Count: 3}}}, engine.Stats{}, t0)
	s := v.series["orders"]
	if len(s.Rates) != 1 || s.Rates[0] != 500 || s.Retained != 1090 {
		t.Errorf("rates=%v retained=%d, want [500] 1090", s.Rates, s.Retained)
	}
	if len(v.events) != 1 || toastFor(v.events[0].Event).Title != "Topic created" {
		t.Errorf("events = %v", v.events)
	}
}

func TestBenchCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	out := runKaas(t, "bench", "produce", "-b", brokers, "-t", "orders", "--records", "200", "--compression", "none,lz4")
	if n := strings.Count(out, "200 records sent"); n != 2 {
		t.Errorf("want 2 case lines, got:\n%s", out)
	}
	var res []struct{ Records int64 }
	if err := json.Unmarshal([]byte(runKaas(t, "bench", "produce", "-b", brokers, "-t", "orders", "--records", "50", "-o", "json")), &res); err != nil || len(res) != 1 || res[0].Records != 50 {
		t.Errorf("json = %v (%v)", res, err)
	}

	// 453 records now: 2×200 and 50, plus a warm-up record for each of the 3 cases.
	out = runKaas(t, "bench", "consume", "-b", brokers, "-t", "orders", "--records", "300", "--fetch-size", "4KB,1MB")
	if n := strings.Count(out, "300 records read"); n != 2 || !strings.Contains(out, "fetch=4KB wait=500ms") {
		t.Errorf("want 2 consume case lines, got:\n%s", out)
	}
	var cres []struct {
		Case struct {
			FetchSize int `json:"fetch_size"`
			Records   int
		}
		Records int64
		Polls   int64
	}
	if err := json.Unmarshal([]byte(runKaas(t, "bench", "consume", "-b", brokers, "-t", "orders", "-o", "json")), &cres); err != nil ||
		len(cres) != 1 || cres[0].Records != 453 || cres[0].Case.Records != 453 || cres[0].Case.FetchSize != 1<<20 || cres[0].Polls == 0 {
		t.Errorf("consume json = %+v (%v)", cres, err)
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int{"1048576": 1 << 20, "64KB": 64 << 10, "1mb": 1 << 20, "8 MiB": 8 << 20, "512k": 512 << 10} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0", "-1KB", "1GB", "4096MB"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) should fail", in)
		}
	}
}
