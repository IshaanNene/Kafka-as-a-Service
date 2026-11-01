package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// runExit runs kaas and returns stdout, stderr and the exit code report gives.
func runExit(args ...string) (string, string, int) {
	var out, errOut bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(""))
	root.SetArgs(args)
	cmd, err := root.ExecuteC()
	code := report(detect(&errOut, "amber"), err, wantsJSON(cmd))
	return out.String(), errOut.String(), code
}

func TestPlanAndApply(t *testing.T) {
	c, cl := kafkatest.Fake(t)
	brokers := strings.Join(c.ListenAddrs(), ",")
	if _, err := kadm.NewClient(cl).CreateTopics(context.Background(), 2, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "dev.yaml")
	if err := os.WriteFile(file, []byte(`cluster: dev
topics:
  orders: {partitions: 4, configs: {retention.ms: 2d}}
  audit: {partitions: 1}
quotas:
  - {user: etl, values: {producer_byte_rate: 2MiB}}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	out, _, code := runExit("plan", "-b", brokers, "-f", file)
	if code != exitOK {
		t.Fatalf("plan exit %d\n%s", code, out)
	}
	for _, want := range []string{
		"for dev: 4 changes (2 safe, 2 caution, 0 destructive)",
		"+ create-topic", "audit",
		"~ add-partitions", "orders  2 → 4",
		"set-config", "retention.ms  604800000 → 172800000", "less data is kept",
		"+ set-quota", "user=etl producer_byte_rate  2 MiB/s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("plan output lacks %q:\n%s", want, out)
		}
	}

	if _, errOut, code := runExit("plan", "-b", brokers, "-f", file, "--exit-code"); code != exitDrift || !strings.Contains(errOut, "dev differs from") {
		t.Errorf("--exit-code: %d %q", code, errOut)
	}

	jsonOut, _, code := runExit("plan", "-b", brokers, "-f", file, "-o", "json")
	var p struct {
		ID  string
		Ops []struct{ Kind, Target string }
	}
	if err := json.Unmarshal([]byte(jsonOut), &p); err != nil || code != exitOK || len(p.Ops) != 4 || p.ID == "" {
		t.Fatalf("json plan: %v %d %s", err, code, jsonOut)
	}

	if _, errOut, code := runExit("apply", "-b", brokers, "-f", file); code != exitUsage || !strings.Contains(errOut, "--yes") {
		t.Errorf("apply without a terminal or --yes: %d %q", code, errOut)
	}
	if _, errOut, code := runExit("apply", "-b", brokers, "-f", file, "--yes", "--plan", "0123456789abcdef"); code != exitFailure || !strings.Contains(errOut, "not the reviewed 0123456789abcdef") {
		t.Errorf("apply with a stale plan: %d %q", code, errOut)
	}
	out, _, code = runExit("apply", "-b", brokers, "-f", file, "--yes", "--plan", p.ID)
	if code != exitOK || strings.Count(out, "✓") != 4 {
		t.Fatalf("apply: %d\n%s", code, out)
	}
	if out, _, code := runExit("plan", "-b", brokers, "-f", file, "--exit-code"); code != exitOK || !strings.Contains(out, "matches") {
		t.Errorf("after apply: %d\n%s", code, out)
	}
}

func TestPlanRefusals(t *testing.T) {
	c, _ := kafkatest.Fake(t)
	brokers := strings.Join(c.ListenAddrs(), ",")
	dir := t.TempDir()
	blocked := filepath.Join(dir, "blocked.yaml")
	_ = os.WriteFile(blocked, []byte("topics: {tiny: {partitions: 1}}\npolicies: [{name: three, rule: topic.partitions >= 3}]\n"), 0o600)
	bad := filepath.Join(dir, "bad.yaml")
	_ = os.WriteFile(bad, []byte("topix: {}\n"), 0o600)

	for _, tc := range []struct {
		args []string
		exit int
		want string
	}{
		{[]string{"plan", "-b", brokers, "-f", blocked}, exitFailure, "policy three refuses topic tiny"},
		{[]string{"apply", "-b", brokers, "-f", blocked, "--yes"}, exitFailure, "can't be applied"},
		{[]string{"plan", "-b", brokers, "-f", bad}, exitUsage, "field topix not found"},
		{[]string{"plan", "-b", brokers, "-f", filepath.Join(dir, "missing.yaml")}, exitUsage, "can't read it"},
		{[]string{"plan", "-b", brokers}, exitUsage, "--file: required"},
		{[]string{"console", "--allow-apply", "--listen", "0.0.0.0:0"}, exitUsage, "needs a localhost address"},
	} {
		_, errOut, code := runExit(tc.args...)
		if code != tc.exit || !strings.Contains(errOut, tc.want) {
			t.Errorf("kaas %v: exit %d, stderr %q; want %d and %q", tc.args, code, errOut, tc.exit, tc.want)
		}
	}
}

func TestLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:7070": true, "localhost:1": true, "[::1]:80": true,
		"0.0.0.0:7070": false, ":7070": false, "10.0.0.5:7070": false, "nonsense": false,
	} {
		if got := loopback(addr); got != want {
			t.Errorf("loopback(%q) = %v", addr, got)
		}
	}
}
