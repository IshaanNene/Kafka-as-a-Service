package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/pipe"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// runPipe runs kaas with in on stdin and returns stdout, stderr and the error.
func runPipe(in string, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(in))
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func writeScript(t *testing.T, path string, mode os.FileMode, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

func TestPipeStdinToStdout(t *testing.T) {
	in := `{"key":"a","value":"{\"n\":5}"}` + "\n" + `{"key":"b","value":"{\"n\":50}"}` + "\n" + `{"key":"c","value":"plain"}` + "\n"
	out, errOut, err := runPipe(in, "pipe", "--from", "stdin", "--to", "stdout?format=text", "--where", "value.n > 10")
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"n":50}`+"\n" {
		t.Errorf("stdout %q", out)
	}
	if !strings.Contains(errOut, "read 3 · wrote 1 · filtered 1 · rejected 1") {
		t.Errorf("stderr %q", errOut)
	}
}

// A topic copied out to a file and the file piped back into another
// topic arrive whole, and -o json says so.
func TestPipeKafkaRoundTrip(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(3, "orders", "copy"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	dir := t.TempDir()
	file := filepath.Join(dir, "orders.jsonl")

	if _, _, err := runPipe("", "pipe", "-b", brokers, "--from", "gen:orders?count=400&seed=3", "--to", "kafka:orders"); err != nil {
		t.Fatal(err)
	}
	out, _, err := runPipe("", "pipe", "-b", brokers, "--from", "kafka:orders?stop=end", "--to", "file:"+file, "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	var st pipeStatsJSON
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if st.Read != 400 || st.Written != 400 || !st.Sources[0].Done || st.Sinks[0].Bytes == 0 {
		t.Errorf("stats %+v", st)
	}
	if _, _, err := runPipe("", "pipe", "-b", brokers, "--from", "file:"+file, "--to", "kafka:copy", "--where", `value.status == "placed"`); err != nil {
		t.Fatal(err)
	}
	out, _, err = runPipe("", "pipe", "-b", brokers, "--from", "kafka:copy?stop=end", "--to", "stdout")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 100 || len(lines) > 300 {
		t.Fatalf("%d placed orders of 400", len(lines))
	}
	for _, l := range lines {
		if !strings.Contains(l, `\"status\":\"placed\"`) || !strings.Contains(l, `"topic":"copy"`) {
			t.Fatalf("line %s", l)
		}
	}
}

func TestPipeFile(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "iot.csv")
	spec := filepath.Join(dir, "p.yaml")
	writeScript(t, spec, 0o644, `name: iot-sample
from:
  - gen:iot?seed=1
to:
  - file:`+out+`
where: value.battery < 50
max: 25
`)
	got, _, err := runPipe("", "pipe", "-f", spec, "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	var st pipeStatsJSON
	if err := json.Unmarshal([]byte(got), &st); err != nil {
		t.Fatal(err)
	}
	if st.Name != "iot-sample" || st.Written != 25 {
		t.Errorf("stats %+v", st)
	}
	b, _ := os.ReadFile(out)
	if n := strings.Count(string(b), "\n"); n != 26 || !strings.HasPrefix(string(b), "key,value,timestamp\n") {
		t.Errorf("%d lines:\n%s", n, b)
	}

	if err := os.WriteFile(spec, []byte("form: [stdin]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runPipe("", "pipe", "-f", spec); err == nil || !strings.Contains(err.Error(), "form") {
		t.Errorf("a misspelt key: %v", err)
	}
}

func TestPipeUsageErrors(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"pipe", "--to", "stdout"}, "--from"},
		{[]string{"pipe", "--from", "stdin"}, "--to"},
		{[]string{"pipe", "--from", "stdin", "--to", "stdout", "--where", "value.n >"}, "--where"},
		{[]string{"pipe", "--from", "stdin", "--to", "stdout", "-o", "json"}, "stdout is a sink"},
		{[]string{"pipe", "--from", "nosuch:x", "--to", "stdout"}, "kaas-source-nosuch"},
		{[]string{"pipe", "--from", "stdin", "--to", "gen:orders"}, "only be read"},
		{[]string{"pipe", "--from", "stdin", "--to", "stdout", "--max", "-1"}, "--max"},
	} {
		_, _, err := runPipe("", c.args...)
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != exitUsage || !strings.Contains(ce.Message+strings.Join(ce.Hints, " "), strings.TrimPrefix(c.want, "--")) {
			t.Errorf("%v: %v", c.args, err)
		}
	}
}

func TestPipePlugins(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the plugin is a shell script")
	}
	dir := t.TempDir()
	writeScript(t, filepath.Join(dir, "kaas-sink-upper"), 0o755, `#!/bin/sh
while IFS= read -r line; do
  seq=$(echo "$line" | sed 's/.*"seq":\([0-9]*\).*/\1/')
  echo "$line" | tr a-z A-Z >&2
  echo "{\"ack\":$seq}"
done
`)
	out, _, err := runPipe("", "pipe", "plugins", "--plugin-dir", dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "upper") || !strings.Contains(out, "kafka ") || !strings.Contains(out, dir) {
		t.Errorf("plugins:\n%s", out)
	}
	_, errOut, err := runPipe("hi\nthere\n", "pipe", "--from", "stdin?format=text", "--to", "upper:x", "--plugin-dir", dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errOut, `"VALUE":"THERE"`) {
		t.Errorf("the plugin's stderr: %s", errOut)
	}
}

func TestPipeScreen(t *testing.T) {
	src := &memSrc{}
	p := pipe.New([]pipe.Endpoint{{Spec: "kafka:orders?group=etl", Source: src}}, []pipe.Endpoint{{Spec: "file:lake.jsonl", Sink: discard{}}}, pipe.Options{Max: 1000})
	v := &pipeView{name: "orders-to-lake", p: p, where: "value.amount > 100", max: 1000, sinks: 1}
	k := kit.Kit{}
	if err := p.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	v.screen(p.Stats(), at.Add(-time.Second))
	out := kit.Join(v.screen(p.Stats(), at).Render(k, 96))
	for _, want := range []string{"kafka:orders", "3 read", "✓ done", "where value.amoun", "write ×1 ≤ 1,000", "written 3", "orders-to-lake"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q:\n%s", want, out)
		}
	}
	if rateText(4100) != "4.1k/s" || rateText(25000) != "25k/s" || clock(3725*time.Second) != "1:02:05" {
		t.Error("rate or clock text")
	}
}

type memSrc struct{ done bool }

func (s *memSrc) Next(ctx context.Context) (pipe.Batch, error) {
	if s.done {
		return pipe.Batch{}, io.EOF
	}
	s.done = true
	return pipe.Batch{Records: []pipe.Record{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}}}, nil
}
func (s *memSrc) Close() error { return nil }

type discard struct{}

func (discard) Write(context.Context, []pipe.Record) error { return nil }
func (discard) Close() error                               { return nil }

// Replaying production records: a sample taken with kaas consume -o json
// goes back into another cluster's topic through kaas pipe, keys, values,
// binary values and headers intact, so a test environment sees real
// traffic, not made-up data.
func TestPipeReplaysConsumeSample(t *testing.T) {
	prod, _ := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(3, "orders"))
	staging, _ := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(3, "orders"))
	pb, sb := strings.Join(prod.ListenAddrs(), ","), strings.Join(staging.ListenAddrs(), ",")

	cl := kafkatest.Client(t, prod.ListenAddrs()...)
	var want []*kgo.Record
	for i := range 120 {
		r := &kgo.Record{Topic: "orders", Key: []byte(fmt.Sprint("c-", i%7)), Value: []byte(fmt.Sprintf(`{"n":%d}`, i)),
			Headers: []kgo.RecordHeader{{Key: "trace", Value: []byte(fmt.Sprint("t", i))}}}
		if i%40 == 0 {
			r.Value = []byte{0xff, 0x00, byte(i)} // a binary value goes as base64
		}
		want = append(want, r)
	}
	if err := cl.ProduceSync(t.Context(), want...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	sample := filepath.Join(t.TempDir(), "sample.jsonl")
	out, _, err := runPipe("", "consume", "orders", "-b", pb, "--from", "beginning", "--exit", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	writeScript(t, sample, 0o644, out)
	if _, _, err := runPipe("", "pipe", "-b", sb, "--from", "file:"+sample, "--to", "kafka:orders"); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, staging.ListenAddrs(), "orders", 120)
	key := func(r *kgo.Record) string {
		return fmt.Sprintf("%s|%x|%s", r.Key, r.Value, r.Headers[0].Value)
	}
	seen := map[string]bool{}
	for _, r := range got {
		seen[key(r)] = true
	}
	for _, r := range want {
		if !seen[key(r)] {
			t.Fatalf("%s didn't replay", key(r))
		}
	}
}
