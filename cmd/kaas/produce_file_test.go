package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestProduceFiles(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	dir := t.TempDir()
	var big strings.Builder
	for range 2500 { // more than one batch
		big.WriteString("k:{\"n\":1}\n")
	}
	a := writeFile(t, dir, "a.txt", big.String())
	b := writeFile(t, dir, "b.txt", "x:1\r\ny:2\n")
	records := func() int64 {
		ends, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).ListEndOffsets(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		var n int64
		ends.Each(func(o kadm.ListedOffset) { n += o.Offset })
		return n
	}

	got := runKaas(t, "produce", "orders", "-b", brokers, "--key-separator", ":", "-f", a, "-f", b)
	if got != a+": 2500 records\n"+b+": 2 records\n" || records() != 2502 {
		t.Errorf("plain printed %q; %d records in the topic", got, records())
	}

	// -o json: each record's landing, with its file and line.
	sc := bufio.NewScanner(strings.NewReader(runKaas(t, "produce", "orders", "-b", brokers, "--key-separator", ":", "-f", b, "-o", "json")))
	var lines []resultJSON
	for sc.Scan() {
		var r resultJSON
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, r)
	}
	if len(lines) != 2 || lines[0].File != b || lines[0].Line != 1 || lines[1].Line != 2 {
		t.Errorf("json = %+v", lines)
	}

	// A bad line fails its file; the others still send.
	bad := writeFile(t, dir, "bad.txt", "ok:1\nno separator\n")
	before := records()
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"produce", "orders", "-b", brokers, "--key-separator", ":", "-f", bad, "-f", b})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "bad.txt: line 2") || records() != before+2 {
		t.Errorf("a bad file: err %v; %d records added, want b's 2", err, records()-before)
	}

	for _, args := range [][]string{
		{"-f", filepath.Join(dir, "missing.txt")},
		{"-f", dir},
		{"-f", b, "--value", "x"},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append([]string{"produce", "orders", "-b", brokers}, args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != exitUsage {
			t.Errorf("kaas produce %v: %v, want a usage error", args, err)
		}
	}
}

func TestUploadScreens(t *testing.T) {
	u := &upload{paths: []string{"/tmp/a.jsonl", "/tmp/b.jsonl"}, errs: []error{nil, errors.New("line 3: boom")},
		files: []screens.UploadFile{
			{Name: "a.jsonl", Size: 100, Read: 100, Records: 10, State: screens.Sent},
			{Name: "b.jsonl", Size: 50, Read: 20, Records: 2, State: screens.Failed},
		}}
	p := producer{topic: "orders", partition: -1}
	v := u.screen(p, ":", "5L6g3nSh0000")
	if v.Foot != "1 of 2 sent · 120 B of 150 B · 12 records" || !strings.Contains(v.Hint, `key before ":"`) || v.Right != "cluster 5L6g3nSh" {
		t.Errorf("screen = %+v", v)
	}
	tr := u.tray("orders")
	if !tr.Failed || tr.Title != "1 file failed" || tr.Detail != "b.jsonl line 3: boom" || tr.Count != " 1 of 2 " {
		t.Errorf("tray = %+v", tr)
	}
	if err := u.outcome(); err == nil || err.Error() != "/tmp/b.jsonl: line 3: boom" {
		t.Errorf("outcome = %v", err)
	}
}
