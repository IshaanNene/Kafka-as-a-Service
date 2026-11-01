package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// runKaasIn is runKaas with stdin.
func runKaasIn(t *testing.T, in string, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader(in))
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		t.Fatalf("kaas %v: %v", args, err)
	}
	return out.String()
}

// readAll reads every record of topic.
func readAll(t *testing.T, seeds []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for len(out) < n && ctx.Err() == nil {
		out = append(out, cl.PollFetches(ctx).Records()...)
	}
	return out
}

func TestProduceCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")

	// Piped lines, keyed, with a header, to partition 1, as JSON results.
	out := runKaasIn(t, "user-1:{\"a\":1}\nuser-2:{\"a\":2}\n", "produce", "orders", "-b", brokers,
		"--key-separator", ":", "-H", "source=test", "-p", "1", "-o", "json")
	dec := json.NewDecoder(strings.NewReader(out))
	var res []resultJSON
	for dec.More() {
		var r resultJSON
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		res = append(res, r)
	}
	if len(res) != 2 || res[0].Partition != 1 || res[1].Offset != 1 || res[0].Topic != "orders" {
		t.Errorf("json results = %+v", res)
	}
	// One from --value, silent on a pipe.
	if out := runKaasIn(t, "ignored\n", "produce", "orders", "-b", brokers, "-k", "k", "--value", "v"); out != "" {
		t.Errorf("a pipe should get no output, got %q", out)
	}
	recs := readAll(t, c.ListenAddrs(), "orders", 3)
	if len(recs) != 3 {
		t.Fatalf("read %d records, want 3", len(recs))
	}
	var keyed int
	for _, r := range recs {
		if string(r.Key) == "user-2" && (string(r.Value) != `{"a":2}` || len(r.Headers) != 1 || r.Headers[0].Key != "source") {
			t.Errorf("piped record = %+v", r)
		}
		if string(r.Key) == "k" && string(r.Value) == "v" {
			keyed++
		}
	}
	if keyed != 1 {
		t.Error("the --value record is missing")
	}

	for args, wantExit := range map[string]int{
		"produce ordrs":           exitNotFound,
		"produce orders -p 3":     exitUsage,
		"produce orders -H nokey": exitUsage,
		"produce":                 exitUsage,
		"produce orders -o yaml":  exitUsage,
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetIn(strings.NewReader(""))
		root.SetArgs(append(strings.Fields(args), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != wantExit {
			t.Errorf("kaas %s: %v, want exit %d", args, err, wantExit)
		}
	}
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader("no separator here\n"))
	root.SetArgs([]string{"produce", "orders", "-b", brokers, "--key-separator", ":"})
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Errorf("a line without the separator should fail naming it: %v", err)
	}
}

func typeKeys(s string) []tea.Msg {
	var out []tea.Msg
	for _, r := range s {
		switch r {
		case '\n':
			out = append(out, tea.KeyPressMsg{Code: tea.KeyEnter})
		case '\t':
			out = append(out, tea.KeyPressMsg{Code: tea.KeyTab})
		case '\b':
			out = append(out, tea.KeyPressMsg{Code: tea.KeyBackspace})
		default:
			out = append(out, tea.KeyPressMsg{Code: r, Text: string(r)})
		}
	}
	return out
}

func sendEditor(m editorModel, msgs ...tea.Msg) (editorModel, tea.Cmd) {
	var cmd tea.Cmd
	for _, msg := range msgs {
		var next tea.Model
		next, cmd = m.Update(msg)
		m = next.(editorModel)
	}
	return m, cmd
}

func TestEditorModel(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	cl := kafkatest.Client(t, c.ListenAddrs()...)
	th, _ := theme.Get(theme.Default)
	m := newEditor(producer{cl: cl, topic: "orders", partition: -1}, kit.New(th, false))
	m, _ = sendEditor(m, tea.WindowSizeMsg{Width: 90, Height: 20})

	// { then enter indents; typing a field; } closes.
	m, _ = sendEditor(m, typeKeys("{\n\"a\": ")...)
	view := ansi.Strip(m.View().Content)
	if !strings.Contains(view, "✗ line 2") || m.bufs[tabValue].lines[1] != `  "a": ` {
		t.Errorf("half-typed JSON should show its problem:\n%s\nlines %q", view, m.bufs[tabValue].lines)
	}
	m, cmd := sendEditor(m, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	if cmd != nil || !strings.Contains(ansi.Strip(m.View().Content), "fix value line 2") {
		t.Errorf("ctrl-s with a problem should refuse:\n%s", ansi.Strip(m.View().Content))
	}
	m, _ = sendEditor(m, typeKeys("1\n}\tuser-9\ttrace=x")...)
	if m.active != tabHeaders || m.bufs[tabKey].text() != "user-9" || m.bufs[tabValue].text() != "{\n  \"a\": 1\n}" {
		t.Errorf("buffers: active %d, key %q, value %q", m.active, m.bufs[tabKey].text(), m.bufs[tabValue].text())
	}
	if !strings.Contains(ansi.Strip(m.View().Content), "✓ 1 header") {
		t.Errorf("headers tab:\n%s", ansi.Strip(m.View().Content))
	}

	m, cmd = sendEditor(m, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	if cmd == nil {
		t.Fatal("ctrl-s should send")
	}
	m, _ = sendEditor(m, cmd())
	if len(m.sent) != 1 || m.sent[0] != "sent to orders/0 @ 0" || m.bufs[tabValue].dirty || !strings.Contains(ansi.Strip(m.View().Content), "↑ sent to p0 @ 0") {
		t.Errorf("after send: sent %q, view\n%s", m.sent, ansi.Strip(m.View().Content))
	}
	r := readAll(t, c.ListenAddrs(), "orders", 1)
	if len(r) != 1 || string(r[0].Value) != `{"a":1}` || string(r[0].Key) != "user-9" || string(r[0].Headers[0].Value) != "x" {
		t.Errorf("record = %+v", r)
	}
}

func TestBufferEditing(t *testing.T) {
	b := newBuffer("")
	b.insert("{}")
	b.move(0, -1)
	b.newline()
	if b.text() != "{\n  \n}" || b.line != 1 || b.col != 2 {
		t.Errorf("{|} enter = %q at %d:%d", b.text(), b.line, b.col)
	}
	b.insert("x")
	b.backspace()
	b.backspace()
	b.backspace()
	b.backspace()
	if b.text() != "{\n}" || b.line != 0 || b.col != 1 {
		t.Errorf("backspace across lines = %q at %d:%d", b.text(), b.line, b.col)
	}
	b.del()
	if b.text() != "{}" {
		t.Errorf("delete joins lines: %q", b.text())
	}
	b.insert("a\nb")
	if b.text() != "{a\nb}" || b.line != 1 || b.col != 1 {
		t.Errorf("multi-line paste = %q at %d:%d", b.text(), b.line, b.col)
	}
	b.move(-5, 99)
	if b.line != 0 || b.col != 2 {
		t.Errorf("move clamps: %d:%d", b.line, b.col)
	}
}
