package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/configs"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// editKeys types keys into the editor, running each command it returns
// (but not the cursor's blink) until it settles. A key is a name such as
// esc or enter, or text typed a rune at a time.
func editKeys(t *testing.T, e configEditor, keys ...string) configEditor {
	t.Helper()
	feed := func(msg tea.Msg) {
		next, cmd := e.Update(msg)
		e = next.(configEditor)
		for cmd != nil {
			out := cmd()
			if _, blink := out.(editBlink); blink {
				break
			}
			if _, quit := out.(tea.QuitMsg); quit {
				break
			}
			next, cmd = e.Update(out)
			e = next.(configEditor)
		}
	}
	for _, k := range keys {
		switch k {
		case "esc":
			feed(tea.KeyPressMsg{Code: tea.KeyEscape})
		case "enter":
			feed(tea.KeyPressMsg{Code: tea.KeyEnter})
		case "backspace":
			feed(tea.KeyPressMsg{Code: tea.KeyBackspace})
		default:
			for _, r := range k {
				feed(tea.KeyPressMsg{Code: r, Text: string(r)})
			}
		}
	}
	return e
}

func entryAt(t *testing.T, cl *kgo.Client, key string) configs.Entry {
	t.Helper()
	es, err := configs.Describe(context.Background(), cl, "orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range es {
		if e.Key == key {
			return e
		}
	}
	t.Fatalf("no %s", key)
	return configs.Entry{}
}

func TestConfigEditor(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	ctx := context.Background()
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	v := configView{topic: "orders"}
	cmd := newConfigEditCmd(new(string), new(string))
	cmd.SetContext(ctx)
	if err := v.load(ctx, cmd, cl); err != nil {
		t.Fatal(err)
	}
	e := newConfigEditor(v, cl, k)
	e.w, e.h = 100, 30
	if !strings.Contains(ansi.Strip(e.View().Content), "NORMAL") {
		t.Fatalf("opened:\n%s", e.View().Content)
	}

	// Set retention to 3 days on a new line, :w, see it, y.
	e = editKeys(t, e, "G", "o", "retention.ms = 3d", "esc")
	if !e.dirty() || e.mode != screens.ModeNormal {
		t.Fatalf("after typing: dirty %v, mode %s", e.dirty(), e.mode)
	}
	e = editKeys(t, e, ":w", "enter")
	view := ansi.Strip(e.View().Content)
	if len(e.changes) != 1 || !strings.Contains(view, "~ retention.ms  7d → 3d") || !strings.Contains(view, "apply them?") {
		t.Fatalf("diff %+v:\n%s", e.changes, view)
	}
	if got := entryAt(t, cl, "retention.ms"); got.Level == configs.LevelTopic {
		t.Fatal("applied before y")
	}
	e = editKeys(t, e, "y")
	if got := entryAt(t, cl, "retention.ms"); got.Level != configs.LevelTopic || got.Value != "259200000" {
		t.Fatalf("after y: %+v", got)
	}
	if e.dirty() || !strings.Contains(e.msg, "1 change applied to orders") {
		t.Errorf("after apply: dirty %v, %q", e.dirty(), e.msg)
	}

	// Mistakes stay in the editor, with the cursor on the line.
	e = editKeys(t, e, "G", "o", "oops", "esc", ":w", "enter")
	if !strings.Contains(e.msg, "want key = value") || e.b.line != len(e.b.lines)-1 {
		t.Errorf("bad line: %q at %d", e.msg, e.b.line)
	}
	e = editKeys(t, e, "cc", "retentoin.bytes = 1GiB", "esc", ":w", "enter")
	if !strings.Contains(e.msg, "did you mean retention.bytes?") {
		t.Errorf("unknown key: %q", e.msg)
	}
	e = editKeys(t, e, ":q", "enter")
	if e.quit || !strings.Contains(e.msg, ":q! leaves") {
		t.Errorf(":q with changes: quit %v, %q", e.quit, e.msg)
	}
	e = editKeys(t, e, "u", "u", "u")
	if e.dirty() {
		t.Errorf("undo left changes:\n%s", e.b.text())
	}
	// n keeps editing; :%s changes text everywhere it is.
	e = editKeys(t, e, ":%s/259200000/2d/g", "enter")
	if !strings.Contains(e.msg, "1 substitution on 1 line") {
		t.Errorf("substitute: %q", e.msg)
	}
	e = editKeys(t, e, ":w", "enter", "n")
	if e.changes != nil || !strings.Contains(e.msg, "not applied") {
		t.Errorf("n: %+v %q", e.changes, e.msg)
	}

	// Delete the line to unset it, then :wq.
	for i, l := range e.b.lines {
		if strings.HasPrefix(l, "retention.ms") {
			e.b.line = i
		}
	}
	e = editKeys(t, e, "dd", ":wq", "enter")
	if e.changes == nil || e.changes[0].Op != configs.OpUnset {
		t.Fatalf("unset diff %+v, %q", e.changes, e.msg)
	}
	e = editKeys(t, e, "y")
	if !e.quit || entryAt(t, cl, "retention.ms").Level != configs.LevelDefault {
		t.Errorf("after :wq: quit %v, %+v", e.quit, entryAt(t, cl, "retention.ms"))
	}

	// Without the editor.
	brokers := strings.Join(c.ListenAddrs(), ",")
	out := runKaas(t, "config", "edit", "orders", "--set", "retention.ms=2d", "--set", "cleanup.policy=compact", "--dry-run", "-b", brokers)
	if !strings.Contains(out, "~ retention.ms 7d → 2d") || !strings.Contains(out, "nothing applied") || entryAt(t, cl, "retention.ms").Level != configs.LevelDefault {
		t.Errorf("dry run:\n%s", out)
	}
	var js struct {
		Applied bool               `json:"applied"`
		Changes []configChangeJSON `json:"changes"`
	}
	if err := json.Unmarshal([]byte(runKaas(t, "config", "edit", "orders", "--set", "retention.ms=2d", "-b", brokers, "-o", "json")), &js); err != nil {
		t.Fatal(err)
	}
	if !js.Applied || len(js.Changes) != 1 || *js.Changes[0].To != "172800000" || entryAt(t, cl, "retention.ms").Value != "172800000" {
		t.Errorf("json %+v", js)
	}
	if out := runKaas(t, "config", "edit", "orders", "--unset", "retention.ms", "-b", brokers); !strings.Contains(out, "(unset)") || !strings.Contains(out, "1 change applied") {
		t.Errorf("unset:\n%s", out)
	}
	if out := runKaas(t, "config", "edit", "orders", "--unset", "retention.ms", "-b", brokers); !strings.Contains(out, "nothing to change") {
		t.Errorf("unset again:\n%s", out)
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"orders"}, exitUsage}, // no terminal for the editor
		{[]string{"orders", "--set", "retention.ms"}, exitUsage},
		{[]string{"orders", "--set", "retention.ms=1d", "--unset", "retention.ms"}, exitUsage},
		{[]string{"orders", "--set", "retentoin.ms=1d"}, exitUsage},
		{[]string{"orders", "--set", "retention.ms=soon"}, exitUsage},
		{[]string{"--broker", "9", "--set", "x=1"}, exitUsage},
		{[]string{"ordrs", "--set", "retention.ms=1d"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(append([]string{"config", "edit"}, tc.args...), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas config edit %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}

func TestWordMotions(t *testing.T) {
	l := "retention.ms = 3d  # note"
	if got := wordStart(l, 0, 1); got != 9 {
		t.Errorf("w from 0: %d", got)
	}
	if got := wordStart(l, 9, 1); got != 10 {
		t.Errorf("w from .: %d", got)
	}
	if got := wordStart(l, 15, -1); got != 13 {
		t.Errorf("b from 3d: %d", got)
	}
	if got := wordEnd(l, 15); got != 17 {
		t.Errorf("cw end: %d", got)
	}
}
