package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// listed is whether the cluster lists each name, through a fresh client:
// kgo caches metadata for a few seconds.
func listed(t *testing.T, c *kfake.Cluster, names ...string) map[string]bool {
	t.Helper()
	td, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).ListTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, n := range names {
		out[n] = td.Has(n)
	}
	return out
}

func TestTopicsDeleteCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(2, "a", "b", "c", "d", "__kaas_internal"))
	brokers := strings.Join(c.ListenAddrs(), ",")

	if got := runKaas(t, "topics", "delete", "-b", brokers, "a", "--yes"); got != "deleted a\n" || listed(t, c, "a")["a"] {
		t.Errorf("--yes printed %q", got)
	}
	var js []deleteJSON
	if err := json.Unmarshal([]byte(runKaas(t, "topics", "delete", "-b", brokers, "c", "b", "c", "--yes", "-o", "json")), &js); err != nil {
		t.Fatal(err)
	}
	if len(js) != 2 || js[0].Topic != "b" || !js[0].Deleted || !js[1].Deleted {
		t.Errorf("json = %+v, want b then c, both deleted, once each", js)
	}

	// The typed name confirms; anything else leaves the topic.
	real := answerable
	answerable = func(io.Reader) bool { return true }
	t.Cleanup(func() { answerable = real })
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader("yes\n"))
	root.SetArgs([]string{"topics", "delete", "-b", brokers, "d"})
	var ce *cliError
	if err := root.Execute(); !errors.As(err, &ce) || ce.Code != "cancelled" || ce.Exit != exitFailure || !listed(t, c, "d")["d"] {
		t.Errorf("answering yes instead of the name: err %v, d listed %v", err, listed(t, c, "d")["d"])
	}
	if got := runKaasIn(t, "d\n", "topics", "delete", "-b", brokers, "d"); got != "deleted d\n" || listed(t, c, "d")["d"] {
		t.Errorf("typing the name printed %q", got)
	}
	answerable = real

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"keep"}, exitUsage},                     // no terminal, no --yes
		{[]string{"nope", "--yes"}, exitNotFound},         // unknown
		{[]string{"__kaas_internal", "--yes"}, exitUsage}, // internal
		{nil, exitUsage},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetIn(strings.NewReader(""))
		root.SetArgs(append([]string{"topics", "delete", "-b", brokers}, tc.args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas topics delete %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
	if !listed(t, c, "__kaas_internal")["__kaas_internal"] {
		t.Error("an internal topic was deleted")
	}
}

// driveDelete sends msgs, running the commands they return. Ticks are
// dropped so a test controls time with now and explicit deleteTicks.
func driveDelete(t *testing.T, m deleteModel, msgs ...tea.Msg) deleteModel {
	t.Helper()
	var run func(tea.Cmd)
	run = func(cmd tea.Cmd) {
		if cmd == nil {
			return
		}
		switch out := cmd().(type) {
		case tea.BatchMsg:
			for _, c := range out {
				run(c)
			}
		case deleteTick, tea.QuitMsg:
		default:
			next, more := m.Update(out)
			m = next.(deleteModel)
			run(more)
		}
	}
	for _, msg := range msgs {
		next, cmd := m.Update(msg)
		m = next.(deleteModel)
		run(cmd)
	}
	return m
}

func TestDeleteDialog(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders", "audit"))
	th, _ := theme.Get(theme.Default)
	start := time.Now()
	clock := start
	open := func(names ...string) deleteModel {
		d, err := gatherDeletion(context.Background(), cl, names)
		if err != nil {
			t.Fatal(err)
		}
		m := deleteModel{d: d, k: kit.New(th, false), now: func() time.Time { return clock }}
		return driveDelete(t, m, tea.WindowSizeMsg{Width: 100, Height: 34})
	}
	view := func(m deleteModel) string { return ansi.Strip(m.View().Content) }

	m := open("orders")
	if v := view(m); !strings.Contains(v, "Delete topic?") || !strings.Contains(v, "Type orders to confirm") || !strings.Contains(v, "Topic: orders") {
		t.Fatalf("dialog over the dimmed topic:\n%s", v)
	}
	// A wrong name doesn't arm it; enter goes back to the input.
	m = driveDelete(t, m, typeKeys("order")...)
	m = driveDelete(t, m, tea.KeyPressMsg{Code: tea.KeyTab}, tea.KeyPressMsg{Code: tea.KeyTab}, tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.phase != phaseConfirm || m.focus != screens.FocusInput {
		t.Fatalf("a wrong name: phase %d focus %d", m.phase, m.focus)
	}
	// The name arms it; undo in the window leaves the topic.
	m = driveDelete(t, m, typeKeys("s\n")...)
	if m.phase != phaseGrace || !strings.Contains(view(m), "Deleting orders in 5s") {
		t.Fatalf("after the name: phase %d\n%s", m.phase, view(m))
	}
	m = driveDelete(t, m, tea.KeyPressMsg{Code: 'u', Text: "u"})
	if !m.closed || m.phase != phaseGrace || !listed(t, c, "orders")["orders"] {
		t.Fatalf("undo: closed %v phase %d", m.closed, m.phase)
	}

	// Again, letting the window run out: the delete goes and the lid shuts.
	m = open("orders", "audit")
	m = driveDelete(t, m, typeKeys("delete 2 topics\n")...)
	clock = start.Add(3 * time.Second)
	if m = driveDelete(t, m, deleteTick{}); m.phase != phaseGrace || !strings.Contains(view(m), "in 2s") {
		t.Fatalf("3s in: phase %d\n%s", m.phase, view(m))
	}
	clock = start.Add(undoWindow + time.Second)
	m = driveDelete(t, m, deleteTick{})
	if v := view(m); m.phase != phaseDone || !strings.Contains(v, "2 topics deleted") || m.d.outcome() != nil {
		t.Fatalf("after the window: phase %d err %v\n%s", m.phase, m.d.outcome(), v)
	}
	if l := listed(t, c, "orders", "audit"); l["orders"] || l["audit"] {
		t.Errorf("still listed: %v", l)
	}
}

func TestDeleteDialogEnterSkipsTheWait(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	th, _ := theme.Get(theme.Default)
	d, err := gatherDeletion(context.Background(), cl, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	m := driveDelete(t, deleteModel{d: d, k: kit.New(th, false), now: time.Now}, typeKeys("orders\n\n")...)
	if m.phase != phaseDone || listed(t, c, "orders")["orders"] {
		t.Errorf("enter in the window: phase %d", m.phase)
	}
}
