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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestTopicsCreateCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(1, "payments.eu"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	describe := func(name string) (topics.Details, int, bool) {
		// A fresh client: kgo caches metadata for a few seconds.
		adm := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...))
		td, err := adm.ListTopics(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		d, _ := topics.Describe(context.Background(), adm, []string{name})
		return d[name], len(td[name].Partitions), td.Has(name)
	}

	if got := runKaas(t, "topics", "create", "-b", brokers, "orders", "-p", "6", "-r", "2", "--dry-run"); got != "The controller accepts orders: 6 partitions · RF 2. Nothing was created.\n" {
		t.Errorf("dry run printed %q", got)
	}
	if _, _, ok := describe("orders"); ok {
		t.Fatal("a dry run created the topic")
	}

	got := runKaas(t, "topics", "create", "-b", brokers, "orders", "-p", "6", "-r", "2", "-c", "cleanup.policy=compact", "-c", "retention.ms=-1")
	if got != "Created orders: 6 partitions · RF 2 · cleanup.policy=compact · retention.ms=-1.\n" {
		t.Errorf("create printed %q", got)
	}
	if d, n, ok := describe("orders"); !ok || n != 6 || d.Cleanup != "compact" || d.Retention != topics.Forever {
		t.Errorf("created %d partitions, details %+v", n, d)
	}

	// The broker's defaults when the counts are left out; JSON for scripts.
	var j createJSON
	if err := json.Unmarshal([]byte(runKaas(t, "topics", "create", "-b", brokers, "audit", "-o", "json")), &j); err != nil {
		t.Fatal(err)
	}
	if !j.Created || j.Partitions < 1 || j.ReplicationFactor != 3 || len(j.ID) != 22 || j.DryRun {
		t.Errorf("json = %+v", j)
	}
	if got := runKaas(t, "topics", "create", "-b", brokers, "orders", "-p", "6", "--if-not-exists", "-o", "json"); !strings.Contains(got, `"created":false`) {
		t.Errorf("--if-not-exists on an existing topic printed %q", got)
	}

	for _, tc := range []struct {
		args []string
		exit int
		code string
	}{
		{[]string{"orders", "-p", "3"}, exitFailure, "exists"},
		{[]string{"payments_eu", "-p", "3"}, exitUsage, "usage"}, // collides with payments.eu
		{[]string{"has space", "-p", "3"}, exitUsage, "usage"},
		{[]string{"-p", "3"}, exitUsage, "usage"}, // no name and no terminal
		{[]string{"x", "-c", "novalue"}, exitUsage, "usage"},
		{[]string{"x", "-p", "-2"}, exitUsage, "usage"},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetIn(strings.NewReader(""))
		root.SetArgs(append([]string{"topics", "create", "-b", brokers}, tc.args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit || ce.Code != tc.code {
			t.Errorf("kaas topics create %v: %v, want %s exit %d", tc.args, err, tc.code, tc.exit)
		}
	}
	// The controller's refusal comes back as it is.
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"topics", "create", "-b", brokers, "wide", "-p", "1", "-r", "5"})
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "INVALID_REPLICATION_FACTOR") {
		t.Errorf("RF 5 on 3 brokers: %v", err)
	}
}

// drive sends msgs to the wizard, running the commands they return the
// way Bubble Tea would, until one quits or none is left.
func drive(t *testing.T, m wizardModel, msgs ...tea.Msg) wizardModel {
	t.Helper()
	for _, msg := range msgs {
		next, cmd := m.Update(msg)
		m = next.(wizardModel)
		for cmd != nil {
			out := cmd()
			if _, ok := out.(tea.QuitMsg); ok {
				break
			}
			next, cmd = m.Update(out)
			m = next.(wizardModel)
		}
	}
	return m
}

func keys(names ...string) []tea.Msg {
	codes := map[string]rune{"enter": tea.KeyEnter, "tab": tea.KeyTab, "esc": tea.KeyEscape, "left": tea.KeyLeft,
		"right": tea.KeyRight, "down": tea.KeyDown, "up": tea.KeyUp, "backspace": tea.KeyBackspace}
	var out []tea.Msg
	for _, n := range names {
		out = append(out, tea.KeyPressMsg{Code: codes[n]})
	}
	return out
}

func TestCreateWizard(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(1, "payments.eu.charges"))
	ctx := context.Background()
	s, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	th, _ := theme.Get(theme.Default)
	wcl := kafkatest.Client(t, c.ListenAddrs()...)
	m := newWizard(creation{cl: wcl, adm: kadm.NewClient(wcl), snap: s, spec: topics.Spec{Configs: map[string]string{"segment.ms": "3600000"}}}, kit.New(th, false))
	m = drive(t, m, tea.WindowSizeMsg{Width: 80, Height: 30})
	view := func() string { return ansi.Strip(m.View().Content) }

	// Name: checked as typed, with its family.
	m = drive(t, m, typeKeys("payments_eu.charges")...)
	m = drive(t, m, keys("enter")...)
	if m.step != stepName || !strings.Contains(view(), "collides with payments.eu.charges") {
		t.Fatalf("a colliding name should stay on step 1:\n%s", view())
	}
	m = drive(t, m, keys("backspace", "backspace", "backspace", "backspace", "backspace", "backspace", "backspace")...)
	m = drive(t, m, typeKeys("refunds")...)
	m.name = "payments.eu.refunds"
	if v := view(); !strings.Contains(v, "payments › eu › refunds") || !strings.Contains(v, "1 topic shares payments.eu") {
		t.Errorf("family:\n%s", v)
	}

	// Layout: the controller's defaults fill in, then RF drops to 2.
	m = drive(t, m, keys("enter")...)
	if m.step != stepLayout || m.parts == "" || m.rfs[m.rf] != 3 {
		t.Fatalf("after the name: step %d parts %q rf %d, want the broker defaults", m.step, m.parts, m.rfs[m.rf])
	}
	m = drive(t, m, keys("backspace", "backspace", "backspace")...)
	m = drive(t, m, typeKeys("4")...)
	m = drive(t, m, keys("tab", "left", "enter")...)
	if m.step != stepConfigs || m.parts != "4" || m.rfs[m.rf] != 2 {
		t.Fatalf("layout: step %d parts %q rf %d", m.step, m.parts, m.rfs[m.rf])
	}
	if v := view(); !strings.Contains(v, "also set: segment.ms=3600000") || !strings.Contains(v, "‹ default ›") {
		t.Errorf("configs:\n%s", v)
	}

	// Configs: retention 1d, cleanup compact.
	m = drive(t, m, keys("right", "right", "down", "right", "right")...)
	m = drive(t, m, keys("enter")...)
	if v := view(); m.step != stepCreate || !m.checked || !strings.Contains(v, "✓ the controller accepts this") ||
		!strings.Contains(v, "cleanup.policy=compact") || !strings.Contains(v, "retention.ms=86400000") || !strings.Contains(v, "segment.ms=3600000") {
		t.Fatalf("review: step %d checked %v\n%s", m.step, m.checked, v)
	}

	// Back and forth keeps what was chosen.
	m = drive(t, m, keys("esc", "esc")...)
	if m.step != stepLayout || m.parts != "4" {
		t.Errorf("back to layout: step %d parts %q", m.step, m.parts)
	}
	m = drive(t, m, keys("enter", "enter")...)

	m = drive(t, m, keys("enter")...)
	if !m.finished || !strings.Contains(view(), "payments.eu.refunds is ready") || !strings.Contains(view(), "Step 4 of 4") {
		t.Fatalf("finish: finished %v\n%s", m.finished, view())
	}
	d, err := topics.Describe(ctx, kadm.NewClient(cl), []string{"payments.eu.refunds"})
	if err != nil || d["payments.eu.refunds"].Cleanup != "compact" || d["payments.eu.refunds"].Set["segment.ms"] != "3600000" {
		t.Errorf("created topic details %+v, %v", d, err)
	}
	if m.c.made.Partitions != 4 || m.c.made.ReplicationFactor != 2 {
		t.Errorf("made %+v, want 4 partitions at RF 2", m.c.made)
	}
}

func TestCreateWizardQuit(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	s, err := snapshot.Fetch(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	th, _ := theme.Get(theme.Default)
	wcl := kafkatest.Client(t, c.ListenAddrs()...)
	m := newWizard(creation{cl: wcl, adm: kadm.NewClient(wcl), snap: s}, kit.New(th, false))
	m = drive(t, m, keys("esc")...)
	if !m.quit || m.finished || m.View().Content != "" {
		t.Errorf("esc on the first step should quit and leave nothing: quit %v view %q", m.quit, m.View().Content)
	}
}
