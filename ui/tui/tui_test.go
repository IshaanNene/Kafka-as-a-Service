package tui

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

var update = flag.Bool("update", false, "rewrite golden files")

var t0 = time.Date(2026, 10, 31, 9, 30, 0, 0, time.UTC)

func fixture() *snapshot.Snapshot {
	r := []int32{1, 2, 3}
	part := func(id, leader int32, isr []int32, start, end int64) snapshot.Partition {
		return snapshot.Partition{ID: id, Leader: leader, Replicas: r, ISR: isr, StartOffset: start, EndOffset: end}
	}
	return &snapshot.Snapshot{
		Generation: 42, TakenAt: t0.Add(-2 * time.Second), ClusterID: "v0kwmoCzR2ORcHrtg41t-g", Controller: 2,
		Brokers: []snapshot.Broker{{ID: 1, Host: "dev-dual-0.kafka", Port: 9092, Rack: "a"}, {ID: 2, Host: "dev-dual-1.kafka", Port: 9092, Rack: "b"}, {ID: 3, Host: "dev-dual-2.kafka", Port: 9092, Rack: "c"}},
		Topics: []snapshot.Topic{
			{Name: "__consumer_offsets", Internal: true, Partitions: []snapshot.Partition{part(0, 1, r, 0, 10)}},
			{Name: "orders", OffsetsAt: t0.Add(-time.Second), Partitions: []snapshot.Partition{
				part(0, 1, r, 0, 120_000), part(1, 2, []int32{2, 3}, 100, 98_000), part(2, -1, nil, 0, 0)}},
			{Name: "orders-v2", OffsetsAt: t0.Add(-4 * time.Second), Partitions: []snapshot.Partition{part(0, 3, r, 0, 12)}},
			{Name: "payments", Partitions: []snapshot.Partition{part(0, 2, r, -1, -1)}},
		},
	}
}

// model returns a TUI at 100×30 showing the fixture, with motion frozen.
func model(t *testing.T) Model {
	t.Helper()
	th, err := theme.Get("amber")
	if err != nil {
		t.Fatal(err)
	}
	m := newModel(th, Options{ReducedMotion: true})
	m.now = t0
	m = send(m, tea.WindowSizeMsg{Width: 100, Height: 30})
	s := fixture()
	return send(m, updateMsg{Snapshot: s, Events: []engine.Event{
		{Kind: engine.LeaderMoved, Topic: "orders", Partition: 1, From: 1, To: 2},
		{Kind: engine.ISRShrunk, Topic: "orders", Partition: 1, Replicas: []int32{1}},
	}})
}

func send(m Model, msgs ...tea.Msg) Model {
	for _, msg := range msgs {
		next, _ := m.Update(msg)
		m = next.(Model)
	}
	return m
}

func keys(s ...string) []tea.Msg {
	var out []tea.Msg
	for _, k := range s {
		switch k {
		case "enter":
			out = append(out, tea.KeyPressMsg{Code: tea.KeyEnter})
		case "esc":
			out = append(out, tea.KeyPressMsg{Code: tea.KeyEscape})
		default:
			for _, r := range k {
				out = append(out, tea.KeyPressMsg{Code: r, Text: string(r)})
			}
		}
	}
	return out
}

func plain(m Model) string {
	lines := strings.Split(ansi.Strip(m.View().Content), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " ")
	}
	return strings.Join(lines, "\n") + "\n"
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	p := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.WriteFile(p, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("%v (run go test ./ui/tui -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs from golden; rerun with -update if intended\n--- got ---\n%s", name, got)
	}
}

func TestGoldenScreens(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{"topics", nil},
		{"topics_filter", []string{"/", "v2"}},
		{"topic_detail", []string{"enter"}},
		{"brokers", []string{":", "brokers", "enter"}},
		{"not_found_command", []string{":", "brokrs", "enter"}},
		{"not_found_topic", []string{":", "topic oders", "enter"}},
		{"help", []string{"?"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := send(model(t), keys(tt.keys...)...)
			golden(t, tt.name, plain(m))
		})
	}
}

func TestNavigation(t *testing.T) {
	m := model(t)
	if m.screen() != screenTopics {
		t.Fatal("should start on topics")
	}
	m = send(m, keys("j", "enter")...)
	if m.screen() != screenTopic || m.topic != "orders-v2" {
		t.Fatalf("screen=%v topic=%q, want orders-v2 detail", m.screen(), m.topic)
	}
	m = send(m, keys("esc")...)
	if m.screen() != screenTopics {
		t.Fatal("esc should go back to topics")
	}
	m = send(m, keys(":", "payments", "enter")...)
	if m.screen() != screenTopic || m.topic != "payments" {
		t.Fatal(":<topic> should open the topic")
	}
	m = send(m, keys(":", "nope-nope", "enter", "esc")...)
	if m.screen() != screenTopic {
		t.Fatal("esc from 404 should return where we were")
	}
}

func TestFilterHidesInternalUnlessAsked(t *testing.T) {
	m := model(t)
	for _, tp := range m.visibleTopics() {
		if tp.Internal {
			t.Fatal("internal topic shown without asking")
		}
	}
	m = send(m, keys("/", "__")...)
	if ts := m.visibleTopics(); len(ts) != 1 || ts[0].Name != "__consumer_offsets" {
		t.Fatalf("filter __ = %v", ts)
	}
}

func TestSuggest(t *testing.T) {
	got := suggest("oders", []string{"orders", "orders-v2", "payments", "topics"}, 3)
	if len(got) == 0 || got[0] != "orders" {
		t.Errorf("suggest = %v, want orders first", got)
	}
	if got := suggest("zzzzzz", []string{"orders"}, 3); len(got) != 0 {
		t.Errorf("suggest far = %v, want none", got)
	}
}

// TestEndToEnd drives the real engine on kfake through the model's own
// update command, the same path a terminal session takes.
func TestEndToEnd(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	e := engine.New(cl, engine.Config{MetadataInterval: 50 * time.Millisecond, FastInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	th, _ := theme.Get("amber")
	m := New(e, th, Options{ReducedMotion: true})
	m = send(m, tea.WindowSizeMsg{Width: 100, Height: 30})
	next := func() {
		t.Helper()
		done := make(chan tea.Msg, 1)
		go func() { done <- m.waitUpdate()() }()
		select {
		case msg := <-done:
			m = send(m, msg)
		case <-time.After(5 * time.Second):
			t.Fatal("no update from engine")
		}
	}
	next()
	if !strings.Contains(plain(m), "orders") {
		t.Fatalf("orders not shown:\n%s", plain(m))
	}
	if err := c.CreateTopic("payments", 2, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(plain(m), "TopicCreated payments") {
		if time.Now().After(deadline) {
			t.Fatalf("TopicCreated never reached the feed:\n%s", plain(m))
		}
		next()
	}
}

// TestFitsTerminal renders every screen at common and cramped sizes and
// checks nothing spills past the terminal's width or height.
func TestFitsTerminal(t *testing.T) {
	screens := [][]string{nil, {"enter"}, {":", "brokers", "enter"}, {":", "oders", "enter"}, {"?"}, {"/", "ord"}}
	for _, size := range [][2]int{{80, 24}, {60, 20}, {120, 40}, {200, 60}} {
		for _, k := range screens {
			m := model(t)
			m = send(m, tea.WindowSizeMsg{Width: size[0], Height: size[1]})
			m = send(m, keys(k...)...)
			lines := strings.Split(m.View().Content, "\n")
			if len(lines) > size[1] {
				t.Errorf("%dx%d %v: %d lines", size[0], size[1], k, len(lines))
			}
			for i, l := range lines {
				if w := ansi.StringWidth(l); w > size[0] {
					t.Errorf("%dx%d %v: line %d is %d wide", size[0], size[1], k, i, w)
					break
				}
			}
		}
	}
}
