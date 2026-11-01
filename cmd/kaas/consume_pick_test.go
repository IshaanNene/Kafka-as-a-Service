package main

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestConsumeWindow(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	seedTimeline(t, c, "orders", []int32{0}, 100, base, func(_ int32, i int) string { return "v" + strconv.Itoa(i) })
	brokers := strings.Join(c.ListenAddrs(), ",")
	from, to := base.Add(10*time.Second).Format(time.RFC3339), base.Add(15*time.Second).Format(time.RFC3339)

	if got := runKaas(t, "consume", "orders", "-b", brokers, "--from", from, "--to", to); got != "v10\nv11\nv12\nv13\nv14\n" {
		t.Errorf("window printed %q", got)
	}
	for _, tc := range []struct {
		args []string
	}{
		{[]string{"--to", to}},                                                             // no --from
		{[]string{"--from", "-5", "--to", to}},                                             // not a time --from
		{[]string{"--from", to, "--to", from}},                                             // backwards
		{[]string{"--from", from, "--to", "tomorrow"}},                                     // not a time
		{[]string{"--from", from, "--to", time.Now().Add(time.Hour).Format(time.RFC3339)}}, // future
		{[]string{"--pick"}},                                                               // no terminal
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append([]string{"consume", "orders", "-b", brokers}, tc.args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != exitUsage {
			t.Errorf("kaas consume %v: %v, want a usage error", tc.args, err)
		}
	}
}

// seedAt writes a record stamped at each of times to topic's partition 0.
func seedAt(t *testing.T, c *kfake.Cluster, topic string, times []time.Time) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var rs []*kgo.Record
	for _, at := range times {
		rs = append(rs, &kgo.Record{Topic: topic, Partition: 0, Timestamp: at, Value: []byte(at.Format(time.RFC3339))})
	}
	if err := cl.ProduceSync(context.Background(), rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestPickModel(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	now := time.Date(2026, 11, 1, 15, 0, 0, 0, time.Local)
	// Two records a day from Oct 20, none on Oct 25.
	var days []time.Time
	for d := time.Date(2026, 10, 20, 9, 0, 0, 0, time.Local); d.Before(now); d = d.AddDate(0, 0, 1) {
		if d.Day() != 25 {
			days = append(days, d, d.Add(time.Hour))
		}
	}
	seedAt(t, c, "orders", days)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	oldest, err := consume.Oldest(ctx, cl, adm, "orders")
	if err != nil || !oldest.Equal(days[0]) {
		t.Fatalf("oldest = %v, %v", oldest, err)
	}
	th, _ := theme.Get(theme.Default)
	m := newPickModel(kit.New(th, false), adm, "orders", now, oldest, consume.From{})
	drivePick := func(m pickModel, msgs ...tea.Msg) pickModel {
		t.Helper()
		run := func(cmd tea.Cmd) {
			for cmd != nil {
				out := cmd()
				if _, ok := out.(tea.QuitMsg); ok {
					return
				}
				next, more := m.Update(out)
				m, cmd = next.(pickModel), more
			}
		}
		run(m.Init())
		for _, msg := range msgs {
			next, cmd := m.Update(msg)
			m = next.(pickModel)
			run(cmd)
		}
		return m
	}
	m = drivePick(m)
	if m.kind(time.Date(2026, 10, 25, 0, 0, 0, 0, time.Local)) != 1 || m.kind(time.Date(2026, 10, 19, 0, 0, 0, 0, time.Local)) != 2 || m.kind(time.Date(2026, 11, 2, 0, 0, 0, 0, time.Local)) != 2 {
		t.Errorf("day kinds wrong")
	}
	// From today, back a week to Oct 25 for the start, then to Oct 27.
	m = drivePick(m, keys("up", "enter", "right", "right")...)
	if v := ansi.Strip(kit.Join(m.screen(80).Render(m.k, 80))); !strings.Contains(v, "Oct 25") || !strings.Contains(v, "Oct 27") || !strings.Contains(v, " 4 ") {
		t.Errorf("following the cursor: Oct 25 to 27 holds 4 records\n%s", v)
	}
	m = drivePick(m, keys("enter", "enter")...)
	if !m.done {
		t.Fatal("not done")
	}
	from, until := m.window()
	if !from.Equal(time.Date(2026, 10, 25, 0, 0, 0, 0, time.Local)) || !until.Equal(time.Date(2026, 10, 28, 0, 0, 0, 0, time.Local)) {
		t.Errorf("window %v to %v", from, until)
	}
	// A day before the oldest record can't be picked.
	m = newPickModel(kit.New(th, false), adm, "orders", now, oldest, consume.From{})
	m = drivePick(m, keys("up", "up", "up", "enter")...)
	if !m.start.IsZero() || m.problem == "" {
		t.Errorf("picked a day with nothing kept: %v", m.start)
	}
}
