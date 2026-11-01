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
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// seedGroup writes n records to each partition of topic and commits group
// at the given offsets.
func seedGroup(t *testing.T, c *kfake.Cluster, topic string, parts, n int, group string, at map[int32]int64) {
	t.Helper()
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var rs []*kgo.Record
	for p := range int32(parts) {
		for range n {
			rs = append(rs, &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")})
		}
	}
	if err := cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	os := kadm.Offsets{}
	for p, o := range at {
		os.Add(kadm.Offset{Topic: topic, Partition: p, At: o, LeaderEpoch: -1})
	}
	if _, err := kadm.NewClient(cl).CommitOffsets(ctx, group, os); err != nil {
		t.Fatal(err)
	}
}

// committedAt is group's commits on topic, through a fresh client.
func committedAt(t *testing.T, c *kfake.Cluster, group, topic string) map[int32]int64 {
	t.Helper()
	rs, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	for p, r := range rs[topic] {
		out[p] = r.At
	}
	return out
}

func TestOffsetsResetCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(3, "orders", "audit"))
	seedGroup(t, c, "orders", 3, 10, "billing", map[int32]int64{0: 10, 1: 8})
	brokers := strings.Join(c.ListenAddrs(), ",")
	base := []string{"offsets", "reset", "billing", "-b", brokers}

	// A dry run plans and commits nothing.
	var js resetJSON
	if err := json.Unmarshal([]byte(runKaas(t, append(base, "-t", "orders", "--to", "-3", "--dry-run", "-o", "json")...)), &js); err != nil {
		t.Fatal(err)
	}
	if !js.DryRun || js.Applied || len(js.Moves) != 3 || js.Moves[0].Change != "change" || js.Moves[0].To != 7 || js.Moves[2].Change != "skip" {
		t.Errorf("dry run json = %+v", js)
	}
	if got := committedAt(t, c, "billing", "orders"); got[0] != 10 || got[1] != 8 {
		t.Errorf("a dry run committed: %v", got)
	}

	// --yes commits; a partition with no commit gets its first.
	js = resetJSON{}
	if err := json.Unmarshal([]byte(runKaas(t, append(base, "--all-topics", "--to", "earliest", "--yes", "-o", "json")...)), &js); err != nil {
		t.Fatal(err)
	}
	if !js.Applied || js.Moves[2].Change != "add" || js.Moves[2].From != nil {
		t.Errorf("applied json = %+v", js)
	}
	if got := committedAt(t, c, "billing", "orders"); got[0] != 0 || got[1] != 0 || got[2] != 0 {
		t.Errorf("after earliest: %v", got)
	}
	if got := runKaas(t, append(base, "-t", "orders", "--to", "4", "--yes")...); got != "orders p0: 0 -> 4\norders p1: 0 -> 4\norders p2: 0 -> 4\n" {
		t.Errorf("plain output %q", got)
	}
	if got := runKaas(t, append(base, "-t", "orders", "--to", "4", "--yes")...); !strings.Contains(got, "nothing to commit") {
		t.Errorf("no change printed %q", got)
	}

	// A group with no commits yet gets its first, on every partition.
	js = resetJSON{}
	if err := json.Unmarshal([]byte(runKaas(t, "offsets", "reset", "fresh", "-b", brokers, "-t", "orders", "--to", "latest", "--yes", "-o", "json")), &js); err != nil {
		t.Fatal(err)
	}
	if js.State != "Dead" || len(js.Moves) != 3 || js.Moves[0].Change != "add" || js.Moves[0].To != 10 {
		t.Errorf("new group json = %+v", js)
	}

	// The typed yes commits; anything else doesn't.
	real := answerable
	answerable = func(io.Reader) bool { return true }
	t.Cleanup(func() { answerable = real })
	if got := runKaasIn(t, "yes\n", append(base, "-t", "orders", "--to", "latest")...); !strings.Contains(got, "orders p0: 4 -> 10") {
		t.Errorf("typed yes printed %q", got)
	}
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader("y\n"))
	root.SetArgs(append(base, "-t", "orders", "--to", "earliest"))
	var ce *cliError
	if err := root.Execute(); !errors.As(err, &ce) || ce.Code != "cancelled" || committedAt(t, c, "billing", "orders")[0] != 10 {
		t.Errorf("answering y: %v", err)
	}
	answerable = real

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"offsets", "reset", "-t", "orders", "--to", "earliest", "--yes"}, exitUsage},   // no group, no terminal
		{[]string{"offsets", "reset", "billing", "--to", "earliest", "--yes"}, exitUsage},        // no topics
		{[]string{"offsets", "reset", "billing", "-t", "orders", "--yes"}, exitUsage},            // no --to
		{[]string{"offsets", "reset", "billing", "-t", "orders", "--to", "soon"}, exitUsage},     // bad --to
		{[]string{"offsets", "reset", "billing", "-t", "orders", "--to", "earliest"}, exitUsage}, // no terminal, no --yes
		{[]string{"offsets", "reset", "billing", "-t", "orders", "--all-topics", "--to", "earliest", "--yes"}, exitUsage},
		{[]string{"offsets", "reset", "billing", "-t", "ordrs", "--to", "earliest", "--yes"}, exitNotFound},
		{[]string{"offsets", "reset", "nobody", "--all-topics", "--to", "earliest", "--yes"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetIn(strings.NewReader(""))
		root.SetArgs(append(tc.args, "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}

func TestOffsetsResetActiveGroup(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	member, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			member.PollFetches(ctx)
		}
	}()
	for {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs([]string{"offsets", "reset", "billing", "-t", "orders", "--to", "earliest", "--yes", "-b", strings.Join(c.ListenAddrs(), ",")})
		err := root.Execute()
		var ce *cliError
		if errors.As(err, &ce) && ce.Code == "group_active" {
			if ce.Exit != exitFailure || !strings.Contains(ce.Message, "1 active member") {
				t.Errorf("active: %+v", ce)
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("never refused: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// driveReset sends msgs, running the commands they return.
func driveReset(t *testing.T, m resetWizard, msgs ...tea.Msg) resetWizard {
	t.Helper()
	for _, msg := range msgs {
		next, cmd := m.Update(msg)
		m = next.(resetWizard)
		for cmd != nil {
			out := cmd()
			if _, ok := out.(tea.QuitMsg); ok {
				break
			}
			next, cmd = m.Update(out)
			m = next.(resetWizard)
		}
	}
	return m
}

func TestResetWizard(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders", "audit"))
	seedGroup(t, c, "orders", 2, 10, "billing", map[int32]int64{0: 10, 1: 10})
	seedGroup(t, c, "audit", 2, 5, "archiver", map[int32]int64{0: 5})
	th, _ := theme.Get(theme.Default)
	m, err := newResetWizard(context.Background(), kadm.NewClient(cl), kit.New(th, false), resetAsk{})
	if err != nil {
		t.Fatal(err)
	}
	view := func(m resetWizard) string { return ansi.Strip(kit.Join(m.screen(90).Render(m.k, 90, 22))) }

	if v := view(m); !strings.Contains(v, "Which group?") || !strings.Contains(v, "archiver") || !strings.Contains(v, "billing") {
		t.Fatalf("group step:\n%s", v)
	}
	// archiver is first; down to billing, whose one topic is ticked.
	m = driveReset(t, m, keys("down", "enter")...)
	if v := view(m); m.step != resetTopics || !strings.Contains(v, "[x] orders") || strings.Contains(v, "audit") {
		t.Fatalf("topics step:\n%s", v)
	}
	// Untick: Continue refuses; tick again.
	m = driveReset(t, m, typeKeys(" ")...)
	m = driveReset(t, m, keys("enter")...)
	if m.step != resetTopics || m.problem == "" {
		t.Fatalf("no topic ticked: step %d", m.step)
	}
	m = driveReset(t, m, typeKeys(" ")...)
	m = driveReset(t, m, keys("enter")...)
	// Where to: shift, back 4.
	m = driveReset(t, m, keys("right", "right", "right", "right")...)
	m = driveReset(t, m, keys("backspace", "backspace", "backspace", "backspace", "backspace")...)
	m = driveReset(t, m, typeKeys("-4\n")...)
	if v := view(m); m.step != resetReview || m.plan == nil || !strings.Contains(v, "2 offsets move") || !strings.Contains(v, "reads 8 records again") {
		t.Fatalf("review (problem %q):\n%s", m.problem, v)
	}
	m = driveReset(t, m, keys("enter")...)
	if !m.finished || strings.Join(m.chosen(), ",") != "orders" || m.toText() != "-4" {
		t.Errorf("finished %v chosen %v to %q", m.finished, m.chosen(), m.toText())
	}
}
