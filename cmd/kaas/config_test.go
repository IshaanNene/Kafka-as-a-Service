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

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestConfigCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	ctx := context.Background()
	adm := kadm.NewClient(cl)
	day, compact, zstd := "86400000", "compact,delete", "zstd"
	if _, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Name: "retention.ms", Value: &day}, {Name: "cleanup.policy", Value: &compact}}, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := adm.AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Name: "compression.type", Value: &zstd}}, 0); err != nil {
		t.Fatal(err)
	}
	brokers := strings.Join(c.ListenAddrs(), ",")

	var js []configJSON
	if err := json.Unmarshal([]byte(runKaas(t, "config", "orders", "-b", brokers, "-o", "json")), &js); err != nil {
		t.Fatal(err)
	}
	by := map[string]configJSON{}
	for _, j := range js {
		by[j.Key] = j
	}
	if r := by["retention.ms"]; *r.Value != day || r.Source != "topic" || r.Section != "retention" || r.Default == nil || *r.Default != "604800000" {
		t.Errorf("retention.ms %+v", r)
	}
	if r := by["compression.type"]; r.Source != "broker" {
		t.Errorf("compression.type %+v", r)
	}
	set := runKaas(t, "config", "orders", "--set", "-b", brokers)
	if strings.Count(set, "\n") != 3 || !strings.Contains(set, "retention.ms") || !strings.Contains(set, "topic    86400000") {
		t.Errorf("--set printed:\n%s", set)
	}
	if got := runKaas(t, "config", "--broker", "0", "-b", brokers); !strings.Contains(got, "(hidden)") || !strings.Contains(got, "broker.rack") {
		t.Errorf("broker printed:\n%s", got)
	}

	// The screen: sections, switches and sliders, the selected config's foot.
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	v := configView{topic: "orders"}
	cmd := newConfigCmd(new(string), new(string))
	cmd.SetContext(ctx)
	if err := v.load(ctx, cmd, cl); err != nil {
		t.Fatal(err)
	}
	m := configModel{v: v, k: k, w: 100, h: 30}
	m.v.sel = m.v.first()
	out := ansi.Strip(m.View().Content)
	for _, want := range []string{"Config · topic orders", "✓ 2 set on the topic · 1 from brokers", "RETENTION", "retention.ms", "1d", "topic", "Kafka's default", "━"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q:\n%s", want, out)
		}
	}
	key := func(s string) {
		var code rune
		switch s {
		case "down":
			code = tea.KeyDown
		case "up":
			code = tea.KeyUp
		default:
			code = rune(s[0])
		}
		next, _ := m.Update(tea.KeyPressMsg{Code: code, Text: map[bool]string{true: s}[len(s) == 1]})
		m = next.(configModel)
	}
	_, es := m.v.rows()
	first := es[m.v.sel].Key
	key("down")
	if _, es := m.v.rows(); es[m.v.sel] == nil || es[m.v.sel].Key == first {
		t.Errorf("down stayed on %q", first)
	}
	key("down") // retention.ms: set on the topic
	if out := ansi.Strip(m.View().Content); !strings.Contains(out, "retention.ms = 86400000 (1d) · set on the topic · without it 7d") {
		t.Errorf("retention.ms foot:\n%s", out)
	}
	key("s")
	out = ansi.Strip(m.View().Content)
	if !strings.Contains(out, "s show all") || strings.Contains(out, "segment.bytes") || !strings.Contains(out, "compression.type") {
		t.Errorf("set only:\n%s", out)
	}
	if h := m.v.screen(100); h.Height() < 12 {
		t.Errorf("height %d", h.Height())
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{nil, exitUsage},
		{[]string{"orders", "payments"}, exitUsage},
		{[]string{"orders", "--broker", "0"}, exitUsage},
		{[]string{"--broker", "9"}, exitUsage},
		{[]string{"orders", "-o", "yaml"}, exitUsage},
		{[]string{"ordrs"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(append([]string{"config"}, tc.args...), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas config %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}
