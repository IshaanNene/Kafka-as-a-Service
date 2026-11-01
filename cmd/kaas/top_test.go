package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestTopCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders", "audit"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	seedGroups(t, cl)

	plain := runKaas(t, "top", "-b", brokers, "--interval", "100ms", "--sort", "partition")
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 5 || !strings.HasPrefix(lines[0], "TOPIC") || !strings.HasPrefix(lines[1], "audit ") ||
		!strings.HasPrefix(lines[4], "orders  1 ") {
		t.Errorf("plain output, by topic and partition:\n%s", plain)
	}

	var rows []topRow
	if err := json.Unmarshal([]byte(runKaas(t, "top", "-b", brokers, "orders", "--interval", "100ms", "-o", "json")), &rows); err != nil {
		t.Fatal(err)
	}
	var end int64
	for _, r := range rows {
		end += r.EndOffset
		if r.Topic != "orders" || r.InPerSec != 0 || r.OutPerSec != 0 {
			t.Errorf("idle orders row: %+v", r)
		}
	}
	if len(rows) != 2 || end != 30 {
		t.Errorf("json for orders: %+v", rows)
	}
	if out := runKaas(t, "top", "-b", brokers, "pay*", "--interval", "100ms", "-o", "json"); strings.TrimSpace(out) != "[]" {
		t.Errorf("no match should be an empty array, got %s", out)
	}

	for args, wantExit := range map[string]int{
		"top ordrs":          exitNotFound,
		"top --sort cpu":     exitUsage,
		"top --top -1":       exitUsage,
		"top --interval 1ms": exitUsage,
		"top [":              exitUsage,
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(strings.Fields(args), "-b", brokers, "--interval", "100ms"))
		if strings.Contains(args, "--interval") {
			root.SetArgs(append(strings.Fields(args), "-b", brokers))
		}
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != wantExit {
			t.Errorf("kaas %s: %v, want exit %d", args, err, wantExit)
		}
	}
}

func TestTopViewRates(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(2), kfake.SeedTopics(2, "orders"))
	ctx := context.Background()
	tv := &topView{cl: cl, adm: kadm.NewClient(cl), sort: screens.SortIn, interval: 100}
	if err := tv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	first := kit.Join(tv.screen(true).Render(k, 100, 30))
	if !strings.Contains(first, "first reading") {
		t.Errorf("first frame should say rates are coming\n%s", first)
	}

	for range 12 {
		r := &kgo.Record{Topic: "orders", Key: []byte("one-key"), Value: []byte("an order")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := tv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if r := tv.rows[0]; r.In <= 0 || r.End != 12 || tv.rows[1].In != 0 {
		t.Errorf("busiest first: %+v", tv.rows)
	}
	s := tv.screen(true)
	if len(s.Meters) != 2 || s.Gauge.Text != "0.0× writes" {
		t.Errorf("meters %+v, gauge %+v", s.Meters, s.Gauge)
	}
	var full float64
	for _, m := range s.Meters {
		full += m.Fill
	}
	if full < 0.99 || full > 1.01 {
		t.Errorf("broker shares should add up to the whole: %+v", s.Meters)
	}
	out := kit.Join(s.Render(k, 100, 30))
	if !strings.Contains(out, "▐  IN/s▼") || !strings.Contains(out, "every 100ns") {
		t.Errorf("live frame\n%s", out)
	}
}
