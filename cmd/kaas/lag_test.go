package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestLagCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	seedGroups(t, cl)

	plain := runKaas(t, "lag", "-b", brokers)
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "GROUP") ||
		!strings.HasPrefix(lines[1], "billing ") || !strings.HasSuffix(lines[1], " 20") ||
		!strings.HasSuffix(lines[3], " 0") {
		t.Errorf("plain output, worst first:\n%s", plain)
	}

	var pairs []lagPair
	if err := json.Unmarshal([]byte(runKaas(t, "lag", "-b", brokers, "billing-*", "-o", "json")), &pairs); err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 || pairs[0] != (lagPair{Group: "billing-eu", Topic: "orders", Lag: 5}) {
		t.Errorf("json for billing-*: %+v", pairs)
	}
	if out := runKaas(t, "lag", "-b", brokers, "nobody", "-o", "json"); strings.TrimSpace(out) != "[]" {
		t.Errorf("no match should be an empty array, got %s", out)
	}
}

func TestLagViewTrendAndCaughtUp(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	seedGroups(t, cl)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	lv := &lagView{adm: adm, top: 10, interval: time.Second, past: map[string][]int64{}}
	if err := lv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if s := lv.screen(true); s.Rows[0].TrendKnown || s.Rows[0].Group != "billing" {
		t.Errorf("first refresh: billing on top with no trend yet, got %+v", s.Rows)
	}

	// Everyone catches up.
	ends, _ := adm.ListEndOffsets(ctx, "orders")
	for _, g := range []string{"billing", "billing-eu"} {
		if err := adm.CommitAllOffsets(ctx, g, ends.Offsets()); err != nil {
			t.Fatal(err)
		}
	}
	if err := lv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if lv.total() != 0 || len(lv.history) != 2 || lv.history[0] != 25 {
		t.Errorf("after catching up: total %d, history %v", lv.total(), lv.history)
	}
	for _, r := range lv.screen(true).Rows {
		if r.Group == "billing" && (!r.TrendKnown || r.Trend != -20) {
			t.Errorf("billing's trend = %+v, want -20", r)
		}
	}
	th, _ := theme.Get(theme.Default)
	surf := surface{width: 80, height: 40, kit: kit.New(th, false)}
	if out := kit.Join(lv.frame(surf, true)); !strings.Contains(out, "You're all caught up") || !strings.Contains(out, "3 groups at the head of 1 topic.") {
		t.Errorf("caught-up card:\n%s", out)
	}
}
