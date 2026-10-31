package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// seedGroups produces 30 records to orders and commits three groups at
// different offsets, so each has a different lag.
func seedGroups(t *testing.T, cl *kgo.Client) {
	t.Helper()
	ctx := context.Background()
	for range 30 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	for group, behind := range map[string]int64{"billing": 20, "billing-eu": 5, "audit": 0} {
		var os kadm.Offsets
		left := behind
		ends.Each(func(o kadm.ListedOffset) {
			back := min(left, o.Offset)
			left -= back
			os.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: o.Offset - back, LeaderEpoch: -1})
		})
		if err := adm.CommitAllOffsets(ctx, group, os); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGroupsCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	seedGroups(t, cl)

	// Plain: worst lag first, raw numbers.
	plain := runKaas(t, "groups", "-b", brokers)
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "GROUP") ||
		!strings.HasPrefix(lines[1], "billing ") || !strings.HasSuffix(lines[1], " 20") ||
		!strings.HasPrefix(lines[2], "billing-eu") || !strings.HasPrefix(lines[3], "audit") {
		t.Errorf("plain output, by lag:\n%s", plain)
	}
	if byName := runKaas(t, "groups", "-b", brokers, "--sort", "name"); !strings.HasPrefix(strings.Split(byName, "\n")[1], "audit") {
		t.Errorf("sorted by name:\n%s", byName)
	}

	var rows []groupJSON
	if err := json.Unmarshal([]byte(runKaas(t, "groups", "-b", brokers, "billing*", "-o", "json")), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Name != "billing" || rows[0].Lag == nil || *rows[0].Lag != 20 ||
		rows[0].Committed != 10 || len(rows[0].Topics) != 1 || rows[0].Topics[0] != "orders" {
		t.Errorf("json for billing*: %+v", rows)
	}

	// A plain name that isn't a group is a 404 with a suggestion.
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"groups", "-b", brokers, "biling"})
	_, err := root.ExecuteC()
	var ce *cliError
	if !errors.As(err, &ce) || ce.Exit != exitNotFound || !strings.Contains(strings.Join(ce.Hints, " "), "billing") {
		t.Errorf("kaas groups biling: %v, want a not-found suggesting billing", err)
	}
}

func TestGroupsEmpty(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	if out := runKaas(t, "groups", "-b", strings.Join(c.ListenAddrs(), ",")); !strings.HasPrefix(out, "GROUP") || strings.Count(out, "\n") != 1 {
		t.Errorf("no groups should print just the header:\n%s", out)
	}
}

// TestGroupViewRates checks the live view's second refresh: rates from the
// commits in between, the history growing, and re-sorting by rate.
func TestGroupViewRates(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	seedGroups(t, cl)
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	gv := groupView{adm: adm, sortBy: "rate", interval: time.Second}
	if err := gv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if s := gv.screen(true); s.Rows[0].Rate != -1 || !strings.Contains(s.Hint, "rates after the next refresh") {
		t.Errorf("first refresh has no rates: %+v", s.Rows[0])
	}
	// billing-eu consumes its 5 records.
	ends, _ := adm.ListEndOffsets(ctx, "orders")
	if err := adm.CommitAllOffsets(ctx, "billing-eu", ends.Offsets()); err != nil {
		t.Fatal(err)
	}
	if err := gv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	gv.cur.At = gv.prev.At.Add(time.Second)
	gv.sort()
	s := gv.screen(true)
	if s.Rows[0].Name != "billing-eu" || s.Rows[0].Rate != 5 || s.Rows[0].Lag != 0 {
		t.Errorf("by rate after a refresh: %+v", s.Rows)
	}
	if len(s.History) != 2 || s.History[0] != 25 || s.History[1] != 20 {
		t.Errorf("history = %v, want total lag 25 then 20", s.History)
	}
	if _, ok := groups.Rate(gv.prev, gv.cur, "audit"); !ok {
		t.Error("audit is in both samples")
	}
}
