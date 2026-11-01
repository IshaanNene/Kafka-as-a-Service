package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func TestTopicsAlterCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	count := func() int {
		// A fresh client each time: kgo caches metadata for a few seconds.
		td, err := kadm.NewClient(kafkatest.Client(t, c.ListenAddrs()...)).ListTopics(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		return len(td["orders"].Partitions)
	}

	// A dry run asks the controller and writes nothing.
	if got := runKaas(t, "topics", "alter", "-b", brokers, "orders", "--partitions", "5", "--dry-run"); got != "orders: 3 → 5 partitions (dry run, nothing written)\n" || count() != 3 {
		t.Errorf("dry run printed %q and left %d partitions", got, count())
	}

	var j alterJSON
	if err := json.Unmarshal([]byte(runKaas(t, "topics", "alter", "-b", brokers, "orders", "--partitions", "5", "--yes", "-o", "json")), &j); err != nil {
		t.Fatal(err)
	}
	if j.From != 3 || j.To != 5 || len(j.Added) != 2 || j.Added[0] != 3 || j.DryRun || count() != 5 {
		t.Errorf("json = %+v, %d partitions", j, count())
	}

	// The same count again is nothing to do, not an error, so scripts can
	// rerun it.
	if got := runKaas(t, "topics", "alter", "-b", brokers, "orders", "--partitions", "5", "--yes"); !strings.Contains(got, "already has 5 partitions") {
		t.Errorf("no-op printed %q", got)
	}

	// A typed yes on the prompt writes; anything else doesn't.
	real := answerable
	answerable = func(io.Reader) bool { return true }
	t.Cleanup(func() { answerable = real })
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader("no\n"))
	root.SetArgs([]string{"topics", "alter", "-b", brokers, "orders", "--partitions", "6"})
	if err := root.Execute(); err == nil || count() != 5 {
		t.Errorf("an answer other than yes: err %v, %d partitions", err, count())
	}
	if got := runKaasIn(t, "yes\n", "topics", "alter", "-b", brokers, "orders", "--partitions", "6"); got != "orders: 5 → 6 partitions, added 5\n" || count() != 6 {
		t.Errorf("confirmed printed %q, %d partitions", got, count())
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"orders", "--partitions", "2", "--yes"}, exitUsage}, // Kafka only adds
		{[]string{"orders", "--yes"}, exitUsage},                      // no count
		{[]string{"orders", "--partitions", "0", "--yes"}, exitUsage},
		{[]string{"ordrs", "--partitions", "9", "--yes"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetIn(strings.NewReader(""))
		root.SetArgs(append([]string{"topics", "alter", "-b", brokers}, tc.args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas topics alter %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
	if count() != 6 {
		t.Errorf("a refused alter changed the topic: %d partitions", count())
	}
}

func TestAlterNeedsYesWithoutATerminal(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader("yes\n"))
	root.SetArgs([]string{"topics", "alter", "-b", strings.Join(c.ListenAddrs(), ","), "orders", "--partitions", "2"})
	_, err := root.ExecuteC()
	var ce *cliError
	if !errors.As(err, &ce) || ce.Exit != exitUsage || !strings.Contains(ce.Message, "--yes") {
		t.Errorf("piped yes without --yes: %v, want a usage error naming --yes", err)
	}
}

func TestConfirm(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{{"yes\n", true}, {"  yes\t\n", true}, {"y\n", false}, {"YES\n", false}, {"", false}} {
		var out bytes.Buffer
		if got := confirm(strings.NewReader(tc.in), &out, kit.Kit{}, "Sure? "); got != tc.want {
			t.Errorf("confirm(%q) = %v, want %v", tc.in, got, tc.want)
		}
		if out.String() != "    Sure? (type yes) " {
			t.Errorf("prompt = %q", out.String())
		}
	}
}

func TestAlterScreens(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(2, "orders"))
	ctx := context.Background()
	td, err := describeTopic(ctx, cl, "orders")
	if err != nil {
		t.Fatal(err)
	}
	a := alteration{td: td, to: 4, readers: []string{"billing"}}
	plan := a.screen()
	if len(plan.Parts) != 4 || !plan.Parts[2].New || plan.Parts[2].Leader != -1 || plan.Focus != 2 {
		t.Errorf("plan parts %+v focus %d, want two new unplaced partitions marked", plan.Parts, plan.Focus)
	}
	if plan.Details[0].Value != "2 → 4 partitions · adds p2, p3 · RF 3, placed by the controller" || !strings.HasPrefix(plan.Details[2].Value, "billing rebalance") {
		t.Errorf("plan details %+v", plan.Details)
	}

	if _, err := kadm.NewClient(cl).UpdatePartitions(ctx, 4, "orders"); err != nil {
		t.Fatal(err)
	}
	after, err := describeTopic(ctx, cl, "orders")
	if err != nil {
		t.Fatal(err)
	}
	a.after = &after
	done := a.screen()
	if !done.Parts[3].New || done.Parts[1].New || done.Parts[3].Leader < 0 || done.StatusRole != kit.OK || !strings.HasPrefix(done.Details[0].Value, "p2, p3 · leaders ") {
		t.Errorf("written screen %+v", done)
	}
}
