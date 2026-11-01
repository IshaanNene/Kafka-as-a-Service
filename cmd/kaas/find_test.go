package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// seedTimeline writes n records a second apart from base to each of
// parts, with values from value.
func seedTimeline(t *testing.T, c *kfake.Cluster, topic string, parts []int32, n int, base time.Time, value func(p int32, i int) string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var rs []*kgo.Record
	for _, p := range parts {
		for i := range n {
			rs = append(rs, &kgo.Record{Topic: topic, Partition: p, Timestamp: base.Add(time.Duration(i) * time.Second), Key: []byte("user-17"), Value: []byte(value(p, i))})
		}
	}
	if err := cl.ProduceSync(context.Background(), rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestOffsetsFindCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	seedTimeline(t, c, "orders", []int32{0, 1}, 400, base, func(p int32, i int) string {
		switch {
		case p == 0 && i >= 290:
			return `{"version":2}`
		case p == 1 && i == 350:
			return `{"version":2}` // a lone match, then old records again
		}
		return `{"version":1}`
	})
	brokers := strings.Join(c.ListenAddrs(), ",")
	at := base.Add(120 * time.Second).Format(time.RFC3339)

	var js []findJSON
	if err := json.Unmarshal([]byte(runKaas(t, "offsets", "find", "orders", "-b", brokers, "--time", at, "-o", "json")), &js); err != nil {
		t.Fatal(err)
	}
	if len(js) != 3 || *js[0].Offset != 120 || *js[1].Offset != 120 || *js[0].IndexOffset != 120 || js[2].Offset != nil || js[0].Reads > 10 {
		t.Errorf("time json = %+v", js)
	}
	if got := runKaas(t, "offsets", "find", "orders", "-b", brokers, "--match", `"version":2`, "-p", "0"); !strings.HasPrefix(got, "orders p0 290 ") || strings.Count(got, "\n") != 1 {
		t.Errorf("match printed %q", got)
	}

	// The screen: the hunt, its log, and the recheck that catches a lone match.
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	cl := kafkatest.Client(t, c.ListenAddrs()...)
	for _, tc := range []struct {
		part int32
		want string
	}{{0, "the next 3 records match too"}, {1, "matches come and go"}} {
		f := finder{topic: "orders", cmdline: "kaas offsets find orders --match v2"}
		f.re = regexp.MustCompile(`"version":2`)
		cmd := newOffsetsFindCmd(new(string), new(string))
		cmd.SetContext(context.Background())
		if err := f.load(context.Background(), cmd, kadm.NewClient(cl), []int32{tc.part}); err != nil {
			t.Fatal(err)
		}
		if err := f.run(cmd, cl, surface{kit: k, w: io.Discard, width: 88, height: 30}, "text"); err != nil {
			t.Fatal(err)
		}
		v := ansi.Strip(kit.Join(f.screen(f.hunts[0]).Render(k, 88, 30)))
		if !strings.Contains(v, tc.want) || !strings.Contains(v, "found") {
			t.Errorf("p%d screen:\n%s", tc.part, v)
		}
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"orders"}, exitUsage},
		{[]string{"orders", "--time", "soon"}, exitUsage},
		{[]string{"orders", "--time", "latest"}, exitUsage},
		{[]string{"orders", "--match", "("}, exitUsage},
		{[]string{"orders", "--match", "x", "--time", "2h"}, exitUsage},
		{[]string{"orders", "--time", "2h", "-p", "7"}, exitUsage},
		{[]string{"ordrs", "--time", "2h"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(append([]string{"offsets", "find"}, tc.args...), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas offsets find %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}
