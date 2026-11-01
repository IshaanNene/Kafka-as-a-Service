package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
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

// seedLedger writes rounds of plain records, plain(i) of them in round i,
// and a transaction of three to topic's partition, every fourth
// transaction aborted.
func seedLedger(t *testing.T, c *kfake.Cluster, topic string, part int32, rounds int, plain func(int) int) {
	t.Helper()
	ctx := context.Background()
	opts := []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DefaultProduceTopic(topic)}
	pc, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	tx, err := kgo.NewClient(append(opts, kgo.TransactionalID("ledger-"+topic))...)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	rec := func(v string) *kgo.Record { return &kgo.Record{Partition: part, Value: []byte(v)} }
	for i := range rounds {
		var rs []*kgo.Record
		for j := range plain(i) {
			rs = append(rs, rec(`{"payment":`+strconv.Itoa(j)+`}`))
		}
		if err := pc.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := tx.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := tx.ProduceSync(ctx, rec(`{"debit":1}`), rec(`{"credit":1}`), rec(`{"fee":1}`)).FirstErr(); err != nil {
			t.Fatal(err)
		}
		end := kgo.TryCommit
		if i%4 == 3 {
			end = kgo.TryAbort
		}
		if err := tx.EndTransaction(ctx, end); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSegmentsCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(2, "ledger"))
	seedLedger(t, c, "ledger", 1, 20, func(int) int { return 2 })
	seedTimeline(t, c, "ledger", []int32{0}, 10, time.Now().Add(-time.Hour), func(int32, int) string { return "x" })
	brokers := strings.Join(c.ListenAddrs(), ",")

	// Without -p: the bigger partition, read whole.
	var j segmentsJSON
	if err := json.Unmarshal([]byte(runKaas(t, "segments", "ledger", "-b", brokers, "-o", "json")), &j); err != nil {
		t.Fatal(err)
	}
	if j.Partition != 1 || j.Sampled || j.End != 120 || j.Records != 120 || j.Holes == nil || *j.Holes != 0 ||
		j.Transactions["committed"] != 15 || j.Transactions["aborted"] != 5 || len(j.Cells) != 120 {
		t.Errorf("json = %+v", j)
	}
	// A budget of a byte: sampled, holes unknown.
	if err := json.Unmarshal([]byte(runKaas(t, "segments", "ledger", "-p", "1", "--budget", "1", "-b", brokers, "-o", "json")), &j); err != nil {
		t.Fatal(err)
	}
	if !j.Sampled || j.Holes != nil || j.Fetches == 0 {
		t.Errorf("sampled json = %+v", j)
	}
	if got := runKaas(t, "segments", "ledger", "-p", "0", "-b", brokers); !strings.Contains(got, "records       10\n") || !strings.Contains(got, "partition     0\n") {
		t.Errorf("text:\n%s", got)
	}

	// The screen, after moving the log start: offsets before it are gone,
	// not holes.
	cl := kafkatest.Client(t, c.ListenAddrs()...)
	adm := kadm.NewClient(cl)
	to := kadm.Offsets{}
	to.Add(kadm.Offset{Topic: "ledger", Partition: 1, At: 30})
	if _, err := adm.DeleteRecords(context.Background(), to); err != nil {
		t.Fatal(err)
	}
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, false)
	s := segmenter{topic: "ledger", part: 1, budget: 64 << 20}
	cmd := newSegmentsCmd(new(string), new(string))
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	if err := s.load(context.Background(), cmd, adm); err != nil {
		t.Fatal(err)
	}
	if err := s.run(cmd, cl, surface{kit: k, w: io.Discard, width: 88, height: 30}, "text"); err != nil {
		t.Fatal(err)
	}
	v := ansi.Strip(kit.Join(s.screen(nil).Render(k, 88, 30)))
	for _, want := range []string{"log · ledger p1", "offsets 30 – 120", "11 ok · 4 aborted", "read whole", "holes        0", "LOG STATS"} {
		if !strings.Contains(v, want) {
			t.Errorf("no %q:\n%s", want, v)
		}
	}
	if len(s.reads) == 0 || strings.Contains(v, "·  ·") {
		t.Errorf("reads %v, screen:\n%s", s.reads, v)
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{nil, exitUsage},
		{[]string{"ledger", "-p", "7"}, exitUsage},
		{[]string{"ledger", "--budget", "lots"}, exitUsage},
		{[]string{"ledger", "-o", "yaml"}, exitUsage},
		{[]string{"ledgr"}, exitNotFound},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(append([]string{"segments"}, tc.args...), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas segments %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}
