package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/clients"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// seedClients starts a group consumer that has read and committed
// everything, and a transactional producer with a transaction open (the
// only producers kfake describes).
func seedClients(t *testing.T, c *kfake.Cluster, cl *kgo.Client) (produce func(int)) {
	t.Helper()
	ctx := context.Background()
	txn, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID("ledger"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(txn.Close)
	if err := txn.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	produce = func(n int) {
		for range n {
			if err := txn.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("v")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	produce(10)
	con, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ClientID("billing-svc"),
		kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(con.Close)
	// One deadline for the whole read: the first poll also joins the group,
	// which can take a few seconds on a busy machine under -race.
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for read := 0; read < 10; {
		fs := con.PollFetches(pctx)
		if pctx.Err() != nil {
			t.Fatalf("consumer read %d of 10 records in 20s", read)
		}
		read += fs.NumRecords()
	}
	if err := con.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}
	_ = cl
	return produce
}

func TestClientsCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"), kfake.SeedTopics(1, "audit"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	seedClients(t, c, cl)

	plain := runKaas(t, "clients", "-b", brokers, "--sort", "name")
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "KIND") ||
		!strings.Contains(lines[1], "billing-svc") || !strings.Contains(lines[1], "billing") ||
		!strings.HasPrefix(lines[2], "producer") || !strings.Contains(lines[2], "orders") {
		t.Errorf("plain output:\n%s", plain)
	}

	var rows []clientJSON
	if err := json.Unmarshal([]byte(runKaas(t, "clients", "-b", brokers, "billing*", "-o", "json")), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != "consumer" || rows[0].ClientID != "billing-svc" || rows[0].Lag == nil || *rows[0].Lag != 0 || rows[0].Partitions != 2 {
		t.Errorf("json for billing*: %+v", rows)
	}
	if out := runKaas(t, "clients", "-b", brokers, "nobody", "-o", "json"); strings.TrimSpace(out) != "[]" {
		t.Errorf("no match should be an empty array, got %s", out)
	}
}

func TestClientViewRates(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	produce := seedClients(t, c, cl)
	ctx := context.Background()
	cv := &clientView{cl: cl, adm: kadm.NewClient(cl), sortBy: "rate", interval: time.Second}
	if err := cv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if s := cv.screen(true); !strings.Contains(s.Hint, "rates after the next refresh") || s.Rows[0].Rate != -1 {
		t.Errorf("first refresh has no rates yet: %+v", s)
	}
	produce(8)
	if err := cv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	// Recompute against an exact 2s gap.
	cv.cur.At = cv.prev.At.Add(2 * time.Second)
	for i := range cv.rows {
		cv.rows[i].Rate = clients.Rate(cv.prev, cv.cur, cv.rows[i].Client)
	}
	cv.sort()
	if r := cv.rows[0]; r.Kind != clients.Producer || r.Rate != 4 || cv.rows[1].Rate != 0 {
		t.Errorf("producer writing 8 in 2s should lead at 4/s: %+v", cv.rows)
	}

	// A producer idle past idleAfter hides unless --all; one in a
	// transaction never does.
	at := time.Now()
	idle := clients.Sample{At: at, Clients: []clients.Client{
		{Kind: clients.Producer, ID: "1", Last: at.Add(-time.Hour)},
		{Kind: clients.Producer, ID: "2", Last: at.Add(-time.Hour), Txn: true},
		{Kind: clients.Producer, ID: "3", Last: at},
	}}
	cv = &clientView{sortBy: "name"}
	cv.apply(idle)
	if len(cv.rows) != 2 || cv.idle != 1 || !strings.Contains(cv.screen(false).Hint, "1 producer idle over 10m hidden") {
		t.Errorf("idle producer should hide: rows %+v, hint %q", cv.rows, cv.screen(false).Hint)
	}
	cv = &clientView{sortBy: "name", all: true}
	cv.apply(idle)
	if len(cv.rows) != 3 {
		t.Errorf("--all shows idle producers: %+v", cv.rows)
	}
}
