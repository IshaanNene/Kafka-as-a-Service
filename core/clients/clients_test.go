package clients

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestFetch(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	ctx := context.Background()
	adm := kadm.NewClient(cl)
	// kfake describes only producers inside a transaction; Kafka itself
	// describes every idempotent producer it keeps state for.
	txn, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID("ledger"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Close()
	if err := txn.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	produce := func(n int) {
		for range n {
			if err := txn.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("v")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	produce(10)
	for range 10 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ClientID("billing-svc"),
		kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	read := 0
	for read < 20 {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fs := consumer.PollFetches(pctx)
		timedOut := pctx.Err() != nil
		cancel()
		if timedOut {
			t.Fatal("no records")
		}
		read += fs.NumRecords()
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}

	s, err := Fetch(ctx, adm, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	var con, pro *Client
	for i := range s.Clients {
		switch s.Clients[i].Kind {
		case Consumer:
			con = &s.Clients[i]
		case Producer:
			pro = &s.Clients[i]
		}
	}
	if con == nil || con.ID != "billing-svc" || con.Group != "billing" || con.Parts != 2 || con.Lag != 0 || con.Host == "" {
		t.Errorf("consumer = %+v", con)
	}
	if pro == nil || pro.Parts != 1 || pro.Marks["orders/0"] != 10 || pro.Last.IsZero() || !pro.Txn || pro.TxnID != "ledger" {
		t.Errorf("producer = %+v", pro)
	}

	produce(6)
	next, err := Fetch(ctx, adm, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	next.At = s.At.Add(2 * time.Second)
	for _, c := range next.Clients {
		want := 0.0 // the consumer has not committed again
		if c.Kind == Producer {
			want = 3
		}
		if got := Rate(s, next, c); got != want {
			t.Errorf("%s rate = %v, want %v", c.Kind, got, want)
		}
	}
	if got := Rate(Sample{At: s.At}, next, next.Clients[0]); got != -1 {
		t.Errorf("a client new since the last sample has rate %v, want -1", got)
	}
}

func TestRateOverSharedPartitions(t *testing.T) {
	at := time.Now()
	prev := Sample{At: at, Clients: []Client{{Kind: Producer, ID: "7", Marks: map[string]int64{"a/0": 100, "a/1": 50}}}}
	cur := Sample{At: at.Add(time.Second), Clients: []Client{{Kind: Producer, ID: "7", Marks: map[string]int64{"a/0": 130, "a/1": 5, "a/2": 900}}}}
	// a/1 went back (a new epoch): zero. a/2 is new: not counted.
	if got := Rate(prev, cur, cur.Clients[0]); got != 30 {
		t.Errorf("rate = %v, want 30", got)
	}
}
