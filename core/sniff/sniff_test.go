package sniff

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// dials records every broker address a client connects to.
type dials struct {
	mu    sync.Mutex
	addrs map[string]bool
}

func (d *dials) OnBrokerConnect(m kgo.BrokerMetadata, _ time.Duration, _ net.Conn, _ error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addrs[net.JoinHostPort(m.Host, strconv.Itoa(int(m.Port)))] = true
}

func TestProxy(t *testing.T) {
	_, admin := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	var seen []Request
	p, err := Start(ctx, admin, "127.0.0.1:0", func(r Request) {
		mu.Lock()
		seen = append(seen, r)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	routes := p.Routes()
	if len(routes) != 3 || p.Addr() != routes[0].Listen {
		t.Fatalf("routes %+v, addr %s", routes, p.Addr())
	}

	d := &dials{addrs: map[string]bool{}}
	pc, err := kgo.NewClient(kgo.SeedBrokers(p.Addr()), kgo.WithHooks(d),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	var rs []*kgo.Record
	for i := range 9 {
		rs = append(rs, &kgo.Record{Topic: "orders", Partition: int32(i % 3), Value: []byte("v" + strconv.Itoa(i))})
	}
	if err := pc.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	pc.Close()

	cc, err := kgo.NewClient(kgo.SeedBrokers(p.Addr()), kgo.WithHooks(d), kgo.ClientID("sniff-test"),
		kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for got < 9 {
		fs := cc.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("read %d of 9: %v", got, err)
		}
		got += fs.NumRecords()
	}
	if err := cc.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	// Every connection the clients made was to a listener of the proxy.
	listens := map[string]bool{}
	for _, r := range p.Routes() {
		listens[r.Listen] = true
	}
	for a := range d.addrs {
		if !listens[a] {
			t.Errorf("a client went around the proxy, to %s", a)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	by := map[string][]Request{}
	records := 0
	for _, r := range seen {
		by[r.Name()] = append(by[r.Name()], r)
		if !r.Answered || r.RespSize == 0 || r.Size < 12 || len(r.Head) == 0 {
			t.Errorf("unanswered or empty: %+v", r)
		}
		if r.Name() == "Produce" {
			if !strings.HasPrefix(r.Info, "orders p") || !strings.HasSuffix(r.Info, "acks all") {
				t.Errorf("produce info %q", r.Info)
			}
			n, _ := strconv.Atoi(strings.Fields(strings.Split(r.Info, " · ")[1])[0])
			records += n
		}
	}
	if records != 9 {
		t.Errorf("produce requests carried %d records, want 9", records)
	}
	for _, want := range []string{"ApiVersions", "Metadata", "Produce", "Fetch", "FindCoordinator", "JoinGroup", "SyncGroup", "OffsetCommit"} {
		if len(by[want]) == 0 {
			t.Errorf("no %s request seen; saw %v", want, keysOf(by))
		}
	}
	rewrote := false
	for _, r := range by["Metadata"] {
		rewrote = rewrote || r.Rewrote
	}
	if !rewrote {
		t.Error("no Metadata answer was rewritten")
	}
	for _, r := range by["JoinGroup"] {
		if !strings.HasPrefix(r.Info, "billing · consumer") || r.ClientID != "sniff-test" {
			t.Errorf("join info %q from %q", r.Info, r.ClientID)
		}
	}
	for i := 1; i < len(seen); i++ {
		if seen[i].N == seen[i-1].N {
			t.Fatalf("request numbers repeat: %d", seen[i].N)
		}
	}
}

func keysOf(m map[string][]Request) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func TestStartErrors(t *testing.T) {
	_, cl := kafkatest.Fake(t)
	ctx := context.Background()
	for _, bad := range []string{"19092", "localhost:x", "localhost:70000"} {
		if _, err := Start(ctx, cl, bad, nil); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
	// A port already taken by something else.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if _, err := Start(ctx, cl, l.Addr().String(), nil); err == nil {
		t.Error("a taken port: no error")
	}
}

func TestBatchRecords(t *testing.T) {
	if n := batchRecords(nil); n != 0 {
		t.Errorf("nil: %d", n)
	}
}
