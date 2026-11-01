package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/bench"
	brokerload "github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/top"
	"github.com/IshaanNene/Kafka-as-a-Service/internal/termshot"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/tui"
)

var shots = flag.String("shots", "", "write SVG screenshots of every screen to this directory")

// TestScreenshots drives the real commands against an in-memory Kafka
// cluster (kfake) and saves what each screen draws as an SVG for the docs:
// the probe really probes, the bench really produces, watch runs the real
// engine. Run it with `task screenshots`.
func TestScreenshots(t *testing.T) {
	if *shots == "" {
		t.Skip("pass -shots <dir> to regenerate the docs' screenshots")
	}
	th, _ := theme.Get(theme.Default)
	k := kit.New(th, true)
	save := func(name, title string, b []string) {
		t.Helper()
		svg := termshot.SVG(kit.Join(b), termshot.Options{Title: title, BG: th.BG, FG: th.FG, Chrome: th.Rule})
		if err := os.WriteFile(filepath.Join(*shots, name+".svg"), []byte(svg), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(*shots, 0o755); err != nil {
		t.Fatal(err)
	}

	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders"), kfake.SeedTopics(3, "payments"),
		kfake.SeedTopics(12, "clickstream"), kfake.SeedTopics(2, "inventory"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	// Help and examples, straight from cobra.
	root := newRoot()
	for _, path := range [][]string{nil, {"bench", "produce"}, {"watch"}} {
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatal(err)
		}
		h := helpFor(cmd)
		h.Prompt = "you@laptop: ~/kafka"
		name := "help-" + strings.Join(append([]string{"kaas"}, path...), "-")
		save(name, cmd.CommandPath()+" --help", h.Render(k, 100))
	}

	// Connecting: the real probe, with its real timings.
	var boot []string
	seeds := c.ListenAddrs()[:1]
	boot = append(boot, screens.BootStart(k, seeds)...)
	r := probe.Run(ctx, seeds, probe.Options{Report: func(s probe.Step) { boot = append(boot, screens.BootStep(k, s)...) }})
	save("connect", "kaas snapshot", append(boot, screens.BootDone(k, r)...))

	// Unreachable: a port nobody listens on, and a name that doesn't resolve.
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := l.Addr().String()
	_ = l.Close()
	r = probe.Run(ctx, []string{closed, "kafka-0.prod.invalid:9092"}, probe.Options{})
	save("unreachable", "kaas snapshot -b …", screens.Unreachable(r).Render(k, 100))

	// Errors through the real command paths.
	screen := func(args ...string) []string {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(args)
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.screen == nil {
			t.Fatalf("kaas %v: no error screen (%v)", args, err)
		}
		return ce.screen(k, 100)
	}
	save("not-found-topic", "kaas watch -t oders", screen("watch", "-b", brokers, "-t", "oders"))
	save("not-found-command", "kaas wach", screen("wach"))
	save("invalid-flag", "kaas bench produce --acks 2", screen("bench", "produce", "-b", brokers, "-t", "orders", "--size", "512", "--acks", "2"))

	// Bench: four real cases against kfake.
	b := benchRun{
		cmd: &cobra.Command{}, topic: "orders", seeds: kgo.SeedBrokers(c.ListenAddrs()...),
		// Capped, so each case runs long enough to chart its throughput;
		// kfake is in-memory and would otherwise report millions per second.
		cases: bench.Matrix([]int{100, 1024}, []string{"1", "all"}, []string{"none", "lz4"}, 30_000, 15_000),
		out:   surface{w: io.Discard, raw: io.Discard, rich: true, width: 100, height: 60, kit: k},
		title: "bench produce · orders", right: "3 brokers · kfake",
	}
	b.cmd.SetContext(ctx)
	b.lv = &live{s: b.out}
	if err := b.run(); err != nil {
		t.Fatal(err)
	}
	save("bench", "kaas bench produce -t orders --size 100,1024 --compression none,lz4", b.lv.last)

	// Watch: the real engine while records flow and topics come and go.
	e := engine.New(cl, engine.Config{MetadataInterval: time.Second, FastInterval: 500 * time.Millisecond})
	sub := e.Subscribe()
	defer sub.Close()
	defer e.Watch("orders", "payments")()
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = e.Run(ectx) }()
	go produceWaves(ectx, c.ListenAddrs())
	view := newWatchView(surface{w: io.Discard, raw: io.Discard, rich: true, width: 100, height: 34, kit: k}, []string{"orders", "payments"}, e.Config())
	deadline := time.After(14 * time.Second)
	created, moved := false, false
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-sub.Ready():
			u, _ := sub.Next()
			view.update(u, e.Stats(), time.Now())
			switch n := len(view.series["orders"].Rates); {
			case !moved && n > 8:
				moved = true
				movePartitionLeader(t, c, s0(e), "orders", 3)
			case !created && n > 14:
				created = true
				_, _ = adm.CreateTopics(ctx, 4, 3, nil, "audit")
				_, _ = adm.CreatePartitions(ctx, 2, "payments")
			}
		}
	}
	view.draw(time.Now())
	save("watch", "kaas watch -t orders -t payments", view.lv.last)

	// Topics: prefixed families and a compacted topic that sets its own
	// retention, through the command's own listing path.
	compact, month := "compact", "2592000000"
	_, _ = adm.CreateTopics(ctx, 3, 3, nil, "payments.eu.charges", "payments.eu.refunds", "payments.us.charges", "orders.dlq")
	_, _ = adm.CreateTopic(ctx, 6, 3, map[string]*string{"cleanup.policy": &compact, "retention.ms": &month}, "users.profiles")
	for i := range 400 {
		cl.Produce(ctx, &kgo.Record{Topic: "payments.eu.charges", Value: []byte("a charge of some size")}, nil)
		if i%2 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "users.profiles", Key: []byte{byte(i)}, Value: []byte(`{"name":"ada"}`)}, nil)
		}
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tl, err := listTopics(ctx, cl, "", false, "status")
	if err != nil {
		t.Fatal(err)
	}
	save("topics", "kaas topics", tl.tableScreen().Render(k, 100))
	tl.sep = "."
	save("topics-tree", "kaas topics --tree", tl.treeScreen().Render(k, 84))
	td, err := describeTopic(ctx, cl, "users.profiles")
	if err != nil {
		t.Fatal(err)
	}
	save("topics-describe", "kaas topics describe users.profiles", td.screen().Render(k, 90))

	// Brokers: the cluster above, its leaders spread by kfake, topics led
	// most under each broker.
	bsnap, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	disk, derr := brokerload.Disk(ctx, adm)
	bl := brokerList{snap: bsnap, brokers: brokerload.Summarize(bsnap, disk), diskErr: derr, top: 4}
	save("brokers", "kaas brokers", bl.screen().Render(k, 100))

	// Groups: a live consumer, and committed groups that consume at their
	// own pace over twelve 2s refreshes while orders keep arriving.
	live, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumerGroup("search-index"),
		kgo.ConsumeTopics("orders"), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	gv := groupView{adm: adm, sortBy: "lag", interval: 2 * time.Second}
	lagv := &lagView{adm: adm, top: 8, interval: 2 * time.Second, past: map[string][]int64{}}
	// Each committed group reads its topics at its own records per refresh;
	// fraud-check has stalled. search-index is the live consumer above.
	reads := map[string][]string{"billing": {"orders", "payments"}, "fraud-check": {"payments"},
		"audit-sink": {"orders"}, "inventory-sync": {"inventory"}}
	pace := map[string]int64{"billing": 900, "fraud-check": 0, "audit-sink": 1 << 40, "inventory-sync": 0}
	at := map[string]int64{}
	base := time.Now()
	for i := range 12 {
		for range 600 {
			cl.Produce(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}, nil)
			cl.Produce(ctx, &kgo.Record{Topic: "payments", Value: []byte("a payment")}, nil)
		}
		if err := cl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		pctx, pcancel := context.WithTimeout(ctx, time.Second)
		live.PollFetches(pctx)
		_ = live.CommitUncommittedOffsets(pctx)
		pcancel()
		for group, topics := range reads {
			ends, err := adm.ListEndOffsets(ctx, topics...)
			if err != nil {
				t.Fatal(err)
			}
			var os kadm.Offsets
			ends.Each(func(o kadm.ListedOffset) {
				key := group + "/" + o.Topic + "/" + strconv.Itoa(int(o.Partition))
				if i == 0 {
					at[key] = o.Offset / 3
				}
				at[key] = min(o.Offset, at[key]+pace[group]/int64(len(ends[o.Topic])))
				os.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: at[key], LeaderEpoch: -1})
			})
			if err := adm.CommitAllOffsets(ctx, group, os); err != nil {
				t.Fatal(err)
			}
		}
		if err := gv.refresh(ctx); err != nil {
			t.Fatal(err)
		}
		gv.cur.At = base.Add(time.Duration(i) * 2 * time.Second)
		gv.sort()
		if err := lagv.refresh(ctx); err != nil {
			t.Fatal(err)
		}
		lagv.cur.At = gv.cur.At
	}
	save("groups", "kaas groups", gv.screen(true).Render(k, 100))
	save("lag", "kaas lag", lagv.screen(true).Render(k, 100, 40))
	save("lag-caught-up", "kaas lag", screens.CaughtUp{Title: "You're all caught up",
		Detail: "5 groups at the head of 4 topics. Watching every 2s.", Action: "kaas groups for members and rates →"}.Render(k, 64))

	// Consume: keyed orders with a header and a tombstone, read the way the
	// command reads them, into the channel screen.
	for i := range 9 {
		r := &kgo.Record{Topic: "inventory", Key: []byte(fmt.Sprintf("sku-%d", 100+i*7)),
			Value: []byte(fmt.Sprintf(`{"sku":"A-%d","warehouse":"ams-%d","on_hand":%d,"reserved":%d}`, 100+i*7, 1+i%3, 40-i*3, i%4))}
		switch i {
		case 3:
			r.Headers = []kgo.RecordHeader{{Key: "trace-id", Value: []byte("4bf92f3577b34da6")}, {Key: "source", Value: []byte("restock")}}
		case 6:
			r.Value = nil
		}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	from, _ := consume.ParseFrom("-5", time.Now())
	cplan, err := consume.Plan(ctx, adm, "inventory", nil, from)
	if err != nil {
		t.Fatal(err)
	}
	cv := newChannelView(surface{width: 100, height: 30, kit: k}, consume.Options{Topic: "inventory", Plan: cplan}, from, "kafka-1:9092")
	co := consume.Options{Seeds: c.ListenAddrs(), Topic: "inventory", Plan: cplan, Exit: true}
	if err := consume.Read(ctx, co, nil, func(r consume.Record) error { cv.add(r); return nil }); err != nil {
		t.Fatal(err)
	}
	cv.rate = 3
	save("consume", "kaas consume inventory --from -5", cv.frame(time.Now()))

	// Top: two readings 2s apart, with orders busiest, payments next,
	// and billing reading its share of both.
	tv := &topView{cl: cl, adm: adm, sort: screens.SortIn, limit: 12, interval: 2 * time.Second}
	if err := tv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	before := tv.cur.End
	for i := range 2400 {
		cl.Produce(ctx, &kgo.Record{Topic: "orders", Key: []byte(strconv.Itoa(i % 7)), Value: []byte(`{"order":"an order"}`)}, nil)
		if i%3 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "payments", Key: []byte(strconv.Itoa(i % 5)), Value: []byte("a payment")}, nil)
		}
		if i%40 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "inventory", Value: []byte("a restock")}, nil)
		}
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for group, topics := range reads {
		if pace[group] == 0 {
			continue
		}
		ends, err := adm.ListEndOffsets(ctx, topics...)
		if err != nil {
			t.Fatal(err)
		}
		var os kadm.Offsets
		ends.Each(func(o kadm.ListedOffset) {
			key := group + "/" + o.Topic + "/" + strconv.Itoa(int(o.Partition))
			// Each reader keeps up with most of what was just written.
			at[key] = min(o.Offset, at[key]+(o.Offset-before[top.Key{Topic: o.Topic, Partition: o.Partition}])*9/10)
			os.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: at[key], LeaderEpoch: -1})
		})
		if err := adm.CommitAllOffsets(ctx, group, os); err != nil {
			t.Fatal(err)
		}
	}
	if err := tv.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	tv.cur.At = tv.prev.At.Add(2 * time.Second)
	tv.rows = top.Rates(tv.prev, tv.cur)
	tv.sortRows()
	save("top", "kaas top", tv.screen(true).Render(k, 100, 34))

	// Snapshot: the same path as the command, metadata then offsets.
	snap, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tp := range snap.Topics {
		names = append(names, tp.Name)
	}
	if starts, err := adm.ListStartOffsets(ctx, names...); err == nil {
		if ends, err := adm.ListEndOffsets(ctx, names...); err == nil {
			snap = snapshot.WithOffsets(snap, names, starts, ends, time.Now())
		}
	}
	rows, internal := topicRows(snap, "status", nil)
	save("snapshot", "kaas snapshot", snapshotScreen(snap, rows, internal, "status").Render(k, 100))

	// The TUI over the same cluster, with the events the engine just saw.
	s := e.Latest()
	var feed []engine.Event
	for i := len(view.events) - 1; i >= 0; i-- {
		feed = append(feed, view.events[i].Event)
	}
	for name, keys := range map[string][]string{
		"tui-topics":  nil,
		"tui-topic":   {":", "topic orders", "enter"},
		"tui-brokers": {":", "brokers", "enter"},
		"tui-404":     {":", "oders", "enter"},
	} {
		frame := tui.Frame(th, s, feed, time.Now(), 110, 32, keys...)
		save(name, "kaas", strings.Split(frame, "\n"))
	}
}

func s0(e *engine.Engine) *snapshot.Snapshot { return e.Latest() }

// movePartitionLeader hands a partition's leadership to another replica,
// so watch and the TUI show a real LeaderMoved event.
func movePartitionLeader(t *testing.T, c *kfake.Cluster, s *snapshot.Snapshot, topic string, p int32) {
	t.Helper()
	tp, ok := s.Topic(topic)
	if !ok {
		t.Fatalf("no topic %s", topic)
	}
	for _, part := range tp.Partitions {
		if part.ID != p {
			continue
		}
		for _, b := range s.Brokers {
			if b.ID != part.Leader {
				if err := c.MoveTopicPartition(topic, p, b.ID); err != nil {
					t.Fatal(err)
				}
				return
			}
		}
	}
}

// produceWaves writes to orders in bursts that rise and fall, so the
// sparklines have a shape, and a trickle to payments.
func produceWaves(ctx context.Context, seeds []string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.DefaultProduceTopic("orders"))
	if err != nil {
		return
	}
	defer cl.Close()
	for i := 0; ctx.Err() == nil; i++ {
		n := 20 + 30*(i%8)
		if i%8 > 4 {
			n = 20 + 30*(8-i%8)
		}
		for range n {
			cl.Produce(ctx, &kgo.Record{Value: []byte("order")}, nil)
		}
		if i%3 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "payments", Value: []byte("pay")}, nil)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
