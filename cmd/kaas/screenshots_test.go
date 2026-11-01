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
	"sync/atomic"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/core/bench"
	brokerload "github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/clients"
	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/sniff"
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
	// Capped, so each case runs long enough to chart its throughput;
	// kfake is in-memory and would otherwise report millions per second.
	b := produceRun(&cobra.Command{}, "orders",
		bench.Matrix([]int{100, 1024}, []string{"1", "all"}, []string{"none", "lz4"}, 30_000, 15_000),
		kgo.SeedBrokers(c.ListenAddrs()...))
	b.out = surface{w: io.Discard, raw: io.Discard, rich: true, width: 100, height: 60, kit: k}
	b.title, b.right = "bench produce · orders", "3 brokers · kfake"
	b.cmd.SetContext(ctx)
	b.lv = &live{s: b.out}
	if err := b.run(); err != nil {
		t.Fatal(err)
	}
	save("bench", "kaas bench produce -t orders --size 100,1024 --compression none,lz4", b.lv.last)

	// Bench consume: read back what the produce cases wrote, large
	// fetches against small ones. Each fetch is held 5ms, as a network
	// would, or in-memory kfake finishes before the chart has a point.
	var slowFetch atomic.Bool
	slowFetch.Store(true)
	c.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if slowFetch.Load() {
			c.SleepControl(func() { time.Sleep(5 * time.Millisecond) })
		}
		return nil, nil, false
	})
	cr := consumeRun(&cobra.Command{}, "orders",
		bench.FetchMatrix([]int{1 << 20, 256 << 10, 16 << 10}, []time.Duration{500 * time.Millisecond}, 100_000),
		kgo.SeedBrokers(c.ListenAddrs()...), 100_000)
	cr.out = b.out
	cr.title, cr.right = "bench consume · orders", "3 brokers · kfake"
	cr.cmd.SetContext(ctx)
	cr.lv = &live{s: cr.out}
	if err := cr.run(); err != nil {
		t.Fatal(err)
	}
	slowFetch.Store(false)
	save("bench-consume", "kaas bench consume -t orders --records 100000 --fetch-size 1MB,256KB,16KB", cr.lv.last)

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
	alter := alteration{td: td, to: len(td.topic.Partitions) + 2, readers: topicReaders(ctx, kadm.NewClient(cl), "users.profiles")}
	save("topics-alter", fmt.Sprintf("kaas topics alter users.profiles --partitions %d", alter.to), alter.screen().Render(k, 90))

	// Create: the wizard driven to its configs step, against the cluster.
	wsnap, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	wz := newWizard(creation{cl: cl, adm: kadm.NewClient(cl), snap: wsnap}, k)
	wz = drive(t, wz, tea.WindowSizeMsg{Width: 80, Height: 24})
	wz = drive(t, wz, typeKeys("payments.eu.chargebacks")...)
	wz = drive(t, wz, keys("enter")...)
	wz = drive(t, wz, keys("backspace", "backspace", "backspace")...)
	wz = drive(t, wz, typeKeys("12")...)
	wz = drive(t, wz, keys("enter", "left", "left", "down", "right", "right", "down")...)
	save("topics-create", "kaas topics create", wz.screen(80).Render(k, 80, 22))

	// Delete: the dialog over the dimmed topic, half typed; then the bin
	// as it closes. The topic stays: later screens need it.
	del, err := gatherDeletion(ctx, cl, []string{"users.profiles"})
	if err != nil {
		t.Fatal(err)
	}
	dm := deleteModel{d: del, k: k, now: time.Now}
	dm = driveDelete(t, dm, tea.WindowSizeMsg{Width: 90, Height: 34})
	dm = driveDelete(t, dm, typeKeys("users.prof")...)
	save("topics-delete", "kaas topics delete users.profiles", strings.Split(dm.View().Content, "\n"))
	del.results = map[string]error{"users.profiles": nil}
	save("topics-delete-done", "kaas topics delete users.profiles", del.card(0).Render(k, 56))

	// Produce --file: real files into a cluster slowed to 15ms a produce,
	// caught mid-upload; then the tray once every file is in.
	uc, ucl := kafkatest.Fake(t, kfake.SeedTopics(6, "orders"))
	uc.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		uc.KeepControl()
		uc.SleepControl(func() { time.Sleep(15 * time.Millisecond) })
		return nil, nil, false
	})
	dir := t.TempDir()
	var paths []string
	for _, f := range []struct {
		name  string
		lines int
	}{{"orders-2026-09.jsonl", 4_000}, {"orders-2026-10.jsonl", 30_000}, {"orders-2026-11.jsonl", 22_000}, {"refunds.jsonl", 9_000}, {"chargebacks.jsonl", 3_000}} {
		var b strings.Builder
		for i := range f.lines {
			fmt.Fprintf(&b, "user-%d:{\"order\":%d,\"total\":%d.90,\"currency\":\"EUR\"}\n", i%977, i, 10+i%300)
		}
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	up := newUpload(paths)
	uprod := producer{cl: ucl, topic: "orders", partition: -1, out: io.Discard}
	upDone := make(chan error, 1)
	go func() { upDone <- uprod.run(ctx, up, ":", surface{}, "5L6g3nSh") }()
	for {
		up.mu.Lock()
		first := up.files[0].State
		up.mu.Unlock()
		if first == screens.Sent {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	save("produce-file", "kaas produce orders -f orders-*.jsonl -f refunds.jsonl …",
		up.screen(uprod, ":", "5L6g3nSh").Render(k, 72, 26))
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	save("produce-file-done", "kaas produce orders -f …", up.tray("orders").Render(k, 56))

	// Offsets reset: six hours of records a minute apart, billing near the
	// end of orders and never on payments; the wizard at where-to, then
	// the plan for 2h back, committed.
	oc, ocl := kafkatest.Fake(t, kfake.SeedTopics(4, "orders"), kfake.SeedTopics(2, "payments"))
	ocm, err := kgo.NewClient(kgo.SeedBrokers(oc.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	var orecs []*kgo.Record
	for _, tp := range []struct {
		topic string
		parts int32
		every time.Duration
	}{{"orders", 4, 40 * time.Second}, {"payments", 2, 3 * time.Minute}} {
		for p := range tp.parts {
			for at := now.Add(-6 * time.Hour); at.Before(now); at = at.Add(tp.every + time.Duration(p)*time.Second) {
				orecs = append(orecs, &kgo.Record{Topic: tp.topic, Partition: p, Timestamp: at, Value: []byte("{}")})
			}
		}
	}
	if err := ocm.ProduceSync(ctx, orecs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ocm.Close()
	oadm := kadm.NewClient(ocl)
	ends, err := oadm.ListEndOffsets(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	commits := kadm.Offsets{}
	for p, back := range []int64{3, 0, 41, 12} {
		e, _ := ends.Lookup("orders", int32(p))
		commits.Add(kadm.Offset{Topic: "orders", Partition: int32(p), At: e.Offset - back, LeaderEpoch: -1})
	}
	for _, g := range []string{"billing", "ledger-sync", "notifications"} {
		if _, err := oadm.CommitOffsets(ctx, g, commits); err != nil {
			t.Fatal(err)
		}
	}
	rwz, err := newResetWizard(ctx, oadm, k, resetAsk{})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range keys("enter", "enter", "right", "right") {
		next, _ := rwz.Update(msg)
		rwz = next.(resetWizard)
	}
	save("offsets-reset-wizard", "kaas offsets reset", rwz.screen(80).Render(k, 80, 22))
	two := offsets.Target{Kind: offsets.Time, At: now.Add(-2 * time.Hour)}
	oplan, err := offsets.PlanReset(ctx, oadm, "billing", []string{"orders", "payments"}, two)
	if err != nil {
		t.Fatal(err)
	}
	ors := reset{plan: oplan, cmdline: "kaas offsets reset billing -t orders -t payments --to 2h"}
	start := time.Now()
	if ors.results, err = offsets.Apply(ctx, oadm, oplan); err != nil {
		t.Fatal(err)
	}
	ors.took, ors.applied = time.Since(start), true
	save("offsets-reset", ors.cmdline, ors.screen("applied", kit.OK).Render(k, 90))

	// Offsets find: a real bisect over 12,480 order records a second
	// apart, for where 90 minutes ago starts.
	fc, fcl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	fbase := now.Add(-12480 * time.Second)
	seedTimeline(t, fc, "orders", []int32{0}, 12480, fbase, func(_ int32, i int) string {
		return fmt.Sprintf(`{"order":%d,"total":%d.90,"currency":"EUR"}`, 104300+i, 10+i%300)
	})
	ff := finder{topic: "orders", at: now.Add(-90 * time.Minute).Truncate(time.Minute)}
	ff.cmdline = "kaas offsets find orders --time " + ff.at.Format("2006-01-02T15:04")
	fcmd := newOffsetsFindCmd(new(string), new(string))
	fcmd.SetContext(ctx)
	if err := ff.load(ctx, fcmd, kadm.NewClient(fcl), nil); err != nil {
		t.Fatal(err)
	}
	if err := ff.run(fcmd, fcl, surface{kit: k, w: io.Discard, width: 88, height: 27}, "text"); err != nil {
		t.Fatal(err)
	}
	save("offsets-find", ff.cmdline, ff.screen(ff.hunts[0]).Render(k, 88, 26))

	// Consume --pick: three weeks of orders, quieter at weekends and none
	// on one day, with the last week but two picked.
	pc, pcl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	var stamps []time.Time
	for d := midnight(now).AddDate(0, 0, -23); d.Before(now); d = d.AddDate(0, 0, 1) {
		n := 40 + d.Day()%7*9
		if wd := d.Weekday(); wd == time.Saturday || wd == time.Sunday {
			n /= 4
		}
		if d.Equal(midnight(now).AddDate(0, 0, -12)) {
			n = 0 // an outage
		}
		for i := range n {
			if at := d.Add(time.Duration(i) * 24 * time.Hour / time.Duration(n)); at.Before(now) {
				stamps = append(stamps, at)
			}
		}
	}
	seedAt(t, pc, "orders", stamps)
	padm := kadm.NewClient(pcl)
	poldest, err := consume.Oldest(ctx, pcl, padm, "orders")
	if err != nil {
		t.Fatal(err)
	}
	pm := newPickModel(k, padm, "orders", now, poldest, consume.From{})
	run := func(cmd tea.Cmd) {
		for cmd != nil {
			next, more := pm.Update(cmd())
			pm, cmd = next.(pickModel), more
		}
	}
	run(pm.Init())
	for _, msg := range keys("up", "right", "enter", "right", "right", "right", "right", "enter") {
		next, cmd := pm.Update(msg)
		pm = next.(pickModel)
		run(cmd)
	}
	save("consume-pick", "kaas consume orders --pick", pm.screen(80).Render(k, 80))

	// Sniff: a real proxy in front of the cluster, a producer and a group
	// consumer going through it, and a produce request selected.
	sc := newCapture(nil)
	sp, err := sniff.Start(ctx, cl, "127.0.0.1:0", sc.add)
	if err != nil {
		t.Fatal(err)
	}
	checkout, err := kgo.NewClient(kgo.SeedBrokers(sp.Addr()), kgo.ClientID("checkout-api"), kgo.ProducerLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		var rs []*kgo.Record
		for j := range 3 + i {
			rs = append(rs, &kgo.Record{Topic: "orders", Key: []byte("cust-" + strconv.Itoa(j)), Value: []byte(`{"amount":12.5}`)})
		}
		if err := checkout.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	checkout.Close()
	billing, err := kgo.NewClient(kgo.SeedBrokers(sp.Addr()), kgo.ClientID("billing-svc"),
		kgo.ConsumerGroup("billing-sniff"), kgo.ConsumeTopics("orders"), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		pctx, pcancel := context.WithTimeout(ctx, time.Second)
		billing.PollFetches(pctx)
		_ = billing.CommitUncommittedOffsets(pctx)
		pcancel()
	}
	billing.Close()
	sm := newSniffModel(k, sp, sc, "all requests", 0)
	next, _ := sm.Update(sniffTick(time.Now()))
	sm = next.(sniffModel)
	for i := len(sm.reqs) - 1; i >= 0; i-- {
		if sm.reqs[i].Key == 0 { // the last produce
			sm.sel = i
			break
		}
	}
	sm.rate = 18400
	sp.Close()
	save("sniff", "kaas sniff", sm.screen().Render(k, 100, 30))

	// Sniff --trace: a service starting up through the proxy, its group
	// joined and its first write acked, timed stage by stage.
	tc := newCapture(nil)
	tp, err := sniff.StartWith(ctx, cl, sniff.Options{Listen: "127.0.0.1:0", Out: tc.add, Sent: tc.addSent, Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	tm := newTraceModel(k, tp, tc, "billing-svc", 10*time.Second, 0)
	svc, err := kgo.NewClient(kgo.SeedBrokers(tp.Addr()), kgo.ClientID("billing-svc"),
		kgo.ConsumerGroup("billing-trace"), kgo.ConsumeTopics("orders"), kgo.FetchMaxWait(300*time.Millisecond),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatal(err)
	}
	tctx, tcancel := context.WithTimeout(ctx, 700*time.Millisecond)
	svc.PollFetches(tctx)
	tcancel()
	if err := svc.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("billed")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	tctx, tcancel = context.WithTimeout(ctx, 400*time.Millisecond)
	svc.PollFetches(tctx)
	tcancel()
	next, _ = tm.Update(sniffTick(time.Now()))
	tm = next.(traceModel)
	tm.over = true
	for i, r := range tm.reqs {
		if r.Key == 14 {
			tm.sel = i // the SyncGroup that joined it
		}
	}
	svc.Close()
	tp.Close()
	tf := tm.screen()
	tf.Hint = ""
	save("sniff-trace", "kaas sniff --trace --client-id billing-svc", tf.Render(k, 104, 38))

	// Sniff --profile: producers on two topics and a consumer group, through
	// the proxy, added up by API, broker, topic and stage.
	pcap := newCapture(nil)
	pp, err := sniff.StartWith(ctx, cl, sniff.Options{Listen: "127.0.0.1:0", Out: pcap.add, Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	fm := newProfileModel(k, pp, pcap, "every client", 0, 0)
	prod, err := kgo.NewClient(kgo.SeedBrokers(pp.Addr()), kgo.ClientID("checkout-api"), kgo.ProducerLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	burst := func(n int) {
		var rs []*kgo.Record
		for i := range n {
			topic := "orders"
			if i%3 == 0 {
				topic = "payments"
			}
			rs = append(rs, &kgo.Record{Topic: topic, Key: []byte("cust-" + strconv.Itoa(i%17)), Value: []byte(`{"amount":12.5,"currency":"EUR"}`)})
		}
		if err := prod.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
		next, _ = fm.Update(sniffTick(time.Now())) // drain as the live screen does
		fm = next.(profileModel)
	}
	// A backlog first, so the consumer's fetches find records waiting.
	for range 40 {
		burst(30)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(pp.Addr()), kgo.ClientID("billing-svc"),
		kgo.ConsumerGroup("billing-profile"), kgo.ConsumeTopics("orders", "payments"),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithCancel(ctx)
	rdone := make(chan struct{})
	go func() {
		defer close(rdone)
		for rctx.Err() == nil {
			reader.PollFetches(rctx)
		}
	}()
	for range 60 {
		burst(12)
	}
	time.Sleep(300 * time.Millisecond)
	rcancel()
	<-rdone
	reader.Close()
	prod.Close()
	next, _ = fm.Update(sniffTick(time.Now()))
	fm = next.(profileModel)
	pp.Close()
	pfull, _ := fm.view(fm.prof.Root)
	for _, api := range pfull.Kids {
		if api.Name == "Produce" {
			// The hottest stage of the busiest produce path.
			b := api.Kids[0]
			fm.sel = []string{"Produce", b.Name, b.Kids[0].Name, b.Kids[0].Kids[0].Name}
		}
	}
	save("sniff-profile", "kaas sniff --profile", fm.screen().Render(k, 100, 26))

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

	// Clients: two fulfilment members and search-index from above as
	// consumers, two transactional producers mid-transaction (the only
	// producers kfake describes), over a 2s window.
	var members []*kgo.Client
	for range 2 {
		m, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ClientID("fulfilment-svc"),
			kgo.ConsumerGroup("fulfilment"), kgo.ConsumeTopics("orders"), kgo.DisableAutoCommit())
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		members = append(members, m)
	}
	var txns []*kgo.Client
	for _, id := range []string{"orders-api", "ledger"} {
		p, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID(id))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		txns = append(txns, p)
	}
	write := func(n int) {
		for i := range n {
			txns[0].Produce(ctx, &kgo.Record{Topic: "orders", Key: []byte(strconv.Itoa(i % 6)), Value: []byte("an order")}, nil)
			if i%3 == 0 {
				txns[1].Produce(ctx, &kgo.Record{Topic: "payments", Key: []byte(strconv.Itoa(i % 3)), Value: []byte("a payment")}, nil)
			}
		}
		for _, p := range txns {
			if err := p.Flush(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	read := func() {
		for _, m := range append(members, live) {
			pctx, pcancel := context.WithTimeout(ctx, time.Second)
			m.PollFetches(pctx)
			_ = m.CommitUncommittedOffsets(pctx)
			pcancel()
		}
	}
	write(600)
	read()
	clv := &clientView{cl: cl, adm: adm, sortBy: "rate", interval: 2 * time.Second}
	var clHistory []float64
	for i := range 12 {
		write(1200 + 300*(i%3))
		read()
		if err := clv.refresh(ctx); err != nil {
			t.Fatal(err)
		}
		clv.cur.At = base.Add(time.Duration(i) * 2 * time.Second)
		if i > 0 {
			var total float64
			for j := range clv.rows {
				clv.rows[j].Rate = clients.Rate(clv.prev, clv.cur, clv.rows[j].Client)
				total += max(0, clv.rows[j].Rate)
			}
			clHistory = append(clHistory, total)
			clv.sort()
		}
	}
	clv.history = clHistory
	save("clients", "kaas clients", clv.screen(true).Render(k, 100))

	// Produce: a record typed into the editor key by key, sent, then a
	// field changed for the next one.
	ed := newEditor(producer{cl: cl, topic: "orders", partition: -1}, k)
	ed, _ = sendEditor(ed, tea.WindowSizeMsg{Width: 100, Height: 24})
	ed, _ = sendEditor(ed, typeKeys("\tuser-17\tsource=checkout\ntrace-id=4bf92f3577b34da6\t")...)
	ed, _ = sendEditor(ed, typeKeys(`{`+"\n"+`"order_id": "A-1043",`+"\n"+`"customer": "user-17",`+"\n"+
		`"items": [{"sku": "B-220", "qty": 2}, {"sku": "C-118", "qty": 1}],`+"\n"+`"total": 49.90,`+"\n"+
		`"gift": false,`+"\n"+`"note": null`+"\n"+`}`)...)
	ed, sendCmd := sendEditor(ed, tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl})
	ed, _ = sendEditor(ed, sendCmd())
	ed, _ = sendEditor(ed, tea.KeyPressMsg{Code: tea.KeyUp}, tea.KeyPressMsg{Code: tea.KeyUp}, tea.KeyPressMsg{Code: tea.KeyUp})
	ed, _ = sendEditor(ed, tea.KeyPressMsg{Code: tea.KeyEnd})
	ed, _ = sendEditor(ed, typeKeys("\b\b\b\b\b\b59.90,")...)
	save("produce", "kaas produce orders", ed.screen().Render(k, 100, 22))

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
