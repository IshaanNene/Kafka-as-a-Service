package pipe

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// These checks run here on kfake, and on a real broker in the
// integration and chaos tiers (integration_test.go), against the same
// properties: what a pipe acknowledges is written, and what it doesn't
// comes round again.

func fakeCluster(t *testing.T, topics ...string) []string {
	t.Helper()
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(6, topics...))
	return c.ListenAddrs()
}

// seed writes n records keyed k-0… with values 0… to topic.
func seed(t testing.TB, seeds []string, topic string, n int) {
	t.Helper()
	cl := kafkatest.Client(t, seeds...)
	rs := make([]*kgo.Record, n)
	for i := range rs {
		rs[i] = &kgo.Record{Topic: topic, Key: []byte(fmt.Sprint("k-", i)), Value: []byte(fmt.Sprint(i))}
	}
	if err := cl.ProduceSync(context.Background(), rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// readAll is every value in topic, counted.
func readAll(t testing.TB, seeds []string, topic string) map[string]int {
	t.Helper()
	cl := kafkatest.Client(t, seeds...)
	ends, err := kadm.NewClient(cl).ListEndOffsets(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	got := map[string]int{}
	if total == 0 {
		return got
	}
	c := kafkatest.Client(t, append([]string(nil), seeds...)...)
	c.AddConsumeTopics(topic)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int64
	for n < total {
		fs := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d from %s", n, total, topic)
		}
		fs.EachRecord(func(r *kgo.Record) { got[string(r.Value)]++; n++ })
	}
	return got
}

// checkCopy copies a topic to another with stop=end and checks every
// record arrived once.
func checkCopy(t *testing.T, seeds []string, opts ...kgo.Opt) {
	topic(t, seeds, "src", "dst")
	seed(t, seeds, "src", 1000)
	env := Env{Brokers: seeds, KafkaOpts: opts}
	in, err := OpenSource("kafka:src?stop=end&batch=128", env)
	if err != nil {
		t.Fatal(err)
	}
	out, err := OpenSink("kafka:dst", env)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := New([]Endpoint{in}, []Endpoint{out}, Options{})
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := p.Stats(); st.Written != 1000 || !st.Sources[0].Done {
		t.Fatalf("stats %+v", st)
	}
	got := readAll(t, seeds, "dst")
	if len(got) != 1000 {
		t.Fatalf("dst has %d distinct records, want 1000", len(got))
	}
	for v, n := range got {
		if n != 1 {
			t.Errorf("%s written %d times", v, n)
		}
	}
}

// checkResume runs a group pipe whose sink fails part way, then runs it
// again: the second run starts at the first's last acknowledged batch,
// so nothing is lost, and only the failed batch can repeat.
func checkResume(t *testing.T, seeds []string) {
	topic(t, seeds, "events")
	seed(t, seeds, "events", 600)
	env := Env{Brokers: seeds}
	run := func(sink Sink) error {
		in, err := OpenSource("kafka:events?group=resume&from=start&stop=end&batch=50", env)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return New([]Endpoint{in}, []Endpoint{{Spec: "mem", Sink: sink}}, Options{}).Run(ctx)
	}
	first := &memSink{fail: 4}
	if err := run(first); err == nil {
		t.Fatal("the failing sink didn't fail the run")
	}
	second := &memSink{}
	if err := run(second); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, v := range append(first.values(), second.values()...) {
		seen[v]++
	}
	dups := 0
	for i := range 600 {
		switch n := seen[fmt.Sprint(i)]; {
		case n == 0:
			t.Fatalf("record %d lost", i)
		case n > 1:
			dups++
		}
	}
	// The second run redelivers what the first read but didn't commit:
	// at most the batches in flight across partitions when it failed.
	if dups > 300 {
		t.Errorf("%d duplicates", dups)
	}
	t.Logf("first run wrote %d, second %d, %d redelivered", len(first.values()), len(second.values()), dups)
}

// checkRebalance runs pipes in one group, adding and stopping members
// while records flow, and checks every record reached the sink: a
// stopped member's uncommitted batches go to the others.
func checkRebalance(t *testing.T, seeds []string, total int) {
	const name = "rebalance"
	topic(t, seeds, name)
	seed(t, seeds, name, total)
	sink := &memSink{}
	var wg sync.WaitGroup
	env := Env{Brokers: seeds}
	start := func(ctx context.Context) {
		in, err := OpenSource("kafka:"+name+"?group=rb&from=start&batch=25", env)
		if err != nil {
			t.Error(err)
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := New([]Endpoint{in}, []Endpoint{{Spec: "mem", Sink: sink}}, Options{}).Run(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, stopA := context.WithCancel(ctx)
	start(a)
	start(ctx)
	waitFor(t, func() bool { return len(sink.values()) > total/5 })
	start(ctx) // a third joins mid-stream
	waitFor(t, func() bool { return len(sink.values()) > total/2 })
	stopA() // and the first leaves
	waitFor(t, func() bool { return distinct(sink.values()) == total })
	cancel()
	wg.Wait()
	all := sink.values()
	t.Logf("%d records, %d writes for them: %d redelivered across rebalances", total, len(all), len(all)-total)
}

func distinct(vs []string) int {
	m := map[string]bool{}
	for _, v := range vs {
		m[v] = true
	}
	return len(m)
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// topic creates topics with six partitions; one that exists, as kfake's
// seeded topics do, is left as it is.
func topic(t testing.TB, seeds []string, names ...string) {
	t.Helper()
	adm := kafkatest.Admin(t, seeds...)
	if _, err := adm.CreateTopics(context.Background(), 6, 1, nil, names...); err != nil {
		t.Fatal(err)
	}
}

// slowDialer adds d before every write on a connection: latency injected
// at the client, as Toxiproxy would add it in front of the broker.
func slowDialer(d time.Duration) func(ctx context.Context, network, host string) (net.Conn, error) {
	return func(ctx context.Context, network, host string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, host)
		if err != nil {
			return nil, err
		}
		return slowConn{c, d}, nil
	}
}

type slowConn struct {
	net.Conn
	d time.Duration
}

func (c slowConn) Write(b []byte) (int, error) {
	time.Sleep(c.d)
	return c.Conn.Write(b)
}

func TestKafkaCopy(t *testing.T) { checkCopy(t, fakeCluster(t, "src", "dst")) }

func TestKafkaResume(t *testing.T) { checkResume(t, fakeCluster(t, "events")) }

func TestKafkaRebalance(t *testing.T) { checkRebalance(t, fakeCluster(t, "rebalance"), 3000) }

func TestKafkaLatency(t *testing.T) {
	seeds := fakeCluster(t, "src", "dst")
	start := time.Now()
	checkCopy(t, seeds, kgo.Dialer(slowDialer(10*time.Millisecond)))
	if took := time.Since(start); took < 50*time.Millisecond {
		t.Errorf("a copy with 10ms on every write took %s", took)
	}
}

// A broker refusing a produce, as MockProducer's errorNext does in Java:
// the write fails, the batch isn't committed, and a rerun writes it.
func TestKafkaProduceRefused(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(1), kfake.SeedTopics(3, "in", "out"))
	seeds := c.ListenAddrs()
	seed(t, seeds, "in", 300)
	var refused atomic.Int32
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if refused.Load() >= 2 { // refuse the first two produces, then let them by
			return nil, nil, false
		}
		refused.Add(1)
		pr := req.(*kmsg.ProduceRequest)
		resp := pr.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range pr.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.InvalidRecord.Code // not retriable: the client gives up
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	env := Env{Brokers: seeds}
	run := func() error {
		in, err := OpenSource("kafka:in?group=g&from=start&stop=end&batch=100", env)
		if err != nil {
			t.Fatal(err)
		}
		out, err := OpenSink("kafka:out", env)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return New([]Endpoint{in}, []Endpoint{out}, Options{}).Run(ctx)
	}
	if err := run(); err == nil {
		t.Fatalf("a refused produce didn't fail the pipe (%d refused)", refused.Load())
	}
	for refused.Load() < 2 {
		run() //nolint:errcheck // until the broker stops refusing
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, seeds, "out")
	for i := range 300 {
		if got[fmt.Sprint(i)] == 0 {
			t.Fatalf("record %d lost after the refusal", i)
		}
	}
}
