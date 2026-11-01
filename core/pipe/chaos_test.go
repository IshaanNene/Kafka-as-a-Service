//go:build chaos

package pipe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// Chaos tests disturb a real broker while records flow: the process
// frozen mid-send, latency on every write, members joining and leaving.
// Each asserts what a pipe promises: nothing acknowledged is lost, and
// duplicates come only from at-least-once redelivery, never from a retry.

// A broker frozen while a pipe is producing: franz-go keeps the batches
// in flight and resends them with the same sequence numbers once it
// answers, so the idempotent producer writes each record exactly once.
func TestChaosPauseMidStream(t *testing.T) {
	b := kafkatest.RealBroker(t)
	topic(t, b.Seeds, "frozen")
	env := Env{Brokers: b.Seeds}
	in, err := OpenSource("gen:orders?count=6000&rate=1500&seed=11", env)
	if err != nil {
		t.Fatal(err)
	}
	out, err := OpenSink("kafka:frozen", env)
	if err != nil {
		t.Fatal(err)
	}
	p := New([]Endpoint{in}, []Endpoint{out}, Options{})
	go func() {
		waitFor(t, func() bool { return p.Stats().Written > 1000 })
		b.Pause(t)
		t.Logf("paused at %d written", p.Stats().Written)
		time.Sleep(8 * time.Second)
		b.Unpause(t)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	start := time.Now()
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 8*time.Second {
		t.Errorf("finished in %s: the pause didn't hold it up", took)
	}
	got := readAll(t, b.Seeds, "frozen")
	if len(got) != 6000 {
		t.Fatalf("%d distinct records, want 6000", len(got))
	}
	for v, n := range got {
		if n != 1 {
			t.Fatalf("%s written %d times through the pause", v[:40], n)
		}
	}
}

// The case where a send's outcome is unknown: the broker freezes with a
// batch in flight and the pipe is told to stop. franz-go doesn't give up
// on a request it has sent, so the write waits for the broker; once it
// answers, the pipe still acknowledges what was written, so the next run
// starts after it. Nothing is lost, and any record that does land twice
// is counted.
func TestChaosStopDuringPause(t *testing.T) {
	b := kafkatest.RealBroker(t)
	topic(t, b.Seeds, "in", "out")
	seed(t, b.Seeds, "in", 5000)
	env := Env{Brokers: b.Seeds}
	spec := "kafka:in?group=unsure&from=start&stop=end&batch=200"

	in, _ := OpenSource(spec, env)
	out, _ := OpenSink("kafka:out", env)
	ctx, stop := context.WithCancel(context.Background())
	unpaused := make(chan struct{})
	freeze := &onWrite{Sink: out.Sink, at: 4, do: func() {
		b.Pause(t) // the fourth batch goes out to a frozen broker
		go func() {
			defer close(unpaused)
			time.Sleep(time.Second)
			stop() // Ctrl-C while it can't answer
			time.Sleep(3 * time.Second)
			b.Unpause(t)
		}()
	}}
	p := New([]Endpoint{in}, []Endpoint{{Spec: out.Spec, Sink: freeze}}, Options{})
	start := time.Now()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
	<-unpaused
	first := p.Stats()
	if took := time.Since(start); took < 4*time.Second || first.Acked != first.Written {
		t.Errorf("first run took %s, wrote %d, acked %d: it should wait out the pause and ack what it wrote", took, first.Written, first.Acked)
	}

	in, _ = OpenSource(spec, env)
	out, _ = OpenSink("kafka:out", env)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p2 := New([]Endpoint{in}, []Endpoint{out}, Options{})
	if err := p2.Run(ctx2); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, b.Seeds, "out")
	dups := 0
	for i := range 5000 {
		switch n := got[fmt.Sprint(i)]; {
		case n == 0:
			t.Fatalf("record %d lost", i)
		case n > 1:
			dups += n - 1
		}
	}
	t.Logf("stopped mid-pause: first run wrote and acked %d, second wrote %d, %d landed twice, none lost", first.Acked, p2.Stats().Written, dups)
}

// onWrite runs do before the at-th write.
type onWrite struct {
	Sink
	at, n int
	do    func()
}

func (s *onWrite) Write(ctx context.Context, rs []Record) error {
	if s.n++; s.n == s.at {
		s.do()
	}
	return s.Sink.Write(ctx, rs)
}

// Latency on every write to the broker, as a slow link or a busy broker
// adds it: the copy slows but stays whole.
func TestChaosLatency(t *testing.T) {
	b := kafkatest.RealBroker(t)
	start := time.Now()
	checkCopy(t, b.Seeds, kgo.Dialer(slowDialer(25*time.Millisecond)))
	t.Logf("copied 1,000 records with 25ms on every write in %s", time.Since(start).Round(time.Millisecond))
}

// Members join and leave a group mid-stream on a real broker.
func TestChaosRebalance(t *testing.T) {
	b := kafkatest.RealBroker(t)
	checkRebalance(t, b.Seeds, 50_000)
}
