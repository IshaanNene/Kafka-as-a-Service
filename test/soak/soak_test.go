//go:build soak

// Package soak runs pipes for a long time against a real broker and
// checks they stay healthy: lag bounded, memory and goroutines flat,
// every record delivered. It is the soak test of a release:
//
//	KAAS_SOAK_DURATION=30m KAAS_SOAK_REPORT=soak.md go test -tags soak ./test/soak -timeout 2h
//
// KAAS_SOAK_BROKERS points it at an existing cluster instead of a
// container; KAAS_SOAK_RATE sets the records a second (default 2,000).
package soak

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/pipe"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

type sample struct {
	at                  time.Duration
	produced, delivered int64
	lag                 int64
	heapMiB             float64
	goroutines          int
}

func TestSoak(t *testing.T) {
	dur := 2 * time.Minute
	if v := os.Getenv("KAAS_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		dur = d
	}
	rate := 2000
	if v := os.Getenv("KAAS_SOAK_RATE"); v != "" {
		rate, _ = strconv.Atoi(v)
	}
	var seeds []string
	if v := os.Getenv("KAAS_SOAK_BROKERS"); v != "" {
		seeds = strings.Split(v, ",")
	} else {
		seeds = kafkatest.Real(t)
	}
	run := fmt.Sprint(time.Now().Unix())
	in, out, group := "soak-in-"+run, "soak-out-"+run, "soak-"+run
	adm := kafkatest.Admin(t, seeds...)
	if _, err := adm.CreateTopics(context.Background(), 6, 1, nil, in, out); err != nil {
		t.Fatal(err)
	}

	env := pipe.Env{Brokers: seeds}
	open := func(from, to string) *pipe.Pipe {
		src, err := pipe.OpenSource(from, env)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := pipe.OpenSink(to, env)
		if err != nil {
			t.Fatal(err)
		}
		return pipe.New([]pipe.Endpoint{src}, []pipe.Endpoint{dst}, pipe.Options{})
	}
	// A loads the input topic at a steady rate; B moves it on, as a
	// consumer group, the whole time.
	load := open(fmt.Sprintf("gen:orders?rate=%d&seed=1", rate), "kafka:"+in)
	move := open("kafka:"+in+"?group="+group+"&from=start", "kafka:"+out)
	lctx, stopLoad := context.WithTimeout(context.Background(), dur)
	defer stopLoad()
	mctx, stopMove := context.WithCancel(context.Background())
	defer stopMove()
	loadErr, moveErr := make(chan error, 1), make(chan error, 1)
	go func() { loadErr <- load.Run(lctx) }()
	go func() { moveErr <- move.Run(mctx) }()

	var samples []sample
	start := time.Now()
	every := max(dur/30, 5*time.Second)
	tick := time.NewTicker(every)
	defer tick.Stop()
	take := func() sample {
		var ms runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&ms)
		l, m := load.Stats(), move.Stats()
		return sample{at: time.Since(start).Round(time.Second), produced: l.Written, delivered: m.Acked,
			lag: l.Written - m.Acked, heapMiB: float64(ms.HeapInuse) / (1 << 20), goroutines: runtime.NumGoroutine()}
	}
loop:
	for {
		select {
		case err := <-loadErr:
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			break loop
		case err := <-moveErr:
			t.Fatalf("the moving pipe stopped: %v", err)
		case <-tick.C:
			s := take()
			samples = append(samples, s)
			t.Logf("%8s produced %9d delivered %9d lag %7d heap %6.1f MiB goroutines %d", s.at, s.produced, s.delivered, s.lag, s.heapMiB, s.goroutines)
		}
	}
	// Drain: once the load stops, B catches up.
	produced := load.Stats().Written
	deadline := time.Now().Add(2 * time.Minute)
	for move.Stats().Acked < produced {
		if time.Now().After(deadline) {
			t.Fatalf("delivered %d of %d two minutes after the load stopped", move.Stats().Acked, produced)
		}
		time.Sleep(200 * time.Millisecond)
	}
	stopMove()
	if err := <-moveErr; err != nil {
		t.Fatal(err)
	}
	samples = append(samples, take())

	// Every record arrived.
	ends, err := adm.ListEndOffsets(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	var landed int64
	ends.Each(func(o kadm.ListedOffset) { landed += o.Offset })
	if landed < produced {
		t.Errorf("%d records in %s, %d produced", landed, out, produced)
	}
	// Lag stayed within ten seconds of load; heap and goroutines flat.
	n := len(samples)
	if n >= 6 {
		var maxLag int64
		for _, s := range samples {
			maxLag = max(maxLag, s.lag)
		}
		if maxLag > int64(rate*10) {
			t.Errorf("lag reached %d, more than ten seconds of load", maxLag)
		}
		avg := func(ss []sample) (heap float64, gr int) {
			for _, s := range ss {
				heap += s.heapMiB
				gr += s.goroutines
			}
			return heap / float64(len(ss)), gr / len(ss)
		}
		h1, g1 := avg(samples[1 : n/3])
		h3, g3 := avg(samples[2*n/3 : n-1])
		if h3 > 2*h1+16 {
			t.Errorf("heap grew from %.1f to %.1f MiB: a leak", h1, h3)
		}
		if g3 > g1+50 {
			t.Errorf("goroutines grew from %d to %d", g1, g3)
		}
	}
	report(t, dur, rate, produced, landed, samples)
}

// report writes the run as a Markdown table, the evidence a release keeps.
func report(t *testing.T, dur time.Duration, rate int, produced, landed int64, ss []sample) {
	path := os.Getenv("KAAS_SOAK_REPORT")
	if path == "" {
		return
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Soak: %s at %d records/s\n\n", dur, rate)
	fmt.Fprintf(&sb, "%d produced, %d landed, Go %s, %s\n\n", produced, landed, runtime.Version(), time.Now().UTC().Format(time.RFC3339))
	sb.WriteString("| at | produced | delivered | lag | heap MiB | goroutines |\n|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range ss {
		fmt.Fprintf(&sb, "| %s | %d | %d | %d | %.1f | %d |\n", s.at, s.produced, s.delivered, s.lag, s.heapMiB, s.goroutines)
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Error(err)
	}
}
