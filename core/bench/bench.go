// Package bench measures produce throughput and latency against a real
// cluster, the way kafka-producer-perf-test does, but as a matrix of cases
// with live progress for a renderer to draw.
package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

// Case is one cell of the benchmark matrix.
type Case struct {
	Size        int    `json:"size"`        // value bytes per record
	Acks        string `json:"acks"`        // "0", "1" or "all"
	Compression string `json:"compression"` // none, gzip, snappy, lz4, zstd
	Records     int    `json:"records"`
	Throughput  int    `json:"throughput"` // records/sec cap; 0 is unlimited
}

// Name is the case's label in tables: "100B acks=all none".
func (c Case) Name() string {
	return fmt.Sprintf("%s acks=%s %s", Bytes(int64(c.Size)), c.Acks, c.Compression)
}

// Matrix is every combination of sizes, acks and codecs, in that nesting.
func Matrix(sizes []int, acks, codecs []string, records, throughput int) []Case {
	var out []Case
	for _, s := range sizes {
		for _, a := range acks {
			for _, c := range codecs {
				out = append(out, Case{Size: s, Acks: a, Compression: c, Records: records, Throughput: throughput})
			}
		}
	}
	return out
}

// Validate reports the first field a case can't run with.
func (c Case) Validate() (field, msg string) {
	switch {
	case c.Size <= 0:
		return "size", "must be positive"
	case c.Records <= 0:
		return "records", "must be positive"
	case c.Throughput < 0:
		return "throughput", "must be 0 (unlimited) or positive"
	}
	if _, ok := acks[c.Acks]; !ok {
		return "acks", "must be one of 0, 1, all"
	}
	if _, ok := codecs[c.Compression]; !ok {
		return "compression", "must be one of none, gzip, snappy, lz4, zstd"
	}
	return "", ""
}

var acks = map[string]kgo.Acks{"0": kgo.NoAck(), "1": kgo.LeaderAck(), "all": kgo.AllISRAcks()}

var codecs = map[string]kgo.CompressionCodec{
	"none": kgo.NoCompression(), "gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(),
	"lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression(),
}

// Buckets are the latency histogram's upper bounds; the last is open.
var Buckets = []time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
	500 * time.Millisecond,
}

// Progress is a running case's state, reported every tick.
type Progress struct {
	Sent, Acked, Errors int64
	Elapsed             time.Duration
	Hist                []int64   // len(Buckets)+1 counts
	Rate                []float64 // acked records/sec, one per tick window
}

// Result is a finished case.
type Result struct {
	Case       Case          `json:"case"`
	Records    int64         `json:"records"`
	Errors     int64         `json:"errors"`
	Bytes      int64         `json:"bytes"`
	Took       time.Duration `json:"took"`
	RecsPerSec float64       `json:"records_per_sec"`
	MBPerSec   float64       `json:"mb_per_sec"`
	Avg        time.Duration `json:"avg"`
	P50        time.Duration `json:"p50"`
	P95        time.Duration `json:"p95"`
	P99        time.Duration `json:"p99"`
	P999       time.Duration `json:"p999"`
	Max        time.Duration `json:"max"`
	Hist       []int64       `json:"histogram"` // counts per Buckets, plus overflow
	FirstError string        `json:"first_error,omitempty"`
}

// Options tunes a run.
type Options struct {
	Tick   time.Duration // progress interval; default 100ms
	Report func(Progress)
	Client []kgo.Opt // seeds, TLS, SASL
}

// Produce runs one case against topic and measures every record's ack
// latency. Each case gets its own client, since acks and compression are
// producer-wide settings.
func Produce(ctx context.Context, topic string, c Case, o Options) (Result, error) {
	if f, msg := c.Validate(); f != "" {
		return Result{}, fmt.Errorf("%s %s", f, msg)
	}
	if o.Tick == 0 {
		o.Tick = 100 * time.Millisecond
	}
	opts := append([]kgo.Opt{
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(acks[c.Acks]),
		kgo.ProducerBatchCompression(codecs[c.Compression]),
		kgo.MaxBufferedRecords(max(10_000, c.Records/10)),
	}, o.Client...)
	if c.Acks != "all" {
		opts = append(opts, kgo.DisableIdempotentWrite()) // idempotence requires acks=all
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return Result{}, err
	}
	defer cl.Close()

	value := make([]byte, c.Size)
	_, _ = rand.Read(value) // incompressible, like perf-test's random payload

	// One untimed record first: metadata, connections and the producer ID
	// are one-off costs that would otherwise land on the first records.
	if err := cl.ProduceSync(ctx, &kgo.Record{Value: value}).FirstErr(); err != nil {
		return Result{Case: c, FirstError: err.Error()}, fmt.Errorf("warm-up: %w", err)
	}

	var (
		mu       sync.Mutex
		lat      = make([]time.Duration, 0, c.Records)
		hist     = make([]int64, len(Buckets)+1)
		sent     atomic.Int64
		acked    atomic.Int64
		errs     atomic.Int64
		firstErr atomic.Value
	)
	var limit *rate.Limiter
	if c.Throughput > 0 {
		limit = rate.NewLimiter(rate.Limit(c.Throughput), 1)
	}

	start := time.Now()
	stop := make(chan struct{})
	reported := make(chan struct{})
	go func() {
		defer close(reported)
		t := time.NewTicker(o.Tick)
		defer t.Stop()
		var rates []float64
		last := int64(0)
		for {
			select {
			case <-stop:
				return
			case <-t.C:
			}
			a := acked.Load()
			rates = append(rates, float64(a-last)/o.Tick.Seconds())
			last = a
			if o.Report == nil {
				continue
			}
			mu.Lock()
			h := slices.Clone(hist)
			mu.Unlock()
			o.Report(Progress{Sent: sent.Load(), Acked: a, Errors: errs.Load(), Elapsed: time.Since(start), Hist: h, Rate: slices.Clone(rates)})
		}
	}()

	for range c.Records {
		if limit != nil {
			if err := limit.Wait(ctx); err != nil {
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		t0 := time.Now()
		sent.Add(1)
		cl.Produce(ctx, &kgo.Record{Value: value}, func(_ *kgo.Record, err error) {
			d := time.Since(t0)
			if err != nil {
				errs.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
				return
			}
			mu.Lock()
			lat = append(lat, d)
			hist[bucket(d)]++
			mu.Unlock()
			acked.Add(1)
		})
	}
	ferr := cl.Flush(ctx)
	took := time.Since(start)
	close(stop)
	<-reported

	r := Result{Case: c, Records: acked.Load(), Errors: errs.Load(), Took: took, Hist: hist}
	if v, ok := firstErr.Load().(string); ok {
		r.FirstError = v
	}
	r.Bytes = r.Records * int64(c.Size)
	if s := took.Seconds(); s > 0 {
		r.RecsPerSec = float64(r.Records) / s
		r.MBPerSec = float64(r.Bytes) / s / (1 << 20)
	}
	if len(lat) > 0 {
		slices.Sort(lat)
		var sum time.Duration
		for _, d := range lat {
			sum += d
		}
		r.Avg = sum / time.Duration(len(lat))
		r.P50, r.P95, r.P99, r.P999 = pct(lat, 50), pct(lat, 95), pct(lat, 99), pct(lat, 99.9)
		r.Max = lat[len(lat)-1]
	}
	if ferr != nil {
		return r, ferr
	}
	return r, ctx.Err()
}

func bucket(d time.Duration) int {
	for i, b := range Buckets {
		if d <= b {
			return i
		}
	}
	return len(Buckets)
}

// pct is the nearest-rank percentile of sorted durations.
func pct(sorted []time.Duration, p float64) time.Duration {
	i := int(float64(len(sorted))*p/100+0.5) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

// Bytes formats a size the way case names show it: 100B, 1KB, 1.5MB.
func Bytes(n int64) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%dKB", n>>10)
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}
