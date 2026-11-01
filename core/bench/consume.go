package bench

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// FetchCase is one cell of the consume matrix: how much a fetch may bring
// back per partition and how long the broker may hold it waiting for data.
type FetchCase struct {
	FetchSize int           `json:"fetch_size"` // max bytes per partition per fetch
	MaxWait   time.Duration `json:"max_wait"`
	Records   int           `json:"records"` // to read; fewer when the topic holds fewer
}

// Name is the case's label in tables: "fetch=1MB wait=500ms".
func (c FetchCase) Name() string {
	return fmt.Sprintf("fetch=%s wait=%s", Bytes(int64(c.FetchSize)), c.MaxWait)
}

// FetchMatrix is every combination of fetch sizes and waits.
func FetchMatrix(sizes []int, waits []time.Duration, records int) []FetchCase {
	var out []FetchCase
	for _, s := range sizes {
		for _, w := range waits {
			out = append(out, FetchCase{FetchSize: s, MaxWait: w, Records: records})
		}
	}
	return out
}

// Validate reports the first field a case can't run with.
func (c FetchCase) Validate() (field, msg string) {
	switch {
	case c.FetchSize < 1<<10:
		return "fetch-size", "must be at least 1KB"
	case c.MaxWait < time.Millisecond:
		return "max-wait", "must be at least 1ms"
	case c.Records <= 0:
		return "records", "must be positive"
	}
	return "", ""
}

// ConsumeResult is a finished consume case. Its latencies are poll waits:
// how long the reader waited for each batch.
type ConsumeResult struct {
	Case  FetchCase `json:"case"`
	Polls int64     `json:"polls"`
	Stats
}

// ErrEmpty is a topic with nothing to read.
var ErrEmpty = errors.New("the topic has no records to read")

// Available is how many records topic holds, from its log-start to its
// log-end offsets, and where each partition starts.
func Available(ctx context.Context, adm *kadm.Client, topic string) (int64, map[int32]kgo.Offset, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, nil, fmt.Errorf("start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, nil, fmt.Errorf("end offsets: %w", err)
	}
	var n int64
	from := map[int32]kgo.Offset{}
	var errs []error
	ends.Each(func(e kadm.ListedOffset) {
		s, ok := starts.Lookup(topic, e.Partition)
		if e.Err != nil || !ok || s.Err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", e.Partition, errors.Join(e.Err, s.Err)))
			return
		}
		n += e.Offset - s.Offset
		from[e.Partition] = kgo.NewOffset().At(s.Offset)
	})
	return n, from, errors.Join(errs...)
}

// Consume reads c.Records records of topic from the beginning, or all of
// them when it holds fewer, without joining a group or committing. Each
// case gets its own client, so no fetch is served from another's buffer.
func Consume(ctx context.Context, topic string, c FetchCase, o Options) (ConsumeResult, error) {
	r := ConsumeResult{Case: c}
	if f, msg := c.Validate(); f != "" {
		return r, fmt.Errorf("%s %s", f, msg)
	}
	if o.Tick == 0 {
		o.Tick = 100 * time.Millisecond
	}
	meta, err := kgo.NewClient(o.Client...)
	if err != nil {
		return r, err
	}
	avail, from, err := Available(ctx, kadm.NewClient(meta), topic)
	meta.Close()
	if err != nil {
		return r, err
	}
	if avail == 0 {
		return r, ErrEmpty
	}
	want := min(int64(c.Records), avail)
	r.Case.Records = int(want)

	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}),
		kgo.FetchMaxPartitionBytes(int32(c.FetchSize)),
		kgo.FetchMaxBytes(int32(max(c.FetchSize, 50<<20))),
		kgo.FetchMaxWait(c.MaxWait),
	}, o.Client...)...)
	if err != nil {
		return r, err
	}
	defer cl.Close()

	var (
		mu         sync.Mutex
		lat        []time.Duration
		hist       = make([]int64, len(Buckets)+1)
		polls, got atomic.Int64
		errs       atomic.Int64
		bytes      int64
	)
	start := time.Now()
	stop := ticker(o, start, &polls, &got, &errs, func() []int64 {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(hist)
	})
	for got.Load() < want && ctx.Err() == nil {
		t0 := time.Now()
		fs := cl.PollFetches(ctx)
		d := time.Since(t0)
		if ctx.Err() != nil {
			break
		}
		fs.EachError(func(_ string, p int32, err error) {
			errs.Add(1)
			if r.FirstError == "" {
				r.FirstError = fmt.Sprintf("partition %d: %v", p, err)
			}
		})
		n := int64(0)
		fs.EachRecord(func(rec *kgo.Record) {
			if got.Load()+n < want {
				n++
				bytes += int64(len(rec.Key) + len(rec.Value))
			}
		})
		polls.Add(1)
		mu.Lock()
		lat = append(lat, d)
		hist[bucket(d)]++
		mu.Unlock()
		got.Add(n)
		if r.FirstError != "" && n == 0 {
			break // a partition that keeps failing would otherwise spin
		}
	}
	r.Took = time.Since(start)
	stop()

	r.Records, r.Errors, r.Bytes, r.Hist, r.Polls = got.Load(), errs.Load(), bytes, hist, polls.Load()
	r.finish(lat)
	if r.FirstError != "" && r.Records < want {
		return r, errors.New(r.FirstError)
	}
	return r, ctx.Err()
}
