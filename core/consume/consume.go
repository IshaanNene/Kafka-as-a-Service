// Package consume reads a topic without joining a group: it resolves where
// each partition starts from --from, then fetches from exactly there. It
// never commits, so reading never moves anyone's offsets.
package consume

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// From is where reading starts on every partition.
type From struct {
	kind  fromKind
	back  int64     // records before the end, for "-N"
	since time.Time // for a time or a duration ago
}

type fromKind int

const (
	fromEnd fromKind = iota
	fromBeginning
	fromBack
	fromTime
)

// ParseFrom reads --from: "end", "beginning", "-N" (the last N records of
// each partition), a duration such as "15m" (that long ago), or an RFC 3339
// time.
func ParseFrom(s string, now time.Time) (From, error) {
	switch s {
	case "", "end", "latest":
		return From{kind: fromEnd}, nil
	case "beginning", "earliest", "start":
		return From{kind: fromBeginning}, nil
	}
	if strings.HasPrefix(s, "-") {
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil || n <= 0 {
			return From{}, fmt.Errorf("%q: -N needs a positive record count, like -10", s)
		}
		return From{kind: fromBack, back: n}, nil
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return From{kind: fromTime, since: now.Add(-d)}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return From{kind: fromTime, since: t}, nil
	}
	return From{}, fmt.Errorf("%q: want end, beginning, -N, a duration like 15m, or an RFC 3339 time", s)
}

// String describes f the way the join lines say it.
func (f From) String() string {
	switch f.kind {
	case fromBeginning:
		return "the beginning"
	case fromBack:
		return fmt.Sprintf("%d before the end", f.back)
	case fromTime:
		return "records since " + f.since.Format("15:04:05")
	}
	return "the end"
}

// Start is where one partition is read from, and where it ended when the
// read began.
type Start struct {
	Partition int32
	Offset    int64 // first offset to read
	End       int64 // log-end offset when the read began
}

// Plan resolves f to an offset on each partition of topic, or on parts when
// it is non-empty. An unknown partition is an error.
func Plan(ctx context.Context, adm *kadm.Client, topic string, parts []int32, f From) ([]Start, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("end offsets: %w", err)
	}
	var at kadm.ListedOffsets
	if f.kind == fromTime {
		if at, err = adm.ListOffsetsAfterMilli(ctx, f.since.UnixMilli(), topic); err != nil {
			return nil, fmt.Errorf("offsets at %s: %w", f.since.Format(time.RFC3339), err)
		}
	}
	var out []Start
	var all []int32
	var errs []error
	ends.Each(func(e kadm.ListedOffset) {
		all = append(all, e.Partition)
		if len(parts) > 0 && !slices.Contains(parts, e.Partition) {
			return
		}
		if e.Err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", e.Partition, e.Err))
			return
		}
		s, _ := starts.Lookup(topic, e.Partition)
		st := Start{Partition: e.Partition, End: e.Offset}
		switch f.kind {
		case fromEnd:
			st.Offset = e.Offset
		case fromBeginning:
			st.Offset = s.Offset
		case fromBack:
			st.Offset = max(s.Offset, e.Offset-f.back)
		case fromTime:
			st.Offset = e.Offset
			if o, ok := at.Lookup(topic, e.Partition); ok && o.Err == nil && o.Offset >= 0 {
				st.Offset = o.Offset
			}
		}
		out = append(out, st)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	for _, p := range parts {
		if !slices.Contains(all, p) {
			slices.Sort(all)
			return nil, fmt.Errorf("topic %s has no partition %d (it has %d)", topic, p, len(all))
		}
	}
	slices.SortFunc(out, func(a, b Start) int { return int(a.Partition - b.Partition) })
	return out, nil
}

// Header is one record header.
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Record is one record read.
type Record struct {
	Partition int32
	Offset    int64
	Time      time.Time
	Key       []byte // nil for no key
	Value     []byte // nil for a tombstone
	Headers   []Header
}

// Options is one read.
type Options struct {
	Seeds []string
	Topic string
	Plan  []Start
	Max   int  // stop after this many records; 0 for no limit
	Exit  bool // stop once every partition reaches the end it had when the read began
}

// ErrDone ends Read early from inside each without an error.
var ErrDone = errors.New("consume: done")

// Read fetches from the planned offsets and calls each for every record,
// in order within a partition. It returns nil when ctx ends, Max is
// reached, Exit is met, or each returns ErrDone.
func Read(ctx context.Context, o Options, opts []kgo.Opt, each func(Record) error) error {
	offsets := map[int32]kgo.Offset{}
	left := map[int32]bool{} // partitions still short of their end, for Exit
	for _, s := range o.Plan {
		offsets[s.Partition] = kgo.NewOffset().At(s.Offset)
		if s.Offset < s.End {
			left[s.Partition] = true
		}
	}
	if o.Exit && len(left) == 0 {
		return nil
	}
	ends := map[int32]int64{}
	for _, s := range o.Plan {
		ends[s.Partition] = s.End
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(o.Seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{o.Topic: offsets}),
	}, opts...)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	n := 0
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var ferr error
		fs.EachError(func(_ string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				ferr = errors.Join(ferr, fmt.Errorf("partition %d: %w", p, err))
			}
		})
		if ferr != nil {
			return ferr
		}
		var stop error
		fs.EachRecord(func(r *kgo.Record) {
			if stop != nil {
				return
			}
			rec := Record{Partition: r.Partition, Offset: r.Offset, Time: r.Timestamp, Key: r.Key, Value: r.Value}
			for _, h := range r.Headers {
				rec.Headers = append(rec.Headers, Header{Key: h.Key, Value: string(h.Value)})
			}
			if err := each(rec); err != nil {
				stop = err
				return
			}
			n++
			if o.Max > 0 && n >= o.Max {
				stop = ErrDone
			}
			if r.Offset >= ends[r.Partition]-1 {
				delete(left, r.Partition)
			}
			if o.Exit && len(left) == 0 {
				stop = ErrDone
			}
		})
		if errors.Is(stop, ErrDone) {
			return nil
		}
		if stop != nil {
			return stop
		}
	}
}

// Text shows b as text when it is printable UTF-8, else as a size, so a
// binary value never garbles the terminal. ok is false for binary.
func Text(b []byte) (s string, ok bool) {
	if !utf8.Valid(b) {
		return fmt.Sprintf("<%d bytes, binary>", len(b)), false
	}
	for _, r := range string(b) {
		if !unicode.IsPrint(r) && r != '\t' && r != '\n' && r != '\r' {
			return fmt.Sprintf("<%d bytes, binary>", len(b)), false
		}
	}
	return string(b), true
}
