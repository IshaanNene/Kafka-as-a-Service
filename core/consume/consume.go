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

	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
)

// From is where reading starts on every partition, and where it stops
// when it has an end.
type From struct {
	kind  fromKind
	back  int64     // records before the end, for "-N"
	since time.Time // for a time or a duration ago
	until time.Time // stop before the first record at or after this; zero for no stop
	now   time.Time // when it was parsed, for how String writes times
}

// Until gives f an end: each partition stops before its first record at or
// after t.
func (f From) Until(t time.Time) From {
	f.until = t
	return f
}

// Since is where a time From starts; false for end, beginning and -N.
func (f From) Since() (time.Time, bool) { return f.since, f.kind == fromTime }

// End is f's end, zero when it has none.
func (f From) End() time.Time { return f.until }

// ParseUntil reads --to: a time or a duration back, as --from takes them,
// and not in the future.
func ParseUntil(s string, now time.Time) (time.Time, error) {
	t, ok := offsets.ParseTime(s, now)
	switch {
	case !ok:
		return t, fmt.Errorf("%q: want a time like 2026-10-31T09:00 or a duration back like 15m", s)
	case t.After(now):
		return t, fmt.Errorf("%s is in the future: leave --to out to keep following", t.Format(time.RFC3339))
	}
	return t, nil
}

type fromKind int

const (
	fromEnd fromKind = iota
	fromBeginning
	fromBack
	fromTime
)

// ParseFrom reads --from: "end", "beginning", "-N" (the last N records of
// each partition), a duration such as "15m" (that long ago), or a time such
// as 2026-10-31T09:00 (local unless it names a zone).
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
	if t, ok := offsets.ParseTime(s, now); ok {
		return From{kind: fromTime, since: t, now: now}, nil
	}
	return From{}, fmt.Errorf("%q: want end, beginning, -N, a duration like 15m, or a time like 2026-10-31T09:00", s)
}

// String describes f the way the join lines say it.
func (f From) String() string {
	switch f.kind {
	case fromBeginning:
		return "the beginning"
	case fromBack:
		return fmt.Sprintf("%d before the end", f.back)
	case fromTime:
		s := "records since " + clock(f.since, f.now)
		if !f.until.IsZero() {
			s = clock(f.since, f.now) + " to " + clock(f.until, f.now)
		}
		return s
	}
	return "the end"
}

// clock is a time of day in now's zone, with the date when it isn't now's.
func clock(t, now time.Time) string {
	t = t.In(now.Location())
	if t.Format("2006-01-02") != now.Format("2006-01-02") {
		return t.Format("Jan 2 15:04:05")
	}
	return t.Format("15:04:05")
}

// Start is where one partition is read from, and where it ended when the
// read began.
type Start struct {
	Partition int32
	Offset    int64 // first offset to read
	End       int64 // log-end offset when the read began
	Stop      int64 // the first offset not to read, for From.Until; -1 for none
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
	var at, until kadm.ListedOffsets
	if f.kind == fromTime {
		if at, err = adm.ListOffsetsAfterMilli(ctx, f.since.UnixMilli(), topic); err != nil {
			return nil, fmt.Errorf("offsets at %s: %w", f.since.Format(time.RFC3339), err)
		}
	}
	if !f.until.IsZero() {
		if until, err = adm.ListOffsetsAfterMilli(ctx, f.until.UnixMilli(), topic); err != nil {
			return nil, fmt.Errorf("offsets at %s: %w", f.until.Format(time.RFC3339), err)
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
		st := Start{Partition: e.Partition, End: e.Offset, Stop: -1}
		if until != nil {
			st.Stop = e.Offset
			if o, ok := until.Lookup(topic, e.Partition); ok && o.Err == nil && o.Offset >= 0 {
				st.Stop = min(o.Offset, e.Offset)
			}
		}
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
	from := map[int32]kgo.Offset{}
	// limit is where each partition stops: its Stop, or with Exit its end
	// when the read began. left is the partitions still short of theirs.
	limit, left, hard := map[int32]int64{}, map[int32]bool{}, map[int32]bool{}
	bounded := true
	for _, s := range o.Plan {
		from[s.Partition] = kgo.NewOffset().At(s.Offset)
		switch {
		case s.Stop >= 0:
			limit[s.Partition], hard[s.Partition] = s.Stop, true
		case o.Exit:
			limit[s.Partition] = s.End
		default:
			bounded = false
			continue
		}
		if s.Offset < limit[s.Partition] {
			left[s.Partition] = true
		}
	}
	if bounded && len(left) == 0 {
		return nil
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(o.Seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{o.Topic: from}),
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
			if hard[r.Partition] && r.Offset >= limit[r.Partition] {
				// Past --to: a fetch returns whole batches, so stop here.
				delete(left, r.Partition)
				if len(left) == 0 {
					stop = ErrDone
				}
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
			if l, ok := limit[r.Partition]; ok && r.Offset >= l-1 {
				delete(left, r.Partition)
			}
			if bounded && len(left) == 0 {
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

// Counts is, for each of times, the sum over topic's partitions of the
// first offset at or after it, the end where there is none: the records
// between two times are the difference.
func Counts(ctx context.Context, adm *kadm.Client, topic string, times []time.Time) ([]int64, error) {
	out := make([]int64, len(times))
	for i, t := range times {
		at, err := adm.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return nil, err
		}
		var sum int64
		var errs []error
		at.Each(func(o kadm.ListedOffset) {
			if o.Err != nil {
				errs = append(errs, fmt.Errorf("partition %d: %w", o.Partition, o.Err))
				return
			}
			sum += o.Offset
		})
		if err := errors.Join(errs...); err != nil {
			return nil, err
		}
		out[i] = sum
	}
	return out, nil
}

// Oldest is the time of the oldest record topic still keeps, reading each
// partition's first record; zero when every partition is empty.
func Oldest(ctx context.Context, cl *kgo.Client, adm *kadm.Client, topic string) (time.Time, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return time.Time{}, err
	}
	var oldest time.Time
	var errs []error
	for _, ps := range starts {
		for p, s := range ps {
			if s.Err != nil {
				errs = append(errs, fmt.Errorf("partition %d: %w", p, s.Err))
				continue
			}
			r, err := offsets.NewReader(ctx, cl, topic, p)
			if err != nil {
				return time.Time{}, err
			}
			rec, err := r.At(ctx, s.Offset)
			if err != nil {
				return time.Time{}, err
			}
			if rec != nil && (oldest.IsZero() || rec.Timestamp.Before(oldest)) {
				oldest = rec.Timestamp
			}
		}
	}
	return oldest, errors.Join(errs...)
}
