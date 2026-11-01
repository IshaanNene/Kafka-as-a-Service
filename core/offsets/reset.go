// Package offsets plans and applies consumer group offset resets: where
// each partition's committed offset is, where a target puts it, and the
// commit that moves it.
package offsets

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Kinds of target.
const (
	Earliest = iota
	Latest
	Offset // an absolute offset
	Shift  // relative to the committed offset
	Time   // the first record at or after a time
)

// Target is where --to puts each partition.
type Target struct {
	Kind int
	N    int64     // the offset for Offset, the delta for Shift
	At   time.Time // for Time
}

func (t Target) String() string {
	switch t.Kind {
	case Earliest:
		return "earliest"
	case Latest:
		return "latest"
	case Offset:
		return "offset " + strconv.FormatInt(t.N, 10)
	case Shift:
		return fmt.Sprintf("%+d", t.N)
	}
	return t.At.Format(time.RFC3339)
}

// ParseTo reads --to: earliest, latest, an offset (1043), a shift (+100,
// -100), a time (2026-10-31T09:00:00Z, 2026-10-31 09:00, 2026-10-31) in
// the local zone unless it says otherwise, or a duration back from now
// (90m, 2h, 7d).
func ParseTo(s string, now time.Time) (Target, error) {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "earliest", "beginning", "start":
		return Target{Kind: Earliest}, nil
	case "latest", "end":
		return Target{Kind: Latest}, nil
	case "":
		return Target{}, errors.New("empty")
	}
	if s[0] == '+' || s[0] == '-' {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Target{}, fmt.Errorf("%q: a shift is a whole number of records, like +100 or -100", s)
		}
		return Target{Kind: Shift, N: n}, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return Target{Kind: Offset, N: n}, nil
	}
	t, ok := ParseTime(s, now)
	switch {
	case !ok:
		return Target{}, fmt.Errorf("%q: want earliest, latest, an offset, +N or -N, a time like 2026-10-31T09:00, or a duration back like 2h", s)
	case t.After(now):
		return Target{}, fmt.Errorf("%s is in the future: use latest", t.Format(time.RFC3339))
	}
	return Target{Kind: Time, At: t}, nil
}

// ParseTime reads a moment the way kaas takes them everywhere: a duration
// back from now (90m, 2h, 7d), or a time (2026-10-31T09:00:00Z,
// 2026-10-31 09:00, 2026-10-31) in now's zone unless it names one.
func ParseTime(s string, now time.Time) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if d, ok := ago(s); ok {
		return now.Add(-d), true
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, now.Location()); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ago reads a duration back from now; Go's durations plus d for days.
func ago(s string) (time.Duration, bool) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || n < 0 {
			return 0, false
		}
		return time.Duration(n * float64(24*time.Hour)), true
	}
	d, err := time.ParseDuration(s)
	return d, err == nil && d >= 0
}

// Move is one partition's change.
type Move struct {
	Topic      string
	Partition  int32
	From       int64 // the committed offset; -1 when the group has none here
	To         int64
	Start, End int64 // the partition's log
	Clamped    bool  // the target fell outside the log, so it was moved inside
	Skip       string
}

// Changed is whether the move commits anything.
func (m Move) Changed() bool { return m.Skip == "" && m.To != m.From }

// Plan is a reset before it is applied.
type Plan struct {
	Group   string
	State   string // the coordinator's: Empty, Dead, Stable…
	Members int
	Target  Target
	Moves   []Move // by topic, then partition
}

// Counts is the partitions the plan commits for the first time, moves,
// and leaves where they are.
func (p Plan) Counts() (add, change, same int) {
	for _, m := range p.Moves {
		switch {
		case !m.Changed():
			same++
		case m.From < 0:
			add++
		default:
			change++
		}
	}
	return add, change, same
}

// Records is how many records the group will read again (moves back) and
// skip (moves forward). A first commit counts from nothing, so it is in
// neither.
func (p Plan) Records() (reread, skipped int64) {
	for _, m := range p.Moves {
		if !m.Changed() || m.From < 0 {
			continue
		}
		if m.To < m.From {
			reread += m.From - m.To
		} else {
			skipped += m.To - m.From
		}
	}
	return reread, skipped
}

// ErrActive is a group with members: the coordinator refuses commits from
// outside a live generation.
var ErrActive = errors.New("the group has active members")

// Committed is the topics group has committed offsets on, sorted.
func Committed(ctx context.Context, adm *kadm.Client, group string) ([]string, error) {
	rs, err := fetch(ctx, adm, group)
	if err != nil {
		return nil, err
	}
	var out []string
	for t := range rs {
		out = append(out, t)
	}
	slices.Sort(out)
	return out, nil
}

// PlanReset works out where to on topics moves group's offsets. A topic
// the cluster doesn't have is an error; a group with members is planned
// but carries ErrActive.
func PlanReset(ctx context.Context, adm *kadm.Client, group string, topics []string, to Target) (Plan, error) {
	p := Plan{Group: group, Target: to}
	dg, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		return p, fmt.Errorf("describing %s: %w", group, err)
	}
	if d, ok := dg[group]; ok {
		if errors.Is(d.Err, kerr.GroupIDNotFound) {
			d.Err, d.State = nil, "Dead"
		}
		if d.Err != nil {
			return p, fmt.Errorf("describing %s: %w", group, d.Err)
		}
		p.State, p.Members = d.State, len(d.Members)
	}
	committed, err := fetch(ctx, adm, group)
	if err != nil {
		return p, fmt.Errorf("fetching %s's offsets: %w", group, err)
	}
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return p, err
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return p, err
	}
	var at kadm.ListedOffsets
	if to.Kind == Time {
		if at, err = adm.ListOffsetsAfterMilli(ctx, to.At.UnixMilli(), topics...); err != nil {
			return p, err
		}
	}
	for _, t := range topics {
		parts, ok := ends[t]
		if !ok || len(parts) == 0 {
			return p, fmt.Errorf("%s: %w", t, ErrNoTopic)
		}
		if _, missing := parts[-1]; missing {
			return p, fmt.Errorf("%s: %w", t, ErrNoTopic)
		}
		ids := make([]int32, 0, len(parts))
		for id := range parts {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			end, start := parts[id], starts[t][id]
			m := Move{Topic: t, Partition: id, From: -1, Start: start.Offset, End: end.Offset}
			if c, ok := committed.Lookup(t, id); ok && c.Err == nil {
				m.From = c.At
			}
			if end.Err != nil || start.Err != nil {
				m.Skip = "offsets unavailable: " + errors.Join(end.Err, start.Err).Error()
				m.To = m.From
				p.Moves = append(p.Moves, m)
				continue
			}
			want := int64(0)
			switch to.Kind {
			case Earliest:
				want = m.Start
			case Latest:
				want = m.End
			case Offset:
				want = to.N
			case Shift:
				if m.From < 0 {
					m.Skip, m.To = "no committed offset to shift from", -1
					p.Moves = append(p.Moves, m)
					continue
				}
				want = m.From + to.N
			case Time:
				want = m.End
				if o, ok := at.Lookup(t, id); ok && o.Err == nil && o.Offset >= 0 {
					want = o.Offset
				}
			}
			m.To = min(max(want, m.Start), m.End)
			m.Clamped = m.To != want
			p.Moves = append(p.Moves, m)
		}
	}
	if p.Members > 0 {
		return p, ErrActive
	}
	return p, nil
}

// fetch is group's commits; a group the coordinator doesn't know has none.
func fetch(ctx context.Context, adm *kadm.Client, group string) (kadm.OffsetResponses, error) {
	rs, err := adm.FetchOffsets(ctx, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return kadm.OffsetResponses{}, nil
	}
	return rs, err
}

// ErrNoTopic is a topic the cluster doesn't have.
var ErrNoTopic = errors.New("no such topic")

// Apply commits the plan's changed offsets as group. The result maps each
// committed move's topic and partition to its error, nil when it took.
func Apply(ctx context.Context, adm *kadm.Client, p Plan) (map[string]map[int32]error, error) {
	os := kadm.Offsets{}
	for _, m := range p.Moves {
		if m.Changed() {
			os.Add(kadm.Offset{Topic: m.Topic, Partition: m.Partition, At: m.To, LeaderEpoch: -1})
		}
	}
	out := map[string]map[int32]error{}
	if len(os) == 0 {
		return out, nil
	}
	rs, err := adm.CommitOffsets(ctx, p.Group, os)
	if err != nil {
		return nil, err
	}
	for t, ps := range os {
		out[t] = map[int32]error{}
		for id := range ps {
			r, ok := rs.Lookup(t, id)
			switch {
			case !ok:
				out[t][id] = errors.New("no answer from the coordinator")
			default:
				out[t][id] = r.Err
			}
		}
	}
	return out, nil
}
