// Package groups reads consumer groups as kaas shows them: state, members,
// the topics they read, how far behind they are, and, across two samples,
// how fast they consume.
package groups

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Group is one consumer group at one moment.
type Group struct {
	Name         string
	State        string // Stable, Empty, PreparingRebalance, CompletingRebalance, Dead
	ProtocolType string // consumer, connect, ...
	Members      int
	Topics       []string         // topics it has committed offsets for, sorted
	Partitions   int              // partitions with a commit
	Lag          int64            // records behind across those partitions; -1 unknown
	TopicLag     map[string]int64 // records behind per topic; -1 unknown
	Committed    int64            // sum of committed offsets, for the consume rate
	Err          error            // describing the group or fetching its offsets failed
}

// Sample is every group at one moment.
type Sample struct {
	At     time.Time
	Groups []Group // sorted by name
}

// Fetch lists the groups and computes their lag: ListGroups, then kadm.Lag,
// which describes them, fetches their commits and lists end offsets. A group
// that fails on its own keeps its Err and the others are still returned.
func Fetch(ctx context.Context, adm *kadm.Client) (Sample, error) {
	s := Sample{At: time.Now()}
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return s, fmt.Errorf("list groups: %w", err)
	}
	if len(listed) == 0 {
		return s, nil
	}
	lags, err := adm.Lag(ctx, listed.Groups()...)
	if err != nil && len(lags) == 0 {
		return s, fmt.Errorf("group lag: %w", err)
	}
	for _, name := range listed.Groups() {
		l, ok := lags[name]
		g := Group{Name: name, State: listed[name].State, ProtocolType: listed[name].ProtocolType, Lag: -1}
		if !ok {
			s.Groups = append(s.Groups, g)
			continue
		}
		g.State = cmp.Or(l.State, g.State)
		g.ProtocolType = cmp.Or(l.ProtocolType, g.ProtocolType)
		g.Members = len(l.Members)
		g.Err = l.Error()
		for topic, parts := range l.Lag {
			g.Topics = append(g.Topics, topic)
			tl := int64(-1)
			for _, p := range parts {
				if p.Commit.At < 0 {
					continue
				}
				g.Partitions++
				g.Committed += p.Commit.At
				if p.Lag >= 0 {
					g.Lag = max(g.Lag, 0) + p.Lag
					tl = max(tl, 0) + p.Lag
				}
			}
			if g.TopicLag == nil {
				g.TopicLag = map[string]int64{}
			}
			g.TopicLag[topic] = tl
		}
		slices.Sort(g.Topics)
		s.Groups = append(s.Groups, g)
	}
	slices.SortFunc(s.Groups, func(a, b Group) int { return cmp.Compare(a.Name, b.Name) })
	return s, nil
}

// Rate is how many records per second a group consumed between two
// samples, from the growth of its committed offsets. It is false when the
// group is new or the samples are not in order. A commit that moves back
// (an offset reset) counts as zero, not negative.
func Rate(prev, cur Sample, name string) (float64, bool) {
	dt := cur.At.Sub(prev.At).Seconds()
	if dt <= 0 {
		return 0, false
	}
	find := func(s Sample) (Group, bool) {
		i, ok := slices.BinarySearchFunc(s.Groups, name, func(g Group, n string) int { return cmp.Compare(g.Name, n) })
		if !ok {
			return Group{}, false
		}
		return s.Groups[i], true
	}
	a, ok1 := find(prev)
	b, ok2 := find(cur)
	if !ok1 || !ok2 {
		return 0, false
	}
	return float64(max(0, b.Committed-a.Committed)) / dt, true
}
