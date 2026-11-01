// Package top measures how hard each partition works: records written per
// second from log-end offsets, records read per second from consumer
// groups' commits, and disk growth from log-dir sizes, each a difference
// between two samples.
package top

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Key is one partition.
type Key struct {
	Topic     string
	Partition int32
}

// Sample is every partition's counters at one moment. Size and Read may be
// missing for a partition when the broker would not say.
type Sample struct {
	At     time.Time
	End    map[Key]int64            // log-end offset
	Size   map[Key]int64            // bytes on the leader
	Read   map[string]map[Key]int64 // group → its committed offsets
	Leader map[Key]int32
}

// Take reads the counters for topics. Log-end offsets are required; sizes
// (which need DESCRIBE on the cluster) and group commits are best effort,
// and their failure comes back joined in the error alongside the sample.
func Take(ctx context.Context, adm *kadm.Client, topics []string, leaders map[Key]int32) (Sample, error) {
	s := Sample{At: time.Now(), End: map[Key]int64{}, Size: map[Key]int64{}, Read: map[string]map[Key]int64{}, Leader: leaders}
	if len(topics) == 0 {
		return s, nil
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return s, fmt.Errorf("end offsets: %w", err)
	}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			s.End[Key{o.Topic, o.Partition}] = o.Offset
		}
	})
	var errs []error
	if dirs, err := adm.DescribeAllLogDirs(ctx, nil); err != nil {
		errs = append(errs, fmt.Errorf("log dirs: %w", err))
	} else {
		dirs.Each(func(d kadm.DescribedLogDir) {
			if d.Err != nil {
				return
			}
			for topic, parts := range d.Topics {
				for p, info := range parts {
					k := Key{topic, p}
					if leaders[k] == d.Broker {
						s.Size[k] += info.Size
					}
				}
			}
		})
	}
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("groups: %w", err))
	} else if len(listed) > 0 {
		fetched := adm.FetchManyOffsets(ctx, listed.Groups()...)
		for g, r := range fetched {
			if r.Err != nil {
				continue
			}
			r.Fetched.Each(func(o kadm.OffsetResponse) {
				k := Key{o.Topic, o.Partition}
				if _, ok := s.End[k]; o.Err != nil || o.At < 0 || !ok {
					return
				}
				if s.Read[g] == nil {
					s.Read[g] = map[Key]int64{}
				}
				s.Read[g][k] = o.At
			})
		}
	}
	return s, errors.Join(errs...)
}

// Row is one partition's rates between two samples; -1 is unknown.
type Row struct {
	Key
	Leader int32
	In     float64 // records written per second
	Out    float64 // records committed per second, over every group
	Bytes  float64 // disk growth per second on the leader
	Size   int64   // bytes on the leader now
	End    int64
}

// Rates compares two samples of the same partitions. A partition missing
// from prev, or a zero interval, has unknown rates; counters that go back
// (a reset, retention deleting segments) count as zero.
func Rates(prev, cur Sample) []Row {
	dt := cur.At.Sub(prev.At).Seconds()
	rate := func(a, b int64, okA, okB bool) float64 {
		if !okA || !okB || dt <= 0 {
			return -1
		}
		return float64(max(0, b-a)) / dt
	}
	out := make([]Row, 0, len(cur.End))
	for k, end := range cur.End {
		r := Row{Key: k, Leader: -1, End: end, Size: -1}
		if l, ok := cur.Leader[k]; ok {
			r.Leader = l
		}
		pe, ok := prev.End[k]
		r.In = rate(pe, end, ok, true)
		ps, okP := prev.Size[k]
		cs, okC := cur.Size[k]
		r.Bytes = rate(ps, cs, okP, okC)
		if okC {
			r.Size = cs
		}
		r.Out = -1
		if dt > 0 && ok {
			// Only groups in both samples count, so a group that appears
			// between them is not a burst of reads.
			r.Out = 0
			for g, offs := range cur.Read {
				if c, okC := offs[k]; okC {
					if p, okP := prev.Read[g][k]; okP {
						r.Out += float64(max(0, c-p)) / dt
					}
				}
			}
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b Row) int {
		return cmp.Or(cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	return out
}
