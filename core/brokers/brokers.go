// Package brokers sums a cluster snapshot per broker: what each one leads,
// which replicas it holds and which of those are behind, and its disk use.
// It also finds brokers the partitions still name but metadata no longer
// lists, which are down.
package brokers

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

// Broker is one broker's share of the cluster.
type Broker struct {
	ID         int32
	Host       string // empty for a broker that is down
	Port       int32
	Rack       string
	Controller bool
	Online     bool // listed in metadata
	Leaders    int  // partitions it leads
	Replicas   int  // partition replicas it holds, leaders included
	Behind     int  // of those, replicas out of the ISR
	Disk       int64
	Topics     []Topic // topics it leads or follows, most led first
}

// Topic is one topic as seen from one broker.
type Topic struct {
	Name     string
	Internal bool
	Leads    []int32 // partition IDs it leads, ascending
	Follows  int     // partitions it holds as a follower
	Behind   int     // of its replicas, those out of the ISR
}

// Summarize sums s per broker. disk is broker → bytes from Disk; brokers
// missing from it have Disk -1. Brokers are sorted by ID; brokers that
// partitions name but metadata does not list come back with Online false.
func Summarize(s *snapshot.Snapshot, disk map[int32]int64) []Broker {
	byID := map[int32]*Broker{}
	get := func(id int32) *Broker {
		if b, ok := byID[id]; ok {
			return b
		}
		b := &Broker{ID: id, Disk: -1}
		byID[id] = b
		return b
	}
	for _, mb := range s.Brokers {
		b := get(mb.ID)
		b.Host, b.Port, b.Rack, b.Online = mb.Host, mb.Port, mb.Rack, true
	}
	for _, t := range s.Topics {
		per := map[int32]*Topic{}
		for _, p := range t.Partitions {
			for _, id := range p.Replicas {
				b := get(id)
				b.Replicas++
				tp, ok := per[id]
				if !ok {
					tp = &Topic{Name: t.Name, Internal: t.Internal}
					per[id] = tp
				}
				behind := !slices.Contains(p.ISR, id)
				if id == p.Leader {
					b.Leaders++
					tp.Leads = append(tp.Leads, p.ID)
				} else {
					tp.Follows++
				}
				if behind {
					b.Behind++
					tp.Behind++
				}
			}
		}
		for id, tp := range per {
			slices.Sort(tp.Leads)
			byID[id].Topics = append(byID[id].Topics, *tp)
		}
	}
	out := make([]Broker, 0, len(byID))
	for id, b := range byID {
		b.Controller = id == s.Controller
		if d, ok := disk[id]; ok {
			b.Disk = d
		}
		slices.SortFunc(b.Topics, func(x, y Topic) int {
			return cmp.Or(cmp.Compare(len(y.Leads), len(x.Leads)), cmp.Compare(x.Name, y.Name))
		})
		out = append(out, *b)
	}
	slices.SortFunc(out, func(x, y Broker) int { return cmp.Compare(x.ID, y.ID) })
	return out
}

// Disk is bytes on disk per broker across all its log directories. It needs
// DESCRIBE on the cluster, so callers treat a failure as "unknown".
func Disk(ctx context.Context, adm *kadm.Client) (map[int32]int64, error) {
	dirs, err := adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("log dirs: %w", err)
	}
	out := map[int32]int64{}
	dirs.Each(func(d kadm.DescribedLogDir) {
		if d.Err != nil {
			return
		}
		// A broker whose dirs are empty still reports 0, not unknown.
		out[d.Broker] += 0
		for _, parts := range d.Topics {
			for _, p := range parts {
				out[d.Broker] += p.Size
			}
		}
	})
	return out, nil
}

// Skew is the most leaders any online broker has over the even share, so 1
// is perfectly balanced; 0 when nothing leads anything.
func Skew(bs []Broker) float64 {
	online, leaders, most := 0, 0, 0
	for _, b := range bs {
		if b.Online {
			online++
			leaders += b.Leaders
			most = max(most, b.Leaders)
		}
	}
	if online == 0 || leaders == 0 {
		return 0
	}
	return float64(most) / (float64(leaders) / float64(online))
}
