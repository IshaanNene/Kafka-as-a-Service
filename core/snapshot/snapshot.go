// Package snapshot holds the immutable cluster snapshots that every KaaS
// surface renders. Screens and the agent never read live client state; they
// read a Snapshot, which carries a generation number so consumers can tell
// fresh data from old.
//
// core/ must not import ui/ or any transport package (see ADR-0003).
package snapshot

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Snapshot is a point-in-time, read-only view of one cluster. Nothing may
// mutate a Snapshot once it has been published; producers build a new one.
type Snapshot struct {
	Generation uint64
	TakenAt    time.Time
	ClusterID  string
	Controller int32
	Brokers    []Broker
	Topics     []Topic
}

type Broker struct {
	ID   int32
	Host string
	Port int32
	Rack string
}

type Topic struct {
	Name       string
	Internal   bool
	Partitions []Partition
	// OffsetsAt is when StartOffset and EndOffset were last read for this
	// topic. Zero means never; screens show the age rather than hide it.
	OffsetsAt time.Time
}

// UnknownOffset marks an offset that has not been read yet.
const UnknownOffset int64 = -1

type Partition struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
	Offline  []int32

	StartOffset int64 // log-start offset, or UnknownOffset
	EndOffset   int64 // log-end offset (high watermark), or UnknownOffset
}

// UnderReplicated reports whether the partition's ISR is smaller than its
// replica set.
func (p Partition) UnderReplicated() bool { return len(p.ISR) < len(p.Replicas) }

// PartitionCount returns the total number of partitions across all topics.
func (s *Snapshot) PartitionCount() int {
	n := 0
	for _, t := range s.Topics {
		n += len(t.Partitions)
	}
	return n
}

// Topic returns the named topic, if present.
func (s *Snapshot) Topic(name string) (Topic, bool) {
	i, ok := s.topicIndex(name)
	if !ok {
		return Topic{}, false
	}
	return s.Topics[i], true
}

func (s *Snapshot) topicIndex(name string) (int, bool) {
	return slices.BinarySearchFunc(s.Topics, name, func(t Topic, n string) int {
		return strings.Compare(t.Name, n)
	})
}

// Fetch reads cluster metadata in one request and returns an unnumbered
// Snapshot (Generation 0) with brokers, topics and partitions sorted and
// offsets unknown.
//
// It sends a MetadataRequest directly rather than calling kadm.Metadata,
// which is served from kgo's metadata cache (MetadataMinAge, 5 s by
// default): polling through that cache would double worst-case staleness.
// Topics that come back with an error are skipped, not fatal, so one bad
// topic cannot blank the view of the other hundred thousand partitions.
func Fetch(ctx context.Context, cl kmsg.Requestor) (*Snapshot, error) {
	req := kmsg.NewPtrMetadataRequest() // nil Topics: all topics
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("snapshot: metadata: %w", err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, fmt.Errorf("snapshot: metadata: %w", err)
	}
	s := &Snapshot{
		TakenAt:    time.Now(),
		Controller: resp.ControllerID,
	}
	if resp.ClusterID != nil {
		s.ClusterID = *resp.ClusterID
	}
	for _, b := range resp.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		s.Brokers = append(s.Brokers, Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack})
	}
	slices.SortFunc(s.Brokers, func(a, b Broker) int { return cmp.Compare(a.ID, b.ID) })
	for _, rt := range resp.Topics {
		if rt.Topic == nil || rt.ErrorCode != 0 {
			continue
		}
		topic := Topic{Name: *rt.Topic, Internal: rt.IsInternal}
		for _, rp := range rt.Partitions {
			topic.Partitions = append(topic.Partitions, Partition{
				ID:          rp.Partition,
				Leader:      rp.Leader,
				Replicas:    rp.Replicas,
				ISR:         rp.ISR,
				Offline:     rp.OfflineReplicas,
				StartOffset: UnknownOffset,
				EndOffset:   UnknownOffset,
			})
		}
		slices.SortFunc(topic.Partitions, func(a, b Partition) int { return cmp.Compare(a.ID, b.ID) })
		s.Topics = append(s.Topics, topic)
	}
	slices.SortFunc(s.Topics, func(a, b Topic) int { return strings.Compare(a.Name, b.Name) })
	return s, nil
}

// Taker produces successive numbered snapshots of one cluster. It is the
// simple one-shot path used by `kaas snapshot`; long-running surfaces use
// the engine, which adds offsets, budgets and events.
type Taker struct {
	cl  kmsg.Requestor
	gen uint64
}

func NewTaker(cl kmsg.Requestor) *Taker {
	return &Taker{cl: cl}
}

// Take fetches metadata and returns a Snapshot with the next generation.
func (t *Taker) Take(ctx context.Context) (*Snapshot, error) {
	s, err := Fetch(ctx, t.cl)
	if err != nil {
		return nil, err
	}
	t.gen++
	s.Generation = t.gen
	return s, nil
}

// WithOffsets returns a copy of s with fresh offsets for topics. Only the
// touched topics' partition slices are copied; the rest are shared, which is
// safe because published snapshots are never mutated.
func WithOffsets(s *Snapshot, topics []string, starts, ends kadm.ListedOffsets, at time.Time) *Snapshot {
	out := *s
	out.Topics = append([]Topic(nil), s.Topics...)
	set := make(map[string]bool, len(topics))
	for _, t := range topics {
		set[t] = true
	}
	for i := range out.Topics {
		t := &out.Topics[i]
		if !set[t.Name] {
			continue
		}
		t.Partitions = append([]Partition(nil), t.Partitions...)
		t.OffsetsAt = at
		for k := range t.Partitions {
			p := &t.Partitions[k]
			if o, ok := starts.Lookup(t.Name, p.ID); ok && o.Err == nil {
				p.StartOffset = o.Offset
			}
			if o, ok := ends.Lookup(t.Name, p.ID); ok && o.Err == nil {
				p.EndOffset = o.Offset
			}
		}
	}
	return &out
}
