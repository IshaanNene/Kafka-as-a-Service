// Package snapshot holds the immutable cluster snapshots that every KaaS
// surface renders. Screens and the agent never read live client state; they
// read a Snapshot, which carries a generation number so consumers can tell
// fresh data from old.
//
// core/ must not import ui/ or any transport package (see ADR-0003).
package snapshot

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Snapshot is a point-in-time, read-only view of one cluster.
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
}

type Partition struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
	Offline  []int32
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
	i, ok := slices.BinarySearchFunc(s.Topics, name, func(t Topic, n string) int {
		switch {
		case t.Name < n:
			return -1
		case t.Name > n:
			return 1
		}
		return 0
	})
	if !ok {
		return Topic{}, false
	}
	return s.Topics[i], true
}

// Taker produces successive snapshots of one cluster. It is not yet
// interest-scheduled or budgeted; that arrives with the Phase 1 engine.
type Taker struct {
	adm *kadm.Client
	gen uint64
	now func() time.Time
}

func NewTaker(adm *kadm.Client) *Taker {
	return &Taker{adm: adm, now: time.Now}
}

// Take fetches cluster metadata in one request and returns a new Snapshot
// with the next generation number. Topics are sorted by name.
func (t *Taker) Take(ctx context.Context) (*Snapshot, error) {
	md, err := t.adm.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: metadata: %w", err)
	}
	t.gen++
	s := &Snapshot{
		Generation: t.gen,
		TakenAt:    t.now(),
		ClusterID:  md.Cluster,
		Controller: md.Controller,
	}
	for _, b := range md.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		s.Brokers = append(s.Brokers, Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack})
	}
	for _, td := range md.Topics.Sorted() {
		if td.Err != nil {
			return nil, fmt.Errorf("snapshot: topic %q: %w", td.Topic, td.Err)
		}
		topic := Topic{Name: td.Topic, Internal: td.IsInternal}
		for _, pd := range td.Partitions.Sorted() {
			topic.Partitions = append(topic.Partitions, Partition{
				ID:       pd.Partition,
				Leader:   pd.Leader,
				Replicas: slices.Clone(pd.Replicas),
				ISR:      slices.Clone(pd.ISR),
				Offline:  slices.Clone(pd.OfflineReplicas),
			})
		}
		s.Topics = append(s.Topics, topic)
	}
	return s, nil
}
