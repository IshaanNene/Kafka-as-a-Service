package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

// Kind names a change between two consecutive snapshots.
type Kind string

const (
	BrokerAdded      Kind = "BrokerAdded"
	BrokerRemoved    Kind = "BrokerRemoved"
	ControllerMoved  Kind = "ControllerMoved"
	TopicCreated     Kind = "TopicCreated"
	TopicDeleted     Kind = "TopicDeleted"
	PartitionsAdded  Kind = "PartitionsAdded"
	LeaderMoved      Kind = "LeaderMoved"
	PartitionOffline Kind = "PartitionOffline" // leader became -1
	ISRShrunk        Kind = "ISRShrunk"
	ISRExpanded      Kind = "ISRExpanded"
)

// Event is one typed change. Fields not relevant to a Kind are zero;
// Partition is -1 for events that are not about a single partition.
type Event struct {
	Kind      Kind
	Topic     string  `json:",omitempty"`
	Partition int32   // -1 when not partition-scoped
	Broker    int32   `json:",omitempty"` // BrokerAdded, BrokerRemoved
	From      int32   `json:",omitempty"` // ControllerMoved, LeaderMoved, PartitionOffline
	To        int32   `json:",omitempty"` // ControllerMoved, LeaderMoved
	Count     int     `json:",omitempty"` // PartitionsAdded, TopicCreated
	Replicas  []int32 `json:",omitempty"` // ISRShrunk: removed; ISRExpanded: added
}

func (e Event) String() string {
	switch e.Kind {
	case BrokerAdded, BrokerRemoved:
		return fmt.Sprintf("%s broker %d", e.Kind, e.Broker)
	case ControllerMoved:
		return fmt.Sprintf("%s %d → %d", e.Kind, e.From, e.To)
	case TopicCreated:
		return fmt.Sprintf("%s %s (%d partitions)", e.Kind, e.Topic, e.Count)
	case TopicDeleted:
		return fmt.Sprintf("%s %s", e.Kind, e.Topic)
	case PartitionsAdded:
		return fmt.Sprintf("%s %s +%d", e.Kind, e.Topic, e.Count)
	case LeaderMoved:
		return fmt.Sprintf("%s %s/%d %d → %d", e.Kind, e.Topic, e.Partition, e.From, e.To)
	case PartitionOffline:
		return fmt.Sprintf("%s %s/%d (was leader %d)", e.Kind, e.Topic, e.Partition, e.From)
	case ISRShrunk:
		return fmt.Sprintf("%s %s/%d -%v", e.Kind, e.Topic, e.Partition, e.Replicas)
	case ISRExpanded:
		return fmt.Sprintf("%s %s/%d +%v", e.Kind, e.Topic, e.Partition, e.Replicas)
	}
	return string(e.Kind)
}

// Diff returns the events that turn prev into next, in a deterministic
// order: brokers, controller, then topics by name and partitions by ID.
// A nil prev yields no events; the first snapshot is a baseline, not news.
// Diff reads metadata only; offset movement is not an event.
func Diff(prev, next *snapshot.Snapshot) []Event {
	if prev == nil || next == nil {
		return nil
	}
	var evs []Event

	pb, nb := brokerIDs(prev), brokerIDs(next)
	for _, id := range nb {
		if _, ok := slices.BinarySearch(pb, id); !ok {
			evs = append(evs, Event{Kind: BrokerAdded, Partition: -1, Broker: id})
		}
	}
	for _, id := range pb {
		if _, ok := slices.BinarySearch(nb, id); !ok {
			evs = append(evs, Event{Kind: BrokerRemoved, Partition: -1, Broker: id})
		}
	}
	if prev.Controller != next.Controller {
		evs = append(evs, Event{Kind: ControllerMoved, Partition: -1, From: prev.Controller, To: next.Controller})
	}

	// Both topic lists are sorted by name, so a merge join visits each once.
	i, j := 0, 0
	for i < len(prev.Topics) || j < len(next.Topics) {
		var c int
		switch {
		case i == len(prev.Topics):
			c = 1
		case j == len(next.Topics):
			c = -1
		default:
			c = strings.Compare(prev.Topics[i].Name, next.Topics[j].Name)
		}
		switch {
		case c < 0:
			evs = append(evs, Event{Kind: TopicDeleted, Topic: prev.Topics[i].Name, Partition: -1})
			i++
		case c > 0:
			t := next.Topics[j]
			evs = append(evs, Event{Kind: TopicCreated, Topic: t.Name, Partition: -1, Count: len(t.Partitions)})
			j++
		default:
			evs = diffTopic(evs, &prev.Topics[i], &next.Topics[j])
			i++
			j++
		}
	}
	return evs
}

func diffTopic(evs []Event, prev, next *snapshot.Topic) []Event {
	if n := len(next.Partitions) - len(prev.Partitions); n > 0 {
		evs = append(evs, Event{Kind: PartitionsAdded, Topic: next.Name, Partition: -1, Count: n})
	}
	// Partitions are sorted by ID and Kafka never removes them, so the
	// common prefix lines up index for index.
	for k := range min(len(prev.Partitions), len(next.Partitions)) {
		p, n := &prev.Partitions[k], &next.Partitions[k]
		if p.Leader != n.Leader {
			if n.Leader < 0 {
				evs = append(evs, Event{Kind: PartitionOffline, Topic: next.Name, Partition: n.ID, From: p.Leader})
			} else {
				evs = append(evs, Event{Kind: LeaderMoved, Topic: next.Name, Partition: n.ID, From: p.Leader, To: n.Leader})
			}
		}
		if removed := missing(p.ISR, n.ISR); len(removed) > 0 {
			evs = append(evs, Event{Kind: ISRShrunk, Topic: next.Name, Partition: n.ID, Replicas: removed})
		}
		if added := missing(n.ISR, p.ISR); len(added) > 0 {
			evs = append(evs, Event{Kind: ISRExpanded, Topic: next.Name, Partition: n.ID, Replicas: added})
		}
	}
	return evs
}

func brokerIDs(s *snapshot.Snapshot) []int32 {
	ids := make([]int32, len(s.Brokers))
	for i, b := range s.Brokers {
		ids[i] = b.ID
	}
	slices.Sort(ids)
	return ids
}

// missing returns the members of a that are not in b, in a's order.
// ISRs hold a handful of IDs, so a linear scan beats building a set.
func missing(a, b []int32) []int32 {
	var out []int32
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}
	return out
}
