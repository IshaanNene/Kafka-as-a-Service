// Package clients finds the clients a cluster can see through the Kafka
// protocol: consumer group members, from describing their groups, and
// active idempotent producers, from DescribeProducers (KIP-664). Kafka has
// no API that lists connections or request rates, so rates here are
// records per second: commits for a member, sequence numbers for a
// producer, each a difference between two samples.
package clients

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Kind is how the cluster sees a client.
type Kind string

const (
	Consumer Kind = "consumer"
	Producer Kind = "producer"
)

// Client is one client at one moment.
type Client struct {
	Kind   Kind
	ID     string // consumer: client.id; producer: its producer ID
	Group  string // consumer only
	Member string // consumer: member ID, which tells apart members sharing a client.id
	Host   string // consumer only, as the broker saw it: "/10.0.0.4"
	Topics []string
	Parts  int
	Lag    int64 // consumer: records behind on its partitions; -1 unknown
	// Marks is a counter per "topic/partition": a consumer's commit, a
	// producer's last sequence. Rates compare partitions in both samples,
	// so a rebalance or a newly written partition is not a burst.
	Marks map[string]int64
	Last  time.Time // producer: its last write
	Epoch int16     // producer
	Txn   bool      // producer: inside an open transaction
	TxnID string    // producer: its transactional.id, when it has one
}

// Key tells clients apart across samples.
func (c Client) Key() string {
	if c.Kind == Consumer {
		return "c\x00" + c.Group + "\x00" + c.Member
	}
	return "p\x00" + c.ID
}

// Sample is every client at one moment, sorted by kind then key.
type Sample struct {
	At      time.Time
	Clients []Client
}

// Fetch reads group members and, for topics, active producers. A failure
// of one source comes back joined in the error with what the other found;
// only both failing returns an empty sample.
func Fetch(ctx context.Context, adm *kadm.Client, topics []string) (Sample, error) {
	s := Sample{At: time.Now()}
	var errs []error
	consumers, err := members(ctx, adm)
	if err != nil {
		errs = append(errs, err)
	}
	s.Clients = append(s.Clients, consumers...)
	if len(topics) > 0 {
		producers, err := producers(ctx, adm, topics)
		if err != nil {
			errs = append(errs, err)
		}
		s.Clients = append(s.Clients, producers...)
	}
	slices.SortFunc(s.Clients, func(a, b Client) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Key(), b.Key()))
	})
	return s, errors.Join(errs...)
}

func members(ctx context.Context, adm *kadm.Client) ([]Client, error) {
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	if len(listed) == 0 {
		return nil, nil
	}
	lags, err := adm.Lag(ctx, listed.Groups()...)
	if err != nil && len(lags) == 0 {
		return nil, fmt.Errorf("group members: %w", err)
	}
	var out []Client
	for _, g := range lags.Sorted() {
		byMember := map[string]*Client{}
		for _, m := range g.Members {
			c := &Client{Kind: Consumer, ID: m.ClientID, Group: g.Group, Member: m.MemberID, Host: m.ClientHost, Lag: -1, Marks: map[string]int64{}}
			byMember[m.MemberID] = c
		}
		for topic, parts := range g.Lag {
			for p, l := range parts {
				if l.Member == nil {
					continue
				}
				c, ok := byMember[l.Member.MemberID]
				if !ok {
					continue
				}
				c.Parts++
				if !slices.Contains(c.Topics, topic) {
					c.Topics = append(c.Topics, topic)
				}
				if l.Commit.At >= 0 {
					c.Marks[fmt.Sprintf("%s/%d", topic, p)] = l.Commit.At
				}
				if l.Lag >= 0 {
					c.Lag = max(c.Lag, 0) + l.Lag
				}
			}
		}
		for _, m := range g.Members {
			c := byMember[m.MemberID]
			slices.Sort(c.Topics)
			out = append(out, *c)
		}
	}
	return out, nil
}

func producers(ctx context.Context, adm *kadm.Client, topics []string) ([]Client, error) {
	set := kadm.TopicsSet{}
	for _, t := range topics {
		set[t] = nil
	}
	described, err := adm.DescribeProducers(ctx, set)
	if err != nil && len(described) == 0 {
		return nil, fmt.Errorf("describe producers: %w", err)
	}
	byID := map[int64]*Client{}
	var ids []int64
	described.EachProducer(func(d kadm.DescribedProducer) {
		c, ok := byID[d.ProducerID]
		if !ok {
			c = &Client{Kind: Producer, ID: fmt.Sprint(d.ProducerID), Lag: -1, Epoch: d.ProducerEpoch, Marks: map[string]int64{}}
			byID[d.ProducerID] = c
			ids = append(ids, d.ProducerID)
		}
		c.Parts++
		if !slices.Contains(c.Topics, d.Topic) {
			c.Topics = append(c.Topics, d.Topic)
		}
		// Sequences count records from 0 per partition and epoch.
		c.Marks[fmt.Sprintf("%s/%d", d.Topic, d.Partition)] = int64(d.LastSequence) + 1
		c.Epoch = max(c.Epoch, d.ProducerEpoch)
		c.Txn = c.Txn || d.CurrentTxnStartOffset >= 0
		if t := time.UnixMilli(d.LastTimestamp); d.LastTimestamp > 0 && t.After(c.Last) {
			c.Last = t
		}
	})
	// Transactional producers have a name worth showing; this is best
	// effort, as listing transactions needs DESCRIBE on them.
	if len(ids) > 0 {
		if txns, terr := adm.ListTransactions(ctx, ids, nil); terr == nil {
			for _, t := range txns {
				if c, ok := byID[t.ProducerID]; ok {
					c.TxnID = t.TxnID
				}
			}
		}
	}
	slices.Sort(ids)
	out := make([]Client, 0, len(ids))
	for _, id := range ids {
		c := byID[id]
		slices.Sort(c.Topics)
		out = append(out, *c)
	}
	return out, err
}

// Rate is a client's records per second between two samples, over the
// partitions it had in both, or -1 when it is new. A counter that went
// back (a producer's new epoch restarts its sequences) counts as zero.
func Rate(prev, cur Sample, c Client) float64 {
	dt := cur.At.Sub(prev.At).Seconds()
	if dt <= 0 {
		return -1
	}
	for _, p := range prev.Clients {
		if p.Key() != c.Key() {
			continue
		}
		var n int64
		for part, at := range c.Marks {
			if was, ok := p.Marks[part]; ok {
				n += max(0, at-was)
			}
		}
		return float64(n) / dt
	}
	return -1
}
