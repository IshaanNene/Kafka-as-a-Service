package sniff

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// listMax is how many topics or partitions a line names before "+N".
const listMax = 3

// some joins up to listMax of xs and says how many more there are.
func some(xs []string) string {
	if len(xs) > listMax {
		return strings.Join(xs[:listMax], " ") + fmt.Sprintf(" +%d", len(xs)-listMax)
	}
	return strings.Join(xs, " ")
}

// batchRecords counts the records in a produce's record batches: each
// batch says its count 57 bytes in, and its length 8 bytes in.
func batchRecords(b []byte) int {
	n := 0
	for len(b) >= 61 {
		size := int(int32(binary.BigEndian.Uint32(b[8:])))
		n += int(int32(binary.BigEndian.Uint32(b[57:])))
		if size <= 0 || 12+size > len(b) {
			break
		}
		b = b[12+size:]
	}
	return n
}

// batchTime is the first timestamp of the first record batch in b, which
// says it 27 bytes in; v2 batches (magic 2, 16 bytes in) only.
func batchTime(b []byte) (time.Time, bool) {
	if len(b) < 35 || b[16] != 2 {
		return time.Time{}, false
	}
	return time.UnixMilli(int64(binary.BigEndian.Uint64(b[27:]))), true
}

// at says where a ListOffsets asks for.
func at(ts int64) string {
	switch ts {
	case -1:
		return "latest"
	case -2:
		return "earliest"
	case -3:
		return "max time"
	}
	return fmt.Sprintf("time %d", ts)
}

// describe is one line on what a request asks for.
func (p *Proxy) describe(req kmsg.Request) string {
	switch r := req.(type) {
	case *kmsg.ProduceRequest:
		var ts []string
		recs := 0
		for _, t := range r.Topics {
			var ps []string
			for _, pt := range t.Partitions {
				ps = append(ps, fmt.Sprintf("p%d", pt.Partition))
				recs += batchRecords(pt.Records)
			}
			ts = append(ts, p.topic(t.Topic, t.TopicID)+" "+strings.Join(ps, ","))
		}
		acks := map[int16]string{0: "acks 0", 1: "acks 1", -1: "acks all"}[r.Acks]
		return fmt.Sprintf("%s · %s · %s", some(ts), plural(recs, "record"), acks)
	case *kmsg.FetchRequest:
		var ps []string
		for _, t := range r.Topics {
			name := p.topic(t.Topic, t.TopicID)
			for _, pt := range t.Partitions {
				ps = append(ps, fmt.Sprintf("%s p%d@%d", name, pt.Partition, pt.FetchOffset))
			}
		}
		if len(ps) == 0 {
			return fmt.Sprintf("session %d · nothing new to ask for · waits %dms", r.SessionID, r.MaxWaitMillis)
		}
		return fmt.Sprintf("%s · waits %dms", some(ps), r.MaxWaitMillis)
	case *kmsg.MetadataRequest:
		if r.Topics == nil {
			return "all topics"
		}
		var ts []string
		for _, t := range r.Topics {
			if t.Topic != nil {
				ts = append(ts, *t.Topic)
			} else {
				ts = append(ts, p.topic("", t.TopicID))
			}
		}
		if len(ts) == 0 {
			return "brokers only"
		}
		return some(ts)
	case *kmsg.ListOffsetsRequest:
		var ps []string
		for _, t := range r.Topics {
			for _, pt := range t.Partitions {
				ps = append(ps, fmt.Sprintf("%s p%d %s", t.Topic, pt.Partition, at(pt.Timestamp)))
			}
		}
		return some(ps)
	case *kmsg.FindCoordinatorRequest:
		kind := map[int8]string{0: "group", 1: "transaction", 2: "share group"}[r.CoordinatorType]
		keys := r.CoordinatorKeys
		if len(keys) == 0 {
			keys = []string{r.CoordinatorKey}
		}
		return kind + " " + some(keys)
	case *kmsg.JoinGroupRequest:
		return fmt.Sprintf("%s · %s · member %s", r.Group, r.ProtocolType, orNew(r.MemberID))
	case *kmsg.SyncGroupRequest:
		return fmt.Sprintf("%s · generation %d", r.Group, r.Generation)
	case *kmsg.HeartbeatRequest:
		return fmt.Sprintf("%s · generation %d", r.Group, r.Generation)
	case *kmsg.LeaveGroupRequest:
		return r.Group
	case *kmsg.OffsetCommitRequest:
		var ps []string
		for _, t := range r.Topics {
			name := p.topic(t.Topic, t.TopicID)
			for _, pt := range t.Partitions {
				ps = append(ps, fmt.Sprintf("%s p%d→%d", name, pt.Partition, pt.Offset))
			}
		}
		return r.Group + " · " + some(ps)
	case *kmsg.OffsetFetchRequest:
		gs := []string{r.Group}
		if len(r.Groups) > 0 {
			gs = nil
			for _, g := range r.Groups {
				gs = append(gs, g.Group)
			}
		}
		return some(gs)
	case *kmsg.ApiVersionsRequest:
		if r.ClientSoftwareName == "" {
			return "what versions the broker speaks"
		}
		return r.ClientSoftwareName + " " + r.ClientSoftwareVersion
	case *kmsg.InitProducerIDRequest:
		if r.TransactionalID != nil {
			return "transactional " + *r.TransactionalID
		}
		return "idempotent"
	case *kmsg.CreateTopicsRequest:
		var ts []string
		for _, t := range r.Topics {
			ts = append(ts, fmt.Sprintf("%s ×%d", t.Topic, t.NumPartitions))
		}
		return some(ts)
	case *kmsg.DeleteTopicsRequest:
		ts := r.TopicNames
		for _, t := range r.Topics {
			if t.Topic != nil {
				ts = append(ts, *t.Topic)
			} else {
				ts = append(ts, p.topic("", t.TopicID))
			}
		}
		return some(ts)
	}
	return ""
}

// topicsOf is the topics a request names, in order, once each.
func (p *Proxy) topicsOf(req kmsg.Request) []string {
	var ts []string
	seen := map[string]bool{}
	add := func(name string, id [16]byte) {
		if n := p.topic(name, id); !seen[n] {
			seen[n] = true
			ts = append(ts, n)
		}
	}
	switch r := req.(type) {
	case *kmsg.ProduceRequest:
		for _, t := range r.Topics {
			add(t.Topic, t.TopicID)
		}
	case *kmsg.FetchRequest:
		for _, t := range r.Topics {
			add(t.Topic, t.TopicID)
		}
	case *kmsg.ListOffsetsRequest:
		for _, t := range r.Topics {
			add(t.Topic, [16]byte{})
		}
	case *kmsg.OffsetCommitRequest:
		for _, t := range r.Topics {
			add(t.Topic, t.TopicID)
		}
	case *kmsg.MetadataRequest:
		for _, t := range r.Topics {
			if t.Topic != nil {
				add(*t.Topic, [16]byte{})
			} else {
				add("", t.TopicID)
			}
		}
	}
	return ts
}

func orNew(id string) string {
	if id == "" {
		return "new"
	}
	return id
}

func plural(n int, s string) string {
	if n == 1 {
		return "1 " + s
	}
	return fmt.Sprintf("%d %ss", n, s)
}
