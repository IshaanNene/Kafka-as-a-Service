package offsets

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Probe is one record read during a hunt.
type Probe struct {
	At     int64       // the offset asked for
	Record *kgo.Record // the first record at or after At; nil past the last
	Bad    bool
	Took   time.Duration
}

// Hunt is a bisect over one partition for the first record a predicate
// calls bad, assuming that once records turn bad they stay bad.
type Hunt struct {
	Topic      string
	Partition  int32
	Start, End int64 // the log
	Lo, Hi     int64 // every record before Lo is good; the first at or after Hi is bad, or Hi is End
	Testing    int64 // the offset being read now; -1 between reads
	Probes     []Probe
	Found      *kgo.Record // the first bad record; nil when there is none
	Done       bool
}

// Left is how many offsets are still in the range.
func (h Hunt) Left() int64 { return h.Hi - h.Lo }

// Reader reads single records from one partition's leader.
type Reader struct {
	cl        *kgo.Client
	topic     string
	id        [16]byte
	partition int32
	leader    int32
}

// NewReader finds topic's ID and partition's leader.
func NewReader(ctx context.Context, cl *kgo.Client, topic string, partition int32) (*Reader, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Topics) != 1 {
		return nil, fmt.Errorf("%s: %w", topic, ErrNoTopic)
	}
	rt := resp.Topics[0]
	if err := kerr.ErrorForCode(rt.ErrorCode); err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil, fmt.Errorf("%s: %w", topic, ErrNoTopic)
		}
		return nil, err
	}
	for _, p := range rt.Partitions {
		if p.Partition == partition {
			return &Reader{cl: cl, topic: topic, id: rt.TopicID, partition: partition, leader: p.Leader}, nil
		}
	}
	return nil, fmt.Errorf("%s has no partition %d", topic, partition)
}

// At reads the first record at or after offset, nil when there is none
// before the end.
func (r *Reader) At(ctx context.Context, offset int64) (*kgo.Record, error) {
	for range 8 { // aborted or control batches can fill a fetch; read on past them
		req := kmsg.NewPtrFetchRequest()
		req.MaxWaitMillis = 0
		req.MinBytes = 1
		req.MaxBytes = 4 << 20
		req.IsolationLevel = 1 // read committed, as consumers do by default
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = r.topic, r.id
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = r.partition, offset, 1<<20
		rp.CurrentLeaderEpoch, rp.LogStartOffset = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		kr, err := r.cl.Broker(int(r.leader)).Request(ctx, req)
		if err != nil {
			return nil, err
		}
		resp := kr.(*kmsg.FetchResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			return nil, err
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			return nil, errors.New("the leader sent no partition back")
		}
		fp, last := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
			Offset: offset, Topic: r.topic, Partition: r.partition, IsolationLevel: kgo.ReadCommitted(),
		}, &resp.Topics[0].Partitions[0], kgo.DefaultDecompressor(), nil)
		if fp.Err != nil {
			return nil, fp.Err
		}
		for _, rec := range fp.Records {
			if rec.Offset >= offset {
				return rec, nil
			}
		}
		if last < offset || offset >= fp.HighWatermark {
			return nil, nil // nothing more before the end
		}
		offset = last + 1
	}
	return nil, nil
}

// Bisect hunts partition between start and end for the first record bad
// calls bad, calling step before and after each read.
func Bisect(ctx context.Context, r *Reader, start, end int64, bad func(*kgo.Record) bool, step func(Hunt)) (Hunt, error) {
	h := Hunt{Topic: r.topic, Partition: r.partition, Start: start, End: end, Lo: start, Hi: end, Testing: -1}
	var hiRec *kgo.Record
	for h.Lo < h.Hi {
		mid := h.Lo + (h.Hi-h.Lo)/2
		h.Testing = mid
		if step != nil {
			step(h)
		}
		t0 := time.Now()
		rec, err := r.At(ctx, mid)
		if err != nil {
			return h, fmt.Errorf("reading %s p%d at %d: %w", r.topic, r.partition, mid, err)
		}
		p := Probe{At: mid, Record: rec, Took: time.Since(t0)}
		switch {
		case rec == nil || rec.Offset >= h.Hi:
			// No record from mid up to Hi: the first at or after mid is Hi's.
			h.Hi = mid
		case bad(rec):
			p.Bad, h.Hi, hiRec = true, mid, rec
		default:
			h.Lo = rec.Offset + 1
		}
		h.Probes, h.Testing = append(h.Probes, p), -1
		if step != nil {
			step(h)
		}
	}
	if h.Hi < end {
		h.Found = hiRec
		if h.Found == nil || h.Found.Offset < h.Hi {
			// Hi came from an empty stretch; read the bad record it stands for.
			rec, err := r.At(ctx, h.Hi)
			if err != nil {
				return h, err
			}
			h.Found = rec
		}
	}
	h.Done = true
	if step != nil {
		step(h)
	}
	return h, nil
}
