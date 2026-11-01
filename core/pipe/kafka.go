package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	Register(Kind{
		Scheme:  "kafka",
		Summary: "a Kafka topic; read in a consumer group that commits only written records, written idempotently",
		Example: "kafka:orders?group=etl · kafka:orders?from=start&stop=end · kafka:audit?brokers=dr:9092",
		Source:  openKafkaSource,
		Sink:    openKafkaSink,
	})
}

// kafkaOpts are the client options a kafka: endpoint opens with: its
// brokers= parameter or the pipe's cluster, and Env.KafkaOpts.
func kafkaOpts(u *url.URL, env Env) ([]kgo.Opt, []string, error) {
	topics := splitList(pathOf(u))
	if len(topics) == 0 {
		return nil, nil, fmt.Errorf("%s: name a topic, as kafka:orders", u)
	}
	seeds := env.Brokers
	if b := u.Query().Get("brokers"); b != "" {
		seeds = splitList(b)
	}
	if len(seeds) == 0 {
		return nil, nil, fmt.Errorf("%s: no brokers: pass --brokers or brokers= in the spec", u)
	}
	opts := append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, env.KafkaOpts...)
	if id := u.Query().Get("client"); id != "" {
		opts = append(opts, kgo.ClientID(id))
	} else {
		opts = append(opts, kgo.ClientID("kaas-pipe"))
	}
	return opts, topics, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// kafkaSource polls a topic. In a group (group=) it commits a batch's
// offsets only once the pipe acknowledges it, and holds rebalances while
// a batch is in flight, so a member leaving hands over exactly what was
// written: nothing lost, and nothing written twice by a rebalance alone.
// Without a group it reads from= (start, end, or the default, end) and
// commits nothing. stop=end stops at the end offsets as they were when
// it opened, for a bounded copy.
type kafkaSource struct {
	cl    *kgo.Client
	group bool
	batch int

	stop map[string]map[int32]int64 // what's left to read, for stop=end
	mu   sync.Mutex
}

func openKafkaSource(u *url.URL, env Env) (Source, error) {
	opts, topics, err := kafkaOpts(u, env)
	if err != nil {
		return nil, err
	}
	n, err := batchSize(u.Query().Get("batch"), 500)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ConsumeTopics(topics...))
	switch from := query(u, "from", ""); from {
	case "start", "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "", "end", "latest":
		if from == "" && u.Query().Get("stop") == "end" {
			// A bounded copy with no from= means the whole topic.
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		}
	default:
		return nil, fmt.Errorf("from=%s: want start or end", from)
	}
	group := u.Query().Get("group")
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	s := &kafkaSource{cl: cl, group: group != "", batch: n}
	if stop := u.Query().Get("stop"); stop != "" {
		if stop != "end" {
			cl.Close()
			return nil, fmt.Errorf("stop=%s: want end", stop)
		}
		if err := s.bound(topics, group); err != nil {
			cl.Close()
			return nil, err
		}
	}
	return s, nil
}

// bound records where stop=end stops: each partition's end offset now,
// less what the group has already committed.
func (s *kafkaSource) bound(topics []string, group string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(s.cl)
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("listing end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("listing end offsets: %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return fmt.Errorf("listing start offsets: %w", err)
	}
	var committed kadm.OffsetResponses
	if group != "" {
		// A group that never committed is new, not an error. On a new
		// cluster the coordinator may not be up yet: that passes.
		for {
			committed, err = adm.FetchOffsets(ctx, group)
			if err == nil || errors.Is(err, kerr.GroupIDNotFound) {
				break
			}
			if !kerr.IsRetriable(err) || ctx.Err() != nil {
				return fmt.Errorf("fetching %s's offsets: %w", group, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	s.stop = map[string]map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		from := int64(0)
		if st, ok := starts.Lookup(o.Topic, o.Partition); ok {
			from = st.Offset
		}
		if c, ok := committed.Lookup(o.Topic, o.Partition); ok && c.At > from {
			from = c.At
		}
		if o.Offset > from {
			if s.stop[o.Topic] == nil {
				s.stop[o.Topic] = map[int32]int64{}
			}
			s.stop[o.Topic][o.Partition] = o.Offset
		}
	})
	return nil
}

func (s *kafkaSource) done() bool {
	if s.stop == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stop) == 0
}

func (s *kafkaSource) Next(ctx context.Context) (Batch, error) {
	for {
		if s.done() {
			return Batch{}, io.EOF
		}
		fs := s.cl.PollRecords(ctx, s.batch)
		if err := ctx.Err(); err != nil {
			s.allow()
			return Batch{}, err
		}
		if fs.IsClientClosed() {
			return Batch{}, io.EOF
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) {
			if ferr == nil {
				ferr = fmt.Errorf("fetching %s[%d]: %w", t, p, err)
			}
		})
		if ferr != nil {
			s.allow()
			return Batch{}, ferr
		}
		var b Batch
		var kept []*kgo.Record
		fs.EachRecord(func(r *kgo.Record) {
			if !s.wanted(r) {
				return
			}
			kept = append(kept, r)
			b.Records = append(b.Records, fromKgo(r))
		})
		if len(kept) == 0 {
			s.allow()
			continue
		}
		b.Ack = func(ctx context.Context) error {
			defer s.allow()
			if !s.group {
				return nil
			}
			return s.cl.CommitRecords(ctx, kept...)
		}
		return b, nil
	}
}

// wanted is whether r is within a stop=end bound, and notes it read.
func (s *kafkaSource) wanted(r *kgo.Record) bool {
	if s.stop == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := s.stop[r.Topic]
	end, ok := ps[r.Partition]
	if !ok || r.Offset >= end {
		return false
	}
	if r.Offset+1 >= end {
		delete(ps, r.Partition)
		if len(ps) == 0 {
			delete(s.stop, r.Topic)
		}
	}
	return true
}

func (s *kafkaSource) allow() {
	if s.group {
		s.cl.AllowRebalance()
	}
}

func (s *kafkaSource) Close() error {
	s.allow()    // a batch the pipe never acked mustn't hold up leaving
	s.cl.Close() // leaves the group; uncommitted batches go to the next member
	return nil
}

func fromKgo(r *kgo.Record) Record {
	out := Record{Key: r.Key, Value: r.Value, Time: r.Timestamp, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}
	for _, h := range r.Headers {
		out.Headers = append(out.Headers, Header{Key: h.Key, Value: string(h.Value)})
	}
	return out
}

// kafkaSink produces to a topic with the idempotent producer, waiting
// for every record's acknowledgement before a write returns.
type kafkaSink struct {
	cl       *kgo.Client
	topic    string
	keepTime bool
}

func openKafkaSink(u *url.URL, env Env) (Sink, error) {
	opts, topics, err := kafkaOpts(u, env)
	if err != nil {
		return nil, err
	}
	if len(topics) != 1 {
		return nil, fmt.Errorf("%s: a sink writes one topic", u)
	}
	switch acks := query(u, "acks", "all"); acks {
	case "all", "-1":
	case "1", "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default:
		return nil, fmt.Errorf("acks=%s: want all or leader", acks)
	}
	cl, err := kgo.NewClient(append(opts, kgo.DefaultProduceTopic(topics[0]))...)
	if err != nil {
		return nil, err
	}
	return &kafkaSink{cl: cl, topic: topics[0], keepTime: u.Query().Get("time") != "now"}, nil
}

func (s *kafkaSink) Write(ctx context.Context, rs []Record) error {
	recs := make([]*kgo.Record, len(rs))
	for i, r := range rs {
		k := &kgo.Record{Topic: s.topic, Key: r.Key, Value: r.Value}
		if s.keepTime {
			k.Timestamp = r.Time
		}
		for _, h := range r.Headers {
			k.Headers = append(k.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
		}
		recs[i] = k
	}
	return s.cl.ProduceSync(ctx, recs...).FirstErr()
}

func (s *kafkaSink) Close() error {
	s.cl.Close()
	return nil
}
