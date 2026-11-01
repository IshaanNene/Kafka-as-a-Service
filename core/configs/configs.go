// Package configs reads a topic's or a broker's configuration with where
// each value comes from, its type, and its documentation, and sorts it
// into sections for a settings screen.
package configs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ErrNoTopic is a topic the cluster doesn't have.
var ErrNoTopic = errors.New("no such topic")

// Where a value comes from, from the most specific.
const (
	LevelDefault = iota // Kafka's own default
	LevelStatic         // server.properties on the broker
	LevelCluster        // a dynamic default for every broker
	LevelBroker         // set dynamically on this broker
	LevelTopic          // set on the topic
)

// Entry is one config.
type Entry struct {
	Key       string
	Value     string
	Null      bool // no value: unset, or sensitive
	Sensitive bool
	ReadOnly  bool
	Level     int
	Type      kmsg.ConfigType
	Doc       string
	Default   string // what it would be without the value set here; "" when unknown
	HasDef    bool
}

// Set is whether the value is set rather than Kafka's default. For a topic,
// a value inherited from a broker override is set, but not here.
func (e Entry) Set() bool { return e.Level != LevelDefault }

// level is a config source's level.
func level(s kmsg.ConfigSource) int {
	switch s {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return LevelTopic
	case kmsg.ConfigSourceDynamicBrokerConfig, kmsg.ConfigSourceDynamicBrokerLoggerConfig:
		return LevelBroker
	case kmsg.ConfigSourceDynamicDefaultBrokerConfig:
		return LevelCluster
	case kmsg.ConfigSourceStaticBrokerConfig:
		return LevelStatic
	}
	return LevelDefault
}

// Describe reads a topic's configs, or a broker's when broker is not nil:
// a broker answers for itself, so it is asked directly.
func Describe(ctx context.Context, cl *kgo.Client, topic string, broker *int32) ([]Entry, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.IncludeSynonyms = true
	req.IncludeDocumentation = true
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, topic
	var (
		resp *kmsg.DescribeConfigsResponse
		err  error
	)
	if broker != nil {
		r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeBroker, strconv.Itoa(int(*broker))
		req.Resources = append(req.Resources, r)
		resp, err = req.RequestWith(ctx, cl.Broker(int(*broker)))
	} else {
		req.Resources = append(req.Resources, r)
		resp, err = req.RequestWith(ctx, cl)
	}
	if err != nil {
		return nil, err
	}
	if len(resp.Resources) != 1 {
		return nil, fmt.Errorf("asked for one resource's configs, got %d", len(resp.Resources))
	}
	rr := resp.Resources[0]
	if err := kerr.ErrorForCode(rr.ErrorCode); err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil, fmt.Errorf("%s: %w", topic, ErrNoTopic)
		}
		if rr.ErrorMessage != nil && *rr.ErrorMessage != "" {
			return nil, fmt.Errorf("%w: %s", err, *rr.ErrorMessage)
		}
		return nil, err
	}
	out := make([]Entry, 0, len(rr.Configs))
	for _, c := range rr.Configs {
		e := Entry{
			Key: c.Name, Null: c.Value == nil, Sensitive: c.IsSensitive, ReadOnly: c.ReadOnly,
			Level: level(c.Source), Type: c.ConfigType,
		}
		if c.Value != nil && !c.IsSensitive {
			e.Value = *c.Value
		}
		e.Null = e.Null || c.IsSensitive // a sensitive value is never shown
		if c.Documentation != nil {
			e.Doc = *c.Documentation
		}
		// The synonyms are the value's fallbacks, the most specific
		// first; the first below its own level is what it would be.
		for _, s := range c.ConfigSynonyms {
			if level(s.Source) < e.Level && s.Value != nil {
				e.Default, e.HasDef = *s.Value, true
				break
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Section is a heading and its configs.
type Section struct {
	Name    string
	Entries []Entry
}

// topicSections and brokerSections sort configs by key prefix: the first
// section with a matching prefix takes a config, and Other the rest.
var (
	topicSections = []struct {
		name     string
		prefixes []string
	}{
		{"Retention", []string{"retention.", "local.retention.", "delete.retention.", "remote.storage.", "remote.log."}},
		{"Cleanup", []string{"cleanup.", "min.cleanable.", "min.compaction.", "max.compaction.", "file.delete."}},
		{"Segments", []string{"segment.", "index.", "preallocate", "flush."}},
		{"Messages", []string{"max.message.", "message.", "compression.", "follower.replication.", "leader.replication."}},
		{"Replication", []string{"min.insync.", "unclean.", "replication."}},
	}
	brokerSections = []struct {
		name     string
		prefixes []string
	}{
		{"Topics", []string{"auto.create.", "default.replication.", "num.partitions", "min.insync.", "unclean.", "delete.topic."}},
		{"Log", []string{"log.", "num.recovery.", "compression.", "message.max."}},
		{"Replication", []string{"replica.", "num.replica.", "leader.", "auto.leader."}},
		{"Groups", []string{"group.", "offsets.", "offset."}},
		{"Transactions", []string{"transaction.", "transactional.", "producer.id."}},
		{"Network", []string{"listener", "advertised.", "socket.", "num.network.", "num.io.", "queued.", "max.connection", "connections.", "connection."}},
		{"Security", []string{"ssl.", "sasl.", "security.", "authorizer.", "principal.", "super.users", "delegation."}},
		{"Quotas", []string{"quota.", "client.quota.", "replication.quota."}},
		{"Node", []string{"broker.", "node.", "process.", "controller.", "metadata.", "zookeeper.", "reserved."}},
	}
)

// Sections sorts entries into sections, in order, the empty ones left out.
func Sections(es []Entry, broker bool) []Section {
	rules := topicSections
	if broker {
		rules = brokerSections
	}
	out := make([]Section, len(rules)+1)
	for i, r := range rules {
		out[i].Name = r.name
	}
	out[len(rules)].Name = "Other"
	for _, e := range es {
		at := len(rules)
	find:
		for i, r := range rules {
			for _, p := range r.prefixes {
				if strings.HasPrefix(e.Key, p) {
					at = i
					break find
				}
			}
		}
		out[at].Entries = append(out[at].Entries, e)
	}
	var kept []Section
	for _, s := range out {
		if len(s.Entries) > 0 {
			kept = append(kept, s)
		}
	}
	return kept
}

// Kind is how a value reads: a switch, a duration, a size, a ratio, or
// anything else.
type Kind int

const (
	KindText Kind = iota
	KindBool
	KindDuration // milliseconds or seconds by the key's suffix; -1 forever
	KindBytes    // -1 unlimited
	KindRatio    // 0 to 1
	KindCount
)

// KindOf is how e's value reads, from its type and key.
func KindOf(e Entry) Kind {
	k := e.Key
	switch {
	case e.Type == kmsg.ConfigTypeBoolean, e.Value == "true" || e.Value == "false":
		return KindBool
	case strings.HasSuffix(k, ".ms") || strings.HasSuffix(k, ".seconds") || strings.HasSuffix(k, ".minutes") || strings.HasSuffix(k, ".hours"):
		return KindDuration
	case strings.HasSuffix(k, ".bytes") || strings.HasSuffix(k, ".bytes.per.second") || strings.Contains(k, ".bytes."):
		return KindBytes
	case strings.HasSuffix(k, ".ratio") || e.Type == kmsg.ConfigTypeDouble:
		return KindRatio
	case e.Type == kmsg.ConfigTypeInt, e.Type == kmsg.ConfigTypeLong, e.Type == kmsg.ConfigTypeShort:
		return KindCount
	}
	if _, err := strconv.ParseInt(e.Value, 10, 64); err == nil && e.Type == kmsg.ConfigTypeUnknown && !e.Null {
		return KindCount
	}
	return KindText
}

// Duration reads a duration config's value; ok is false when it isn't one,
// and forever is -1.
func Duration(e Entry) (d time.Duration, forever, ok bool) {
	n, err := strconv.ParseInt(e.Value, 10, 64)
	if err != nil {
		return 0, false, false
	}
	if n < 0 {
		return 0, true, true
	}
	unit := time.Millisecond
	switch {
	case strings.HasSuffix(e.Key, ".seconds"):
		unit = time.Second
	case strings.HasSuffix(e.Key, ".minutes"):
		unit = time.Minute
	case strings.HasSuffix(e.Key, ".hours"):
		unit = time.Hour
	}
	if n > int64(1<<62)/int64(unit) {
		return 0, true, true // longer than Go can count: forever, in effect
	}
	return time.Duration(n) * unit, false, true
}

// Summary is a short line about a config, for a settings row: the first
// sentence of its documentation, or a line of kaas's own for the ones
// people look for.
func Summary(e Entry) string {
	if s, ok := summaries[e.Key]; ok {
		return s
	}
	doc := strings.TrimSpace(e.Doc)
	if i := strings.Index(doc, ". "); i > 0 {
		doc = doc[:i]
	}
	return strings.TrimSuffix(doc, ".")
}

var summaries = map[string]string{
	"cleanup.policy":                          "Delete old segments, compact by key, or both",
	"compression.type":                        "Codec the broker stores batches in",
	"delete.retention.ms":                     "How long compaction keeps delete tombstones",
	"file.delete.delay.ms":                    "Wait before deleting a file from disk",
	"flush.messages":                          "Force an fsync after this many records",
	"flush.ms":                                "Force an fsync after this long",
	"index.interval.bytes":                    "Bytes between offset index entries",
	"local.retention.bytes":                   "Bytes kept on local disk with tiered storage",
	"local.retention.ms":                      "Time kept on local disk with tiered storage",
	"max.compaction.lag.ms":                   "Longest a record waits to be compacted",
	"max.message.bytes":                       "Largest record batch the topic accepts",
	"message.timestamp.type":                  "Producer's time, or the broker's on append",
	"message.timestamp.after.max.ms":          "How far ahead of the broker a timestamp may be",
	"message.timestamp.before.max.ms":         "How far behind the broker a timestamp may be",
	"message.timestamp.difference.max.ms":     "How far off the broker's clock a timestamp may be",
	"message.downconversion.enable":           "Convert batches for old consumers",
	"min.cleanable.dirty.ratio":               "Share of the log uncompacted before cleaning",
	"min.compaction.lag.ms":                   "Shortest a record stays uncompacted",
	"min.insync.replicas":                     "Replicas that must have a write for acks=all",
	"preallocate":                             "Preallocate segment files on disk",
	"remote.storage.enable":                   "Tier old segments to remote storage",
	"retention.bytes":                         "Bytes kept per partition before deleting",
	"retention.ms":                            "How long records are kept",
	"segment.bytes":                           "Size a segment file rolls at",
	"segment.index.bytes":                     "Size of each segment's offset index",
	"segment.jitter.ms":                       "Random spread on time-based rolls",
	"segment.ms":                              "Age a segment file rolls at",
	"unclean.leader.election.enable":          "Let an out-of-sync replica lead, losing data",
	"leader.replication.throttled.replicas":   "Replicas throttled on the leader side",
	"follower.replication.throttled.replicas": "Replicas throttled on the follower side",
	"auto.create.topics.enable":               "Create topics on first use",
	"default.replication.factor":              "Replicas a new topic gets",
	"num.partitions":                          "Partitions a new topic gets",
	"delete.topic.enable":                     "Allow deleting topics",
	"auto.leader.rebalance.enable":            "Move leadership back to preferred replicas",
	"broker.rack":                             "Rack, for placing replicas apart",
	"log.retention.hours":                     "How long records are kept, by default",
	"log.retention.ms":                        "How long records are kept, by default",
	"log.segment.bytes":                       "Size a segment file rolls at, by default",
	"offsets.retention.minutes":               "How long a group's offsets outlive the group",
	"transactional.id.expiration.ms":          "How long an idle transactional ID lives",
}
