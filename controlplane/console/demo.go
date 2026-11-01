package console

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/schemas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/quotas"
)

// Demo is a fleet of in-memory clusters with traffic, consumer groups,
// ACLs, quotas, a schema registry and desired-state files that have
// drifted, so the console can be tried, and tested, without Kafka.
type Demo struct {
	Config Config
	dir    string
	stop   []func()
}

type demoCluster struct {
	name, env, region string
	brokers           int
	topics            []demoTopic
	groups            []demoGroup
	acls              []acls.ACL
	quotas            []quotas.Quota
	desired           string
	subjects          [][3]string // subject, type, compatibility
}

type demoTopic struct {
	name       string
	partitions int32
	rate       int // records per second
	configs    map[string]string
}

type demoGroup struct {
	name, topic string
	slow        bool // reads in bursts, so its lag rises and falls
}

func allow(principal, resource, name, op string) acls.ACL {
	a, _ := acls.Normalize(acls.ACL{Principal: principal, Resource: resource, Name: name, Operation: op})
	return a
}

var demoFleet = []demoCluster{
	{
		name: "prod-eu", env: "production", region: "eu-west-1", brokers: 3,
		topics: []demoTopic{
			{"orders", 8, 420, map[string]string{"retention.ms": "604800000"}},
			{"payments", 6, 140, map[string]string{"retention.ms": "2592000000", "min.insync.replicas": "2"}},
			{"clickstream", 12, 900, nil},
			{"inventory.changes", 4, 60, map[string]string{"cleanup.policy": "compact"}},
			{"notifications", 3, 35, nil},
		},
		groups: []demoGroup{{"billing", "orders", false}, {"fraud-scoring", "payments", true}, {"analytics-sink", "clickstream", true}, {"stock-sync", "inventory.changes", false}},
		acls: []acls.ACL{
			allow("User:billing", "topic", "orders", "read"), allow("User:billing", "group", "billing", "read"),
			allow("User:fraud", "topic", "payments", "read"), allow("User:analytics", "topic", "clickstream", "read"),
			allow("User:legacy-etl", "topic", "payments", "write"),
		},
		quotas:   []quotas.Quota{{User: "analytics", Values: map[string]float64{"consumer_byte_rate": 20 << 20}}},
		subjects: [][3]string{{"orders-value", "AVRO", ""}, {"payments-value", "AVRO", "FULL"}, {"clickstream-value", "JSON", ""}, {"inventory.changes-value", "PROTOBUF", "BACKWARD_TRANSITIVE"}},
		desired: `cluster: prod-eu
topics:
  orders:
    partitions: 12          # was 8: Black Friday headroom
    replication: 3
    configs: {retention.ms: 7d}
  payments:
    partitions: 6
    replication: 3
    configs: {retention.ms: 14d, min.insync.replicas: "2"}
  clickstream: {partitions: 12, replication: 3}
  inventory.changes:
    partitions: 4
    replication: 3
    configs: {cleanup.policy: compact}
  notifications: {partitions: 3, replication: 3}
  refunds:
    partitions: 6
    replication: 3
    configs: {retention.ms: 30d}
acls:
  - {principal: User:billing, resource: topic, name: orders, operations: [read]}
  - {principal: User:billing, resource: group, name: billing, operation: read}
  - {principal: User:billing, resource: topic, name: refunds, operations: [read, write]}
  - {principal: User:fraud, resource: topic, name: payments, operation: read}
  - {principal: User:analytics, resource: topic, name: clickstream, operation: read}
quotas:
  - {user: analytics, values: {consumer_byte_rate: 20MiB}}
  - {user: legacy-etl, values: {producer_byte_rate: 1MiB}}
policies:
  - {name: replicated, rule: topic.replication >= 3, message: production topics need three replicas}
prune: {acls: true}
`,
	},
	{
		name: "prod-us", env: "production", region: "us-east-1", brokers: 3,
		topics: []demoTopic{
			{"orders", 12, 610, map[string]string{"retention.ms": "604800000"}},
			{"payments", 6, 210, nil},
			{"clickstream", 12, 1300, nil},
			{"search.queries", 6, 380, nil},
		},
		groups: []demoGroup{{"billing", "orders", false}, {"fraud-scoring", "payments", false}, {"search-indexer", "search.queries", false}, {"analytics-sink", "clickstream", false}},
		acls: []acls.ACL{
			allow("User:billing", "topic", "orders", "read"), allow("User:fraud", "topic", "payments", "read"),
			allow("User:search", "topic", "search.queries", "read"),
		},
		subjects: [][3]string{{"orders-value", "AVRO", ""}, {"payments-value", "AVRO", "FULL"}, {"search.queries-value", "JSON", ""}},
		desired: `cluster: prod-us
topics:
  orders: {partitions: 12, replication: 3, configs: {retention.ms: 7d}}
  payments: {partitions: 6, replication: 3}
  clickstream: {partitions: 12, replication: 3}
  search.queries: {partitions: 6, replication: 3}
acls:
  - {principal: User:billing, resource: topic, name: orders, operation: read}
  - {principal: User:fraud, resource: topic, name: payments, operation: read}
  - {principal: User:search, resource: topic, name: search.queries, operation: read}
policies:
  - {name: replicated, rule: topic.replication >= 3, message: production topics need three replicas}
`,
	},
	{
		name: "staging", env: "staging", region: "eu-west-1", brokers: 1,
		topics: []demoTopic{
			{"orders", 3, 25, nil},
			{"payments", 3, 10, nil},
			{"load-test", 6, 150, map[string]string{"retention.ms": "3600000"}},
		},
		groups: []demoGroup{{"billing", "orders", false}, {"perf-reader", "load-test", true}},
		desired: `cluster: staging
topics:
  orders: {partitions: 3}
  payments: {partitions: 3}
  load-test: {partitions: 6, configs: {retention.ms: 1h}}
  scratch: {partitions: 1}
policies:
  - {name: min-partitions, rule: topic.partitions >= 3, message: one partition can't be consumed in parallel}
`,
	},
}

// StartDemo starts the demo fleet and writes its desired-state files to a
// temporary directory. Close stops it all.
func StartDemo(ctx context.Context) (*Demo, error) {
	d := &Demo{}
	dir, err := os.MkdirTemp("", "kaas-demo-")
	if err != nil {
		return nil, err
	}
	d.dir = dir
	d.stop = append(d.stop, func() { _ = os.RemoveAll(dir) })
	for _, dc := range demoFleet {
		reg := schemas.NewMemory()
		regURL, err := d.serve(reg)
		if err != nil {
			d.Close()
			return nil, err
		}
		seeds, err := d.start(ctx, dc, reg)
		if err != nil {
			d.Close()
			return nil, fmt.Errorf("demo %s: %w", dc.name, err)
		}
		path := filepath.Join(dir, dc.name+".yaml")
		if err := os.WriteFile(path, []byte(dc.desired), 0o600); err != nil {
			d.Close()
			return nil, err
		}
		cc := ClusterConfig{Name: dc.name, Brokers: seeds, Env: dc.env, Region: dc.region, Desired: path}
		if len(dc.subjects) > 0 {
			cc.SchemaRegistry = regURL
		}
		d.Config.Clusters = append(d.Config.Clusters, cc)
	}
	// A disaster-recovery cluster that doesn't answer, to show what that
	// looks like.
	d.Config.Clusters = append(d.Config.Clusters, ClusterConfig{Name: "dr-ap-south", Brokers: []string{"127.0.0.1:1"}, Env: "production", Region: "ap-south-1"})
	return d, nil
}

// Close stops every demo cluster and removes the files.
func (d *Demo) Close() {
	for i := len(d.stop) - 1; i >= 0; i-- {
		d.stop[i]()
	}
	d.stop = nil
}

func (d *Demo) serve(h http.Handler) (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	d.stop = append(d.stop, func() { _ = srv.Close() })
	return "http://" + ln.Addr().String(), nil
}

func (d *Demo) start(ctx context.Context, dc demoCluster, reg *schemas.Memory) ([]string, error) {
	c, err := kfake.NewCluster(kfake.NumBrokers(dc.brokers), kfake.ClusterID("kaas-demo-"+dc.name))
	if err != nil {
		return nil, err
	}
	d.stop = append(d.stop, c.Close)
	seeds := c.ListenAddrs()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ProducerLinger(50*time.Millisecond))
	if err != nil {
		return nil, err
	}
	d.stop = append(d.stop, cl.Close)
	adm := kadm.NewClient(cl)
	for _, t := range dc.topics {
		cfg := map[string]*string{}
		for k, v := range t.configs {
			cfg[k] = &v
		}
		if _, err := adm.CreateTopics(ctx, t.partitions, int16(dc.brokers), cfg, t.name); err != nil {
			return nil, err
		}
	}
	if err := acls.Create(ctx, cl, dc.acls); err != nil {
		return nil, err
	}
	qs := make([]quotas.Change, len(dc.quotas))
	for i, q := range dc.quotas {
		qs[i] = quotas.Change{Quota: q}
	}
	if err := quotas.Apply(ctx, adm, qs, false); err != nil {
		return nil, err
	}
	for _, s := range dc.subjects {
		for v := 1; v <= 1+rand.IntN(4); v++ {
			reg.Register(s[0], s[1], demoSchema(s[0], s[1], v), s[2])
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	d.stop = append(d.stop, func() { cancel(); wg.Wait() })
	for _, t := range dc.topics {
		wg.Add(1)
		go func() { defer wg.Done(); produce(runCtx, cl, t) }()
	}
	for _, g := range dc.groups {
		wg.Add(1)
		go func() { defer wg.Done(); consume(runCtx, seeds, g) }()
	}
	if dc.brokers > 1 {
		// Leaders move now and then, as they do in a rolling restart.
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(time.Duration(50+rand.IntN(30)) * time.Second)
			defer t.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
					c.ShufflePartitionLeaders()
				}
			}
		}()
	}
	return seeds, nil
}

// produce writes t.rate records a second, with a gentle swell so charts
// aren't flat.
func produce(ctx context.Context, cl *kgo.Client, t demoTopic) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		el := time.Since(start).Seconds()
		swell := 1 + 0.35*math.Sin(el/40) + 0.1*rand.Float64()
		batch := int(float64(t.rate) * swell / 4)
		for range batch {
			n++
			v := fmt.Appendf(nil, `{"id":%d,"amount":%d,"region":"%s"}`, n, 5+rand.IntN(900), []string{"eu", "us", "apac"}[rand.IntN(3)])
			cl.Produce(ctx, &kgo.Record{Topic: t.name, Key: fmt.Appendf(nil, "k%d", rand.IntN(5000)), Value: v}, nil)
		}
	}
}

func consume(ctx context.Context, seeds []string, g demoGroup) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumerGroup(g.name), kgo.ConsumeTopics(g.topic),
		kgo.ClientID(g.name), kgo.AutoCommitInterval(2*time.Second))
	if err != nil {
		return
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fs := cl.PollRecords(ctx, 2000)
		if errors.Is(fs.Err0(), context.Canceled) {
			return
		}
		if g.slow {
			// Read in bursts with pauses, so lag climbs and drains.
			pause := 400 * time.Millisecond
			if int(time.Now().Unix()/45)%2 == 0 {
				pause = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
		}
	}
}

func demoSchema(subject, typ string, version int) string {
	name := subject
	for i, r := range name {
		if r == '-' || r == '.' {
			name = name[:i]
			break
		}
	}
	switch typ {
	case "PROTOBUF":
		return fmt.Sprintf("syntax = \"proto3\";\npackage shop.v%d;\n\nmessage %s {\n  string sku = 1;\n  int64 delta = 2;\n  string warehouse = 3;\n}\n", version, title(name))
	case "JSON":
		return fmt.Sprintf(`{"$schema":"http://json-schema.org/draft-07/schema#","title":"%s","type":"object","properties":{"id":{"type":"string"},"at":{"type":"string","format":"date-time"},"v":{"const":%d}},"required":["id","at"]}`, title(name), version)
	}
	fields := `{"name":"id","type":"string"},{"name":"amount","type":"long"},{"name":"currency","type":"string","default":"EUR"}`
	if version > 1 {
		fields += `,{"name":"region","type":["null","string"],"default":null}`
	}
	if version > 2 {
		fields += `,{"name":"channel","type":{"type":"enum","name":"Channel","symbols":["WEB","APP","POS"]},"default":"WEB"}`
	}
	return fmt.Sprintf(`{"type":"record","name":"%s","namespace":"com.shop","fields":[%s]}`, title(name), fields)
}

func title(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 'a' - 'A'
	}
	return string(b)
}
