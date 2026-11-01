// Package console is the web console for a fleet of Kafka clusters: the
// Phase 2 management plane. Each cluster runs the same cluster-state engine
// the CLI uses; the console adds consumer lag, ACLs, quotas, schemas and a
// GitOps plan per cluster, and serves them to the browser as JSON with a
// live event stream.
package console

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/schemas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
	"github.com/IshaanNene/Kafka-as-a-Service/core/quotas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

// Config is a fleet file:
//
//	clusters:
//	  - name: prod-eu
//	    brokers: [kafka-0.prod-eu:9092, kafka-1.prod-eu:9092]
//	    env: production
//	    region: eu-west-1
//	    desired: gitops/prod-eu.yaml
//	    schema_registry: https://schemas.prod-eu:8081
type Config struct {
	Clusters []ClusterConfig `yaml:"clusters"`
}

// ClusterConfig is one cluster in the fleet.
type ClusterConfig struct {
	Name           string   `yaml:"name" json:"name"`
	Brokers        []string `yaml:"brokers" json:"brokers"`
	Env            string   `yaml:"env" json:"env"`         // production, staging, development: shown as a badge
	Region         string   `yaml:"region" json:"region"`   // free text
	Desired        string   `yaml:"desired" json:"desired"` // desired-state file, relative to the fleet file
	SchemaRegistry string   `yaml:"schema_registry" json:"-"`
}

// LoadConfig reads a fleet file. Desired-state paths are made relative to
// the file's directory.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	for i := range c.Clusters {
		if d := c.Clusters[i].Desired; d != "" && !filepath.IsAbs(d) {
			c.Clusters[i].Desired = filepath.Join(filepath.Dir(path), d)
		}
	}
	return c, c.check()
}

func (c Config) check() error {
	if len(c.Clusters) == 0 {
		return errors.New("the fleet has no clusters")
	}
	seen := map[string]bool{}
	for i, cl := range c.Clusters {
		switch {
		case cl.Name == "":
			return fmt.Errorf("clusters[%d] needs a name", i)
		case seen[cl.Name]:
			return fmt.Errorf("cluster %s is listed twice", cl.Name)
		case len(cl.Brokers) == 0:
			return fmt.Errorf("cluster %s needs brokers", cl.Name)
		}
		seen[cl.Name] = true
	}
	return nil
}

// Intervals for the data the engine doesn't refresh itself.
type Intervals struct {
	Groups time.Duration // consumer lag; default 10s
	Access time.Duration // ACLs, quotas, disk and schemas; default 60s
	Plan   time.Duration // re-read the desired file and diff; default 30s
}

func (iv Intervals) withDefaults() Intervals {
	if iv.Groups == 0 {
		iv.Groups = 10 * time.Second
	}
	if iv.Access == 0 {
		iv.Access = 60 * time.Second
	}
	if iv.Plan == 0 {
		iv.Plan = 30 * time.Second
	}
	return iv
}

// historyLen is how many throughput points a cluster keeps: ten minutes at
// the engine's 5-second metadata interval.
const historyLen = 120

// Point is one throughput sample.
type Point struct {
	T int64   `json:"t"` // unix milliseconds
	V float64 `json:"v"` // records per second
}

type topicRate struct {
	sum      int64
	at       time.Time
	rate     float64
	measured bool
}

// Cluster is one cluster's live state.
type Cluster struct {
	Config ClusterConfig

	fleet *Fleet
	cl    *kgo.Client
	adm   *kadm.Client
	eng   *engine.Engine
	reg   *schemas.Client

	mu        sync.RWMutex
	snap      *snapshot.Snapshot
	reachable bool
	lastErr   string
	rates     map[string]*topicRate
	history   []Point
	groups    groups.Sample
	prevGrp   groups.Sample
	groupsErr string
	acls      []acls.ACL
	aclsErr   string
	quotas    []quotas.Quota
	quotasErr string
	disk      map[int32]int64
	subjects  []schemas.Subject
	schemaErr string
	plan      *plan.Plan
	planErr   string
	applying  bool
}

func newCluster(f *Fleet, cfg ClusterConfig, opts []kgo.Opt) (*Cluster, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("kaas-console")}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", cfg.Name, err)
	}
	c := &Cluster{
		Config: cfg, fleet: f, cl: cl, adm: kadm.NewClient(cl),
		eng:   engine.New(cl, engine.Config{SlowInterval: 5 * time.Second}),
		rates: map[string]*topicRate{},
	}
	if cfg.SchemaRegistry != "" {
		if c.reg, err = schemas.New(cfg.SchemaRegistry); err != nil {
			cl.Close()
			return nil, fmt.Errorf("%s: %w", cfg.Name, err)
		}
	}
	return c, nil
}

func (c *Cluster) run(ctx context.Context, iv Intervals) {
	var wg sync.WaitGroup
	loops := []func(context.Context){c.follow, every(iv.Groups, c.refreshGroups), every(iv.Access, c.refreshAccess)}
	if c.Config.Desired != "" {
		loops = append(loops, every(iv.Plan, c.refreshPlan))
	}
	wg.Add(1 + len(loops))
	go func() { defer wg.Done(); _ = c.eng.Run(ctx) }()
	for _, l := range loops {
		go func() { defer wg.Done(); l(ctx) }()
	}
	wg.Wait()
	c.cl.Close()
}

// every runs f now and then every d until ctx ends.
func every(d time.Duration, f func(context.Context)) func(context.Context) {
	return func(ctx context.Context) {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			f(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}
}

// follow takes each snapshot the engine publishes: rates, history, events,
// and whether the cluster answers.
func (c *Cluster) follow(ctx context.Context) {
	sub := c.eng.Subscribe()
	defer sub.Close()
	health := time.NewTicker(2 * time.Second)
	defer health.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-health.C:
			c.checkHealth()
		case <-sub.Ready():
			u, ok := sub.Next()
			if !ok {
				continue
			}
			c.take(u)
		}
	}
}

func (c *Cluster) take(u engine.Update) {
	now := time.Now()
	c.mu.Lock()
	wasReachable := c.reachable
	c.snap, c.reachable, c.lastErr = u.Snapshot, true, ""
	var total float64
	measured := false // some topic has a rate: two reads of its offsets
	seen := map[string]bool{}
	for _, t := range u.Snapshot.Topics {
		seen[t.Name] = true
		var sum int64
		known := !t.OffsetsAt.IsZero()
		for _, p := range t.Partitions {
			if p.EndOffset == snapshot.UnknownOffset {
				known = false
				break
			}
			sum += p.EndOffset
		}
		if !known {
			continue
		}
		r := c.rates[t.Name]
		switch {
		case r == nil:
			c.rates[t.Name] = &topicRate{sum: sum, at: t.OffsetsAt}
		case t.OffsetsAt.After(r.at):
			dt := t.OffsetsAt.Sub(r.at).Seconds()
			r.rate = max(0, float64(sum-r.sum)/dt)
			r.sum, r.at, r.measured = sum, t.OffsetsAt, true
		}
		if r := c.rates[t.Name]; r != nil && r.measured {
			total += r.rate
			measured = true
		}
	}
	for name := range c.rates {
		if !seen[name] {
			delete(c.rates, name)
		}
	}
	if n := len(c.history); measured && (n == 0 || now.UnixMilli()-c.history[n-1].T >= 4500) {
		c.history = append(c.history, Point{T: now.UnixMilli(), V: total})
		if len(c.history) > historyLen {
			c.history = c.history[len(c.history)-historyLen:]
		}
	}
	c.mu.Unlock()

	if !wasReachable {
		c.fleet.publish(Event{Cluster: c.Config.Name, Kind: "ClusterReachable", Severity: SevOK, Text: "connected to " + c.Config.Brokers[0]})
	}
	for _, e := range u.Events {
		c.fleet.publish(fromEngine(c.Config.Name, e))
	}
}

// checkHealth marks the cluster unreachable when the engine's last refresh
// failed and its snapshot is older than three metadata intervals.
func (c *Cluster) checkHealth() {
	st := c.eng.Stats()
	if st.LastError == nil {
		return
	}
	c.mu.Lock()
	stale := c.snap == nil || time.Since(c.snap.TakenAt) > 3*c.eng.Config().MetadataInterval
	was := c.reachable
	c.lastErr = st.LastError.Error()
	if stale {
		c.reachable = false
	}
	c.mu.Unlock()
	if was && stale {
		c.fleet.publish(Event{Cluster: c.Config.Name, Kind: "ClusterUnreachable", Severity: SevCritical, Text: st.LastError.Error()})
	}
}

func (c *Cluster) refreshGroups(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s, err := groups.Fetch(ctx, c.adm)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.groupsErr = err.Error()
		return
	}
	c.prevGrp, c.groups, c.groupsErr = c.groups, s, ""
}

func (c *Cluster) refreshAccess(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	as, aerr := acls.List(ctx, c.cl)
	qs, qerr := quotas.List(ctx, c.adm)
	disk, derr := brokers.Disk(ctx, c.adm)
	var subs []schemas.Subject
	var serr error
	if c.reg != nil {
		subs, serr = c.reg.Subjects(ctx)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acls, c.aclsErr = as, errText(aerr)
	c.quotas, c.quotasErr = qs, errText(qerr)
	if derr == nil {
		c.disk = disk
	}
	if c.reg != nil {
		if serr == nil {
			c.subjects = subs
		}
		c.schemaErr = errText(serr)
	}
}

// refreshPlan re-reads the desired-state file, so a git pull shows up on
// the next pass, and diffs it with the cluster. A plan whose operations
// changed is checked with the controller again.
func (c *Cluster) refreshPlan(ctx context.Context) {
	c.mu.RLock()
	busy := c.applying
	c.mu.RUnlock()
	if busy {
		return
	}
	p, err := c.makePlan(ctx)
	c.mu.Lock()
	prev := c.plan
	if err != nil {
		c.planErr = err.Error()
	} else {
		c.plan, c.planErr = p, ""
	}
	c.mu.Unlock()
	if err == nil && (prev == nil || prev.ID != p.ID) && len(p.Ops) > 0 {
		sev := SevInfo
		if p.Counts()[plan.Destructive] > 0 || p.Blocked() != "" {
			sev = SevWarning
		}
		c.fleet.publish(Event{Cluster: c.Config.Name, Kind: "DriftDetected", Severity: sev, Text: fmt.Sprintf("%s to match %s", plural(len(p.Ops), "change"), filepath.Base(c.Config.Desired))})
	}
}

func (c *Cluster) makePlan(ctx context.Context) (*plan.Plan, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s, err := plan.Load(c.Config.Desired)
	if err != nil {
		return nil, err
	}
	if s.Cluster != "" && s.Cluster != c.Config.Name {
		return nil, fmt.Errorf("%s declares cluster %q, not %q", c.Config.Desired, s.Cluster, c.Config.Name)
	}
	live, err := plan.Read(ctx, c.cl, s)
	if err != nil {
		return nil, err
	}
	p := plan.Diff(s, live)
	c.mu.RLock()
	prev := c.plan
	c.mu.RUnlock()
	if prev != nil && prev.ID == p.ID && prev.Checked {
		// Same operations: keep the controller's answers rather than ask
		// again every pass.
		for i := range p.Ops {
			p.Ops[i].Problem = prev.Ops[i].Problem
		}
		p.Checked = true
		return p, nil
	}
	plan.Check(ctx, c.cl, p)
	return p, nil
}

// ErrPlanChanged is returned by Apply when the cluster or the file changed
// since the plan the caller reviewed.
var ErrPlanChanged = errors.New("the plan changed since it was reviewed")

// ErrApplying is returned by Apply while another apply runs on the cluster.
var ErrApplying = errors.New("a plan is already being applied to this cluster")

// Apply re-plans and applies, provided the fresh plan is the one reviewed.
func (c *Cluster) Apply(ctx context.Context, reviewed string) ([]plan.Result, error) {
	if c.Config.Desired == "" {
		return nil, errors.New("this cluster has no desired-state file")
	}
	c.mu.Lock()
	if c.applying {
		c.mu.Unlock()
		return nil, ErrApplying
	}
	c.applying = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.applying = false
		c.mu.Unlock()
	}()
	p, err := c.makePlan(ctx)
	if err != nil {
		return nil, err
	}
	if p.ID != reviewed {
		c.mu.Lock()
		c.plan = p
		c.mu.Unlock()
		return nil, ErrPlanChanged
	}
	res, err := plan.Apply(ctx, c.cl, p)
	done := 0
	for _, r := range res {
		if r.Done {
			done++
		}
	}
	ev := Event{Cluster: c.Config.Name, Kind: "PlanApplied", Severity: SevOK, Text: fmt.Sprintf("applied %s from %s", plural(done, "change"), filepath.Base(c.Config.Desired))}
	if err != nil {
		ev.Kind, ev.Severity, ev.Text = "PlanFailed", SevCritical, fmt.Sprintf("applied %d of %d changes: %v", done, len(p.Ops), err)
	}
	c.fleet.publish(ev)
	c.eng.Refresh()
	if np, perr := c.makePlan(ctx); perr == nil {
		c.mu.Lock()
		c.plan = np
		c.mu.Unlock()
	}
	return res, err
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Fleet is every cluster the console shows, and the events they raise.
type Fleet struct {
	Clusters []*Cluster
	iv       Intervals

	mu     sync.Mutex
	events []Event // a ring of the newest maxEvents
	seq    uint64
	subs   map[chan Event]struct{}
}

const maxEvents = 500

// NewFleet connects a client per cluster. Nothing is requested until Run.
func NewFleet(cfg Config, iv Intervals, opts ...kgo.Opt) (*Fleet, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	f := &Fleet{iv: iv.withDefaults(), subs: map[chan Event]struct{}{}}
	for _, cc := range cfg.Clusters {
		c, err := newCluster(f, cc, opts)
		if err != nil {
			for _, done := range f.Clusters {
				done.cl.Close()
			}
			return nil, err
		}
		f.Clusters = append(f.Clusters, c)
	}
	return f, nil
}

// Run follows every cluster until ctx ends.
func (f *Fleet) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range f.Clusters {
		wg.Add(1)
		go func() { defer wg.Done(); c.run(ctx, f.iv) }()
	}
	wg.Wait()
}

// Cluster finds a cluster by name.
func (f *Fleet) Cluster(name string) (*Cluster, bool) {
	for _, c := range f.Clusters {
		if c.Config.Name == name {
			return c, true
		}
	}
	return nil, false
}

// Severity is how much an event matters.
type Severity string

// Severities.
const (
	SevOK       Severity = "ok"
	SevInfo     Severity = "info"
	SevWarning  Severity = "warning"
	SevCritical Severity = "critical"
)

// Event is something that happened on a cluster.
type Event struct {
	Seq      uint64    `json:"seq"`
	At       time.Time `json:"at"`
	Cluster  string    `json:"cluster"`
	Kind     string    `json:"kind"`
	Severity Severity  `json:"severity"`
	Text     string    `json:"text"`
	Topic    string    `json:"topic,omitempty"`
}

func fromEngine(cluster string, e engine.Event) Event {
	sev := SevInfo
	switch e.Kind {
	case engine.PartitionOffline, engine.BrokerRemoved:
		sev = SevCritical
	case engine.ISRShrunk:
		sev = SevWarning
	case engine.ISRExpanded, engine.BrokerAdded:
		sev = SevOK
	}
	text := e.String()
	if len(text) > len(e.Kind) && text[:len(e.Kind)] == string(e.Kind) {
		text = text[len(e.Kind)+1:]
	}
	return Event{Cluster: cluster, Kind: string(e.Kind), Severity: sev, Text: text, Topic: e.Topic}
}

func (f *Fleet) publish(e Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	e.Seq = f.seq
	if e.At.IsZero() {
		e.At = time.Now()
	}
	f.events = append(f.events, e)
	if len(f.events) > maxEvents {
		f.events = f.events[len(f.events)-maxEvents:]
	}
	for ch := range f.subs {
		select {
		case ch <- e:
		default: // a slow browser misses events rather than stalling the fleet
		}
	}
}

// Events returns the newest n events, newest last.
func (f *Fleet) Events(n int) []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n > len(f.events) {
		n = len(f.events)
	}
	return append([]Event{}, f.events[len(f.events)-n:]...)
}

func (f *Fleet) subscribe() (chan Event, func()) {
	ch := make(chan Event, 64)
	f.mu.Lock()
	f.subs[ch] = struct{}{}
	f.mu.Unlock()
	return ch, func() {
		f.mu.Lock()
		delete(f.subs, ch)
		f.mu.Unlock()
	}
}
