package console

import (
	"sort"
	"strconv"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/schemas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

// The views are what the browser reads: plain data, computed under the
// cluster's lock from its latest state.

// Status is a cluster's health in one word.
type Status string

// Statuses, worst last.
const (
	StatusConnecting  Status = "connecting"
	StatusHealthy     Status = "healthy"
	StatusDegraded    Status = "degraded"    // under-replicated partitions
	StatusCritical    Status = "critical"    // offline partitions
	StatusUnreachable Status = "unreachable" // no answer for three metadata intervals
)

// Card is a cluster's summary on the fleet page.
type Card struct {
	Name        string  `json:"name"`
	Env         string  `json:"env"`
	Region      string  `json:"region"`
	Status      Status  `json:"status"`
	Reason      string  `json:"reason"`
	Error       string  `json:"error,omitempty"`
	ClusterID   string  `json:"clusterId"`
	Brokers     int     `json:"brokers"`
	Controller  int32   `json:"controller"`
	Topics      int     `json:"topics"`
	Partitions  int     `json:"partitions"`
	URP         int     `json:"urp"`
	Offline     int     `json:"offline"`
	Records     int64   `json:"records"`
	Throughput  float64 `json:"throughput"`
	History     []Point `json:"history"`
	ConsumeRate float64 `json:"consumeRate"`
	Consumed    []Point `json:"consumed"`
	Groups      int     `json:"groups"`
	Lag         int64   `json:"lag"`
	TopLag      string  `json:"topLag,omitempty"`
	// Freshness is how far behind the furthest-behind group is, in
	// seconds of the topic's traffic: 0 caught up, -1 no groups or no rate.
	Freshness  float64 `json:"freshness"`
	FreshGroup string  `json:"freshGroup,omitempty"`
	// Uptime is the last hour in minute slots, oldest first; a slot with
	// no samples has no status. Availability is the share of samples in
	// the hour the cluster answered with every partition online, as a
	// percentage; -1 before the first sample.
	Uptime       []Slot   `json:"uptime"`
	Availability float64  `json:"availability"`
	Disk         int64    `json:"disk"` // bytes on every broker, all replicas
	Cost         float64  `json:"cost"` // estimated per month; 0 without pricing
	Drift        int      `json:"drift"`
	DriftRisk    RiskSums `json:"driftRisk"`
	Blocked      string   `json:"blocked,omitempty"`
	GitOps       bool     `json:"gitops"`
	Schemas      int      `json:"schemas"`
	HasRegistry  bool     `json:"hasRegistry"`
	ACLs         int      `json:"acls"`
	UpdatedAt    int64    `json:"updatedAt"`
}

// Slot is one minute of the uptime timeline: the worst status seen in it.
type Slot struct {
	T int64  `json:"t"`
	S Status `json:"s,omitempty"`
}

const (
	uptimeSlots = 60
	uptimeSlot  = time.Minute
)

// uptime buckets the health samples into the last hour's minutes.
func uptime(hs []healthSample, now time.Time) ([]Slot, float64) {
	end := now.Truncate(uptimeSlot).Add(uptimeSlot)
	start := end.Add(-uptimeSlots * uptimeSlot)
	slots := make([]Slot, uptimeSlots)
	for i := range slots {
		slots[i].T = start.Add(time.Duration(i) * uptimeSlot).UnixMilli()
	}
	up, n := 0, 0
	for _, h := range hs {
		if h.at.Before(start) {
			continue
		}
		i := int(h.at.Sub(start) / uptimeSlot)
		if i >= uptimeSlots {
			i = uptimeSlots - 1
		}
		if worse(h.s, slots[i].S) {
			slots[i].S = h.s
		}
		n++
		if h.s == StatusHealthy || h.s == StatusDegraded {
			up++
		}
	}
	if n == 0 {
		return slots, -1
	}
	return slots, 100 * float64(up) / float64(n)
}

var statusRank = map[Status]int{"": 0, StatusConnecting: 1, StatusHealthy: 2, StatusDegraded: 3, StatusCritical: 4, StatusUnreachable: 5}

func worse(a, b Status) bool { return statusRank[a] > statusRank[b] }

// status is the cluster's health in a word and why. The caller holds mu.
func (c *Cluster) status() (Status, string) {
	if !c.reachable && c.lastErr != "" {
		return StatusUnreachable, "no answer from the brokers"
	}
	s := c.snap
	if s == nil {
		return StatusConnecting, "waiting for the first snapshot"
	}
	offline, urp := 0, 0
	for _, t := range s.Topics {
		for _, p := range t.Partitions {
			if p.Leader < 0 {
				offline++
			} else if p.UnderReplicated() {
				urp++
			}
		}
	}
	switch {
	case offline > 0:
		return StatusCritical, plural(offline, "partition") + " offline"
	case urp > 0:
		return StatusDegraded, plural(urp, "partition") + " under-replicated"
	}
	return StatusHealthy, "every partition fully replicated"
}

// RiskSums counts a plan's operations by risk.
type RiskSums struct {
	Safe        int `json:"safe"`
	Caution     int `json:"caution"`
	Destructive int `json:"destructive"`
}

func (c *Cluster) card() Card {
	k := Card{
		Name: c.Config.Name, Env: c.Config.Env, Region: c.Config.Region,
		GitOps: c.Config.Desired != "", HasRegistry: c.reg != nil,
		Error: c.lastErr, History: append([]Point{}, c.history...), Consumed: append([]Point{}, c.consumed...), Controller: -1,
		Groups: len(c.groups.Groups), Schemas: len(c.subjects), ACLs: len(c.acls), Freshness: -1,
	}
	k.Status, k.Reason = c.status()
	k.Uptime, k.Availability = uptime(c.health, time.Now())
	if n := len(c.consumed); n > 0 {
		k.ConsumeRate = c.consumed[n-1].V
	}
	for _, d := range c.disk {
		k.Disk += d
	}
	if s := c.snap; s != nil {
		k.ClusterID, k.Brokers, k.Controller, k.UpdatedAt = s.ClusterID, len(s.Brokers), s.Controller, s.TakenAt.UnixMilli()
		for _, t := range s.Topics {
			if t.Internal {
				continue
			}
			k.Topics++
			k.Partitions += len(t.Partitions)
			for _, p := range t.Partitions {
				if p.Leader < 0 {
					k.Offline++
				} else if p.UnderReplicated() {
					k.URP++
				}
				if p.EndOffset > 0 && p.StartOffset >= 0 {
					k.Records += p.EndOffset - p.StartOffset
				}
			}
			if r := c.rates[t.Name]; r != nil {
				k.Throughput += r.rate
			}
		}
	}
	var worst int64
	for _, g := range c.groups.Groups {
		if g.Lag > 0 {
			k.Lag += g.Lag
			if g.Lag > worst {
				worst, k.TopLag = g.Lag, g.Name
			}
		}
		if b := c.behind(g); b >= 0 && b >= k.Freshness {
			if b > k.Freshness || k.FreshGroup == "" {
				k.FreshGroup = g.Name
			}
			k.Freshness = b
		}
	}
	if k.Status != StatusUnreachable {
		k.Cost = c.fleet.pricing.monthly(k.Brokers, k.Disk)
	}
	if p := c.plan; p != nil {
		k.Drift = len(p.Ops)
		n := p.Counts()
		k.DriftRisk = RiskSums{Safe: n[plan.Safe], Caution: n[plan.Caution], Destructive: n[plan.Destructive]}
		k.Blocked = p.Blocked()
	}
	return k
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return strconv.Itoa(n) + " " + word + "s"
}

// FleetView is the fleet page.
type FleetView struct {
	Clusters []Card    `json:"clusters"`
	Totals   Totals    `json:"totals"`
	History  []Point   `json:"history"`  // records produced per second, every cluster
	Consumed []Point   `json:"consumed"` // records consumed per second, every cluster
	Events   []Event   `json:"events"`
	Insights []Insight `json:"insights"`
	Since    int64     `json:"since"` // when the console started watching
	Pricing  *Pricing  `json:"pricing,omitempty"`
}

// Totals add up the fleet.
type Totals struct {
	Clusters    int     `json:"clusters"`
	Healthy     int     `json:"healthy"`
	Brokers     int     `json:"brokers"`
	Topics      int     `json:"topics"`
	Partitions  int     `json:"partitions"`
	URP         int     `json:"urp"`
	Offline     int     `json:"offline"`
	Throughput  float64 `json:"throughput"`
	ConsumeRate float64 `json:"consumeRate"`
	Lag         int64   `json:"lag"`
	Drift       int     `json:"drift"`
	Schemas     int     `json:"schemas"`
	Disk        int64   `json:"disk"`
	Cost        float64 `json:"cost"`
	// Availability is the clusters' mean, -1 before any has a sample.
	Availability float64 `json:"availability"`
	// Freshness is the worst of the clusters', with where it is.
	Freshness    float64 `json:"freshness"`
	FreshCluster string  `json:"freshCluster,omitempty"`
	FreshGroup   string  `json:"freshGroup,omitempty"`
}

func (f *Fleet) view() FleetView {
	v := FleetView{Events: f.Events(40), Clusters: []Card{}, Insights: []Insight{}, Since: f.started.UnixMilli(), Pricing: f.pricing}
	v.Totals.Freshness, v.Totals.Availability = -1, -1
	in, out := map[int64]float64{}, map[int64]float64{}
	var avail float64
	measured := 0
	for _, c := range f.Clusters {
		c.mu.RLock()
		k := c.card()
		v.Insights = append(v.Insights, c.insights(k)...)
		c.mu.RUnlock()
		v.Clusters = append(v.Clusters, k)
		t := &v.Totals
		t.Clusters++
		if k.Status == StatusHealthy {
			t.Healthy++
		}
		t.Brokers += k.Brokers
		t.Topics += k.Topics
		t.Partitions += k.Partitions
		t.URP += k.URP
		t.Offline += k.Offline
		t.Throughput += k.Throughput
		t.Lag += k.Lag
		t.Drift += k.Drift
		t.Schemas += k.Schemas
		t.ConsumeRate += k.ConsumeRate
		t.Disk += k.Disk
		t.Cost += k.Cost
		if k.Availability >= 0 {
			avail += k.Availability
			measured++
		}
		if k.Freshness > t.Freshness {
			t.Freshness, t.FreshCluster, t.FreshGroup = k.Freshness, k.Name, k.FreshGroup
		}
		// Clusters sample at their own moments; bucket to add them up.
		for _, p := range k.History {
			in[p.T/5000*5000] += p.V
		}
		for _, p := range k.Consumed {
			out[p.T/10000*10000] += p.V
		}
	}
	if measured > 0 {
		v.Totals.Availability = avail / float64(measured)
	}
	v.History, v.Consumed = series(in), series(out)
	sortInsights(v.Insights)
	return v
}

// series sorts bucketed sums by time, dropping the newest bucket, which is
// still filling.
func series(byTime map[int64]float64) []Point {
	ps := []Point{}
	for t, val := range byTime {
		ps = append(ps, Point{T: t, V: val})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].T < ps[j].T })
	if n := len(ps); n > 1 {
		ps = ps[:n-1]
	}
	return ps
}

// ClusterView is a cluster's page.
type ClusterView struct {
	Card       Card        `json:"card"`
	Brokers    []BrokerRow `json:"brokers"`
	Topics     []TopicRow  `json:"topics"`
	Groups     []GroupRow  `json:"groups"`
	GroupsErr  string      `json:"groupsError,omitempty"`
	Events     []Event     `json:"events"`
	Insights   []Insight   `json:"insights"`
	Budget     BudgetView  `json:"budget"`
	SnapshotAt int64       `json:"snapshotAt"`
	Desired    string      `json:"desired,omitempty"`
}

// BrokerRow is one broker.
type BrokerRow struct {
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Rack       string `json:"rack,omitempty"`
	Controller bool   `json:"controller"`
	Online     bool   `json:"online"`
	Leaders    int    `json:"leaders"`
	Replicas   int    `json:"replicas"`
	Behind     int    `json:"behind"`
	Disk       int64  `json:"disk"`
}

// TopicRow is one topic.
type TopicRow struct {
	Name        string  `json:"name"`
	Internal    bool    `json:"internal"`
	Partitions  int     `json:"partitions"`
	Replication int     `json:"replication"`
	Records     int64   `json:"records"`
	Rate        float64 `json:"rate"`
	URP         int     `json:"urp"`
	Offline     int     `json:"offline"`
	Groups      int     `json:"groups"`
	OffsetsAge  int64   `json:"offsetsAge"` // milliseconds since its offsets were read; -1 never
}

// GroupRow is one consumer group.
type GroupRow struct {
	Name    string   `json:"name"`
	State   string   `json:"state"`
	Members int      `json:"members"`
	Topics  []string `json:"topics"`
	Lag     int64    `json:"lag"`
	Rate    float64  `json:"rate"`    // records committed per second
	Catchup float64  `json:"catchup"` // seconds to reach zero lag at that rate; -1 never, 0 caught up
	Behind  float64  `json:"behind"`  // how old the data it reads is, in seconds of its topics' traffic; -1 unknown
	Error   string   `json:"error,omitempty"`
}

// BudgetView is the engine's request budget, for the cluster footer.
type BudgetView struct {
	Tokens  float64 `json:"tokens"`
	Granted uint64  `json:"granted"`
	Denied  uint64  `json:"denied"`
}

func (c *Cluster) view() ClusterView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := ClusterView{
		Card: c.card(), GroupsErr: c.groupsErr, Desired: c.Config.Desired,
		Brokers: []BrokerRow{}, Topics: []TopicRow{}, Groups: []GroupRow{}, Events: []Event{},
	}
	v.Insights = c.insights(v.Card)
	sortInsights(v.Insights)
	st := c.eng.Stats().Budget
	v.Budget = BudgetView{Tokens: st.Tokens, Granted: st.Granted, Denied: st.Denied}
	readers := map[string]int{}
	for _, g := range c.groups.Groups {
		for _, t := range g.Topics {
			readers[t]++
		}
	}
	if s := c.snap; s != nil {
		v.SnapshotAt = s.TakenAt.UnixMilli()
		for _, b := range brokers.Summarize(s, c.disk) {
			v.Brokers = append(v.Brokers, BrokerRow{
				ID: b.ID, Host: hostPort(b.Host, b.Port), Rack: b.Rack, Controller: b.Controller, Online: b.Online,
				Leaders: b.Leaders, Replicas: b.Replicas, Behind: b.Behind, Disk: b.Disk,
			})
		}
		for _, t := range s.Topics {
			v.Topics = append(v.Topics, c.topicRow(t, readers[t.Name]))
		}
	}
	for _, g := range c.groups.Groups {
		v.Groups = append(v.Groups, c.groupRow(g))
	}
	sort.SliceStable(v.Groups, func(i, j int) bool { return v.Groups[i].Lag > v.Groups[j].Lag })
	for _, e := range c.fleet.Events(maxEvents) {
		if e.Cluster == c.Config.Name {
			v.Events = append(v.Events, e)
		}
	}
	if n := len(v.Events); n > 30 {
		v.Events = v.Events[n-30:]
	}
	return v
}

func hostPort(h string, p int32) string {
	if h == "" {
		return ""
	}
	return h + ":" + strconv.Itoa(int(p))
}

func (c *Cluster) topicRow(t snapshot.Topic, readers int) TopicRow {
	r := TopicRow{Name: t.Name, Internal: t.Internal, Partitions: len(t.Partitions), Groups: readers, OffsetsAge: -1}
	if len(t.Partitions) > 0 {
		r.Replication = len(t.Partitions[0].Replicas)
	}
	for _, p := range t.Partitions {
		if p.Leader < 0 {
			r.Offline++
		} else if p.UnderReplicated() {
			r.URP++
		}
		if p.EndOffset > 0 && p.StartOffset >= 0 {
			r.Records += p.EndOffset - p.StartOffset
		}
	}
	if rt := c.rates[t.Name]; rt != nil {
		r.Rate = rt.rate
	}
	if !t.OffsetsAt.IsZero() {
		r.OffsetsAge = time.Since(t.OffsetsAt).Milliseconds()
	}
	return r
}

func (c *Cluster) groupRow(g groups.Group) GroupRow {
	r := GroupRow{Name: g.Name, State: g.State, Members: g.Members, Topics: g.Topics, Lag: g.Lag, Catchup: 0}
	if g.Err != nil {
		r.Error = g.Err.Error()
	}
	if rate, ok := groups.Rate(c.prevGrp, c.groups, g.Name); ok {
		r.Rate = rate
	}
	switch {
	case g.Lag <= 0:
		r.Catchup = 0
	case r.Rate > 0:
		r.Catchup = float64(g.Lag) / r.Rate
	default:
		r.Catchup = -1
	}
	r.Behind = c.behind(g)
	if r.Topics == nil {
		r.Topics = []string{}
	}
	return r
}

// behind is how far behind a group is in time: its lag over the rate its
// topics are written at. 0 when caught up, -1 when it can't be told (no
// lag known, or nothing being written to divide by). The caller holds mu.
func (c *Cluster) behind(g groups.Group) float64 {
	if g.Lag == 0 {
		return 0
	}
	if g.Lag < 0 {
		return -1
	}
	var rate float64
	for _, t := range g.Topics {
		if r := c.rates[t]; r != nil && r.measured {
			rate += r.rate
		}
	}
	if rate <= 0 {
		return -1
	}
	return float64(g.Lag) / rate
}

// TopicView is one topic: its partitions and configs set on it.
type TopicView struct {
	Row        TopicRow        `json:"row"`
	Partitions []PartitionView `json:"partitions"`
	Brokers    []int32         `json:"brokers"`
	Readers    []GroupRow      `json:"readers"`
}

// PartitionView is one partition.
type PartitionView struct {
	ID       int32   `json:"id"`
	Leader   int32   `json:"leader"`
	Replicas []int32 `json:"replicas"`
	ISR      []int32 `json:"isr"`
	Start    int64   `json:"start"`
	End      int64   `json:"end"`
}

func (c *Cluster) topicView(name string) (TopicView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snap == nil {
		return TopicView{}, false
	}
	t, ok := c.snap.Topic(name)
	if !ok {
		return TopicView{}, false
	}
	v := TopicView{}
	for _, b := range c.snap.Brokers {
		v.Brokers = append(v.Brokers, b.ID)
	}
	for _, g := range c.groups.Groups {
		for _, gt := range g.Topics {
			if gt == name {
				row := c.groupRow(g)
				row.Lag = g.TopicLag[name]
				v.Readers = append(v.Readers, row)
			}
		}
	}
	v.Row = c.topicRow(t, len(v.Readers))
	for _, p := range t.Partitions {
		v.Partitions = append(v.Partitions, PartitionView{ID: p.ID, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, Start: p.StartOffset, End: p.EndOffset})
	}
	if v.Readers == nil {
		v.Readers = []GroupRow{}
	}
	return v, true
}

// AccessView is a cluster's ACLs and quotas.
type AccessView struct {
	ACLs      []acls.ACL `json:"acls"`
	ACLsErr   string     `json:"aclsError,omitempty"`
	Quotas    []QuotaRow `json:"quotas"`
	QuotasErr string     `json:"quotasError,omitempty"`
}

// QuotaRow is a quota with its entity spelled out.
type QuotaRow struct {
	Entity string             `json:"entity"`
	Values map[string]float64 `json:"values"`
}

func (c *Cluster) access() AccessView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := AccessView{ACLs: c.acls, ACLsErr: c.aclsErr, QuotasErr: c.quotasErr, Quotas: []QuotaRow{}}
	if v.ACLs == nil {
		v.ACLs = []acls.ACL{}
	}
	for _, q := range c.quotas {
		v.Quotas = append(v.Quotas, QuotaRow{Entity: q.Entity(), Values: q.Values})
	}
	return v
}

// SchemasView is a cluster's registry.
type SchemasView struct {
	Configured bool              `json:"configured"`
	Subjects   []schemas.Subject `json:"subjects"`
	Error      string            `json:"error,omitempty"`
}

func (c *Cluster) schemas() SchemasView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := SchemasView{Configured: c.reg != nil, Subjects: c.subjects, Error: c.schemaErr}
	if v.Subjects == nil {
		v.Subjects = []schemas.Subject{}
	}
	return v
}

// PlanView is a cluster's GitOps plan.
type PlanView struct {
	Configured bool       `json:"configured"`
	Desired    string     `json:"desired,omitempty"`
	Plan       *plan.Plan `json:"plan"`
	Error      string     `json:"error,omitempty"`
	Blocked    string     `json:"blocked,omitempty"`
	CanApply   bool       `json:"canApply"`
	Applying   bool       `json:"applying"`
}

func (c *Cluster) planView(allowApply bool) PlanView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := PlanView{Configured: c.Config.Desired != "", Desired: c.Config.Desired, Plan: c.plan, Error: c.planErr, Applying: c.applying}
	if c.plan != nil {
		v.Blocked = c.plan.Blocked()
		v.CanApply = allowApply && v.Blocked == "" && len(c.plan.Ops) > 0
	}
	return v
}
