package lifecycle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Demo is a Manager whose clusters exist only in memory. It acts out
// what the operator and Strimzi do, compressed into seconds: brokers
// start one after another, rolls restart one broker at a time, adding or
// removing brokers moves partitions first, and an autoscaled cluster
// follows a load that rises and falls every few minutes.
type Demo struct {
	mu   sync.Mutex
	svcs map[string]*demoSvc
	// Speed scales the simulated durations; 1 is the console demo's pace.
	speed float64
	// Now is the clock changes are stamped with; tests set it.
	Now func() time.Time
}

type demoSvc struct {
	Service
	nodes  []*demoNode
	marks  []Mark
	load   func(time.Time) float64
	next   time.Time // next load sample
	moving time.Time // partitions are moving until then
	roll   *demoRoll
	// pending is the broker count to change to once partitions have
	// moved off the brokers going away.
	pending int
}

type demoNode struct {
	id    int
	state string
	until time.Time // when the current state ends
}

type demoRoll struct {
	next  int // index into nodes of the broker restarting or next to
	total int
	start time.Time
}

const demoSample = 5 * time.Second

// NewDemo is a demo with four clusters at different points in their
// lives, as of now.
func NewDemo(now time.Time) *Demo {
	d := &Demo{svcs: map[string]*demoSvc{}, speed: 1, Now: time.Now}
	wave := func(base, amp float64, period time.Duration) func(time.Time) float64 {
		return func(t time.Time) float64 {
			x := 2 * math.Pi * float64(t.UnixMilli()%period.Milliseconds()) / float64(period.Milliseconds())
			jitter := math.Sin(float64(t.UnixMilli())/7000) * amp * 0.08
			return math.Max(0, base+amp*math.Sin(x)+jitter)
		}
	}
	add := func(s Spec, created time.Duration, load func(time.Time) float64, history ...Event) *demoSvc {
		v := &demoSvc{Service: Service{
			Name: s.Name, Namespace: "kafka", Version: s.Version, Storage: s.Storage, Auto: s.Autoscale,
			Bootstrap: s.Name + "-kafka-bootstrap.kafka.svc:9092", Created: now.Add(-created),
			Phase: Ready, Brokers: Brokers{Desired: s.Brokers}, History: history,
		}, load: load, next: now}
		for i := range s.Brokers {
			v.nodes = append(v.nodes, &demoNode{id: i, state: "running"})
		}
		d.svcs[s.Name] = v
		// An hour of past load, so charts aren't empty on arrival.
		for t := now.Add(-time.Hour); t.Before(now); t = t.Add(30 * time.Second) {
			v.Load = append(v.Load, Sample{T: t.UnixMilli(), V: math.Round(load(t))})
		}
		return v
	}
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	add(Spec{Name: "payments", Version: "4.2.0", Brokers: 6, Storage: "500Gi",
		Autoscale: &Autoscale{Min: 3, Max: 9, Target: 2000, ScaleDownAfter: Duration{2 * time.Minute}}},
		90*24*time.Hour, wave(9000, 5500, 6*time.Minute),
		Event{ago(14 * time.Minute), "autoscaled", "5 → 6 brokers: load needs 6 brokers"},
		Event{ago(3 * 24 * time.Hour), "rolled", "every pod restarted in 6m12s"},
		Event{ago(90 * 24 * time.Hour), "created", "serving on 3 brokers"})
	add(Spec{Name: "search", Version: "4.2.0", Brokers: 3, Storage: "200Gi"},
		40*24*time.Hour, wave(1500, 300, 9*time.Minute),
		Event{ago(12 * 24 * time.Hour), "scaled", "5 → 3 brokers, by hand"},
		Event{ago(40 * 24 * time.Hour), "created", "serving on 5 brokers"})
	an := add(Spec{Name: "analytics", Version: "4.1.1", Brokers: 4, Storage: "1Ti"},
		200*24*time.Hour, wave(4200, 900, 11*time.Minute),
		Event{ago(200 * 24 * time.Hour), "created", "serving on 4 brokers"})
	d.startRoll(an, now)
	st := add(Spec{Name: "edge-staging", Version: "4.2.0", Brokers: 3, Storage: "50Gi"}, 0, wave(220, 60, 4*time.Minute))
	st.Load = nil
	d.provision(st, now)
	return d
}

// Run steps the demo every second until ctx ends.
func (d *Demo) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			d.Step(now)
		}
	}
}

func (d *Demo) after(now time.Time, secs float64) time.Time {
	return now.Add(time.Duration(secs * d.speed * float64(time.Second)))
}

// Where is "demo".
func (d *Demo) Where() string { return "demo · namespace kafka" }

// Services lists the clusters.
func (d *Demo) Services(context.Context) ([]Service, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Service, 0, len(d.svcs))
	for _, v := range d.svcs {
		out = append(out, v.view())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (v *demoSvc) view() Service {
	s := v.Service
	// Lists are never null in the API.
	s.Load = append([]Sample{}, v.Load...)
	s.History = append([]Event{}, v.History...)
	if v.Auto != nil {
		a := *v.Auto
		s.Auto = &a
	}
	s.Brokers.Nodes = make([]Node, 0, len(v.nodes))
	for _, n := range v.nodes {
		s.Brokers.Nodes = append(s.Brokers.Nodes, Node{ID: n.id, Ready: n.state == "running" || n.state == "queued", State: n.state})
		if n.state == "running" || n.state == "queued" {
			s.Brokers.Ready++
		}
	}
	if v.roll != nil {
		s.Roll = &Progress{Done: v.roll.next, Total: v.roll.total}
	}
	return s
}

// Create starts provisioning a new cluster.
func (d *Demo) Create(_ context.Context, s Spec) error {
	if err := s.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.svcs[s.Name]; ok {
		return ErrExists
	}
	now := d.Now()
	n := s.Brokers
	if s.Autoscale != nil {
		n = min(max(n, s.Autoscale.Min), s.Autoscale.Max)
	}
	v := &demoSvc{Service: Service{
		Name: s.Name, Namespace: "kafka", Version: s.Version, Storage: s.Storage, Auto: s.Autoscale,
		Bootstrap: s.Name + "-kafka-bootstrap.kafka.svc:9092", Created: now, Brokers: Brokers{Desired: n},
	}, next: now}
	// New clusters see a little traffic once they're up.
	period := 5 * time.Minute
	v.load = func(t time.Time) float64 {
		x := 2 * math.Pi * float64(t.UnixMilli()%period.Milliseconds()) / float64(period.Milliseconds())
		return math.Max(0, 600+400*math.Sin(x))
	}
	for i := range n {
		v.nodes = append(v.nodes, &demoNode{id: i})
	}
	d.svcs[s.Name] = v
	d.provision(v, now)
	return nil
}

// provision brings v's brokers up one after another.
func (d *Demo) provision(v *demoSvc, now time.Time) {
	v.Phase = Provisioning
	for i, n := range v.nodes {
		n.state, n.until = "pending", d.after(now, 3+2.5*float64(i))
	}
}

// Roll restarts every broker in turn.
func (d *Demo) Roll(_ context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.svcs[name]
	switch {
	case !ok:
		return ErrNotFound
	case v.roll != nil:
		return fmt.Errorf("%w: already restarting", ErrBusy)
	case v.Phase != Ready && v.Phase != Degraded:
		return fmt.Errorf("%w: %s", ErrBusy, v.Phase)
	}
	d.startRoll(v, d.Now())
	return nil
}

func (d *Demo) startRoll(v *demoSvc, now time.Time) {
	v.roll = &demoRoll{total: len(v.nodes), start: now}
	for _, n := range v.nodes {
		n.state = "queued"
	}
	v.event(now, "rolling", fmt.Sprintf("restarting %d brokers, one at a time", len(v.nodes)))
	v.Phase = Rolling
}

// Scale sets the broker count by hand.
func (d *Demo) Scale(_ context.Context, name string, brokers int) error {
	if err := ValidateScale(brokers); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.svcs[name]
	if !ok {
		return ErrNotFound
	}
	if v.Phase == Provisioning || v.Phase == Rolling || v.Phase == Scaling {
		return fmt.Errorf("%w: %s", ErrBusy, v.Phase)
	}
	v.Auto, v.marks, v.Recommended = nil, nil, 0
	now := d.Now()
	if brokers != len(v.nodes) {
		v.event(now, "scaled", fmt.Sprintf("%d → %d brokers, by hand", len(v.nodes), brokers))
		d.resize(v, brokers, now)
	}
	return nil
}

// SetAutoscale turns autoscaling on or off.
func (d *Demo) SetAutoscale(_ context.Context, name string, p *Autoscale) error {
	if p != nil {
		if err := p.Validate(); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.svcs[name]
	if !ok {
		return ErrNotFound
	}
	if p == nil {
		v.Auto, v.marks, v.Recommended, v.Reason = nil, nil, 0, ""
		return nil
	}
	a := *p
	v.Auto, v.marks = &a, nil
	return nil
}

// resize starts adding or removing brokers.
func (d *Demo) resize(v *demoSvc, n int, now time.Time) {
	v.Brokers.Desired = n
	v.Phase = Scaling
	if n > len(v.nodes) {
		next := 0
		for _, x := range v.nodes {
			next = max(next, x.id+1)
		}
		add := n - len(v.nodes)
		for i := range add {
			v.nodes = append(v.nodes, &demoNode{id: next + i, state: "pending", until: d.after(now, 2+2*float64(i))})
		}
		v.Message = fmt.Sprintf("starting %d brokers", add)
		v.pending, v.moving = 0, time.Time{}
		return
	}
	// Partitions come off the brokers going away before they stop.
	v.pending = n
	v.moving = d.after(now, 8)
	v.Message = fmt.Sprintf("moving partitions off %d brokers", len(v.nodes)-n)
}

func (v *demoSvc) event(now time.Time, kind, detail string) {
	v.History = append([]Event{{At: now, Kind: kind, Detail: detail}}, v.History...)
	if len(v.History) > 20 {
		v.History = v.History[:20]
	}
}

// Step moves every cluster on to now.
func (d *Demo) Step(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range d.svcs {
		d.step(v, now)
	}
}

func (d *Demo) step(v *demoSvc, now time.Time) {
	// Brokers starting or stopping.
	kept := v.nodes[:0]
	for _, n := range v.nodes {
		if !n.until.IsZero() && !now.Before(n.until) {
			switch n.state {
			case "pending":
				n.state, n.until = "starting", d.after(now, 2.5)
			case "starting", "restarting":
				n.state, n.until = "running", time.Time{}
			case "stopping":
				continue
			}
		}
		kept = append(kept, n)
	}
	v.nodes = kept
	up := 0
	for _, n := range v.nodes {
		if n.state == "running" || n.state == "queued" {
			up++
		}
	}

	// A roll: one broker at a time, the next once the last is back.
	if r := v.roll; r != nil {
		if v.nodes[r.next].state == "running" {
			r.next++
		}
		if r.next >= r.total {
			v.event(now, "rolled", fmt.Sprintf("every broker restarted in %s", now.Sub(r.start).Round(time.Second)))
			v.roll = nil
		} else if n := v.nodes[r.next]; n.state == "queued" {
			n.state, n.until = "restarting", d.after(now, 4)
		}
	}

	switch {
	case v.Phase == Provisioning:
		if up == len(v.nodes) {
			v.Phase, v.Message = Ready, ""
			v.event(now, "created", fmt.Sprintf("serving on %d brokers", up))
		} else {
			v.Message = fmt.Sprintf("%d of %d brokers up", up, len(v.nodes))
		}
	case v.roll != nil:
		v.Phase, v.Message = Rolling, fmt.Sprintf("%d of %d brokers restarted", v.roll.next, v.roll.total)
	case v.pending > 0:
		if !now.Before(v.moving) {
			for _, n := range v.nodes[v.pending:] {
				if n.state != "stopping" {
					n.state, n.until = "stopping", d.after(now, 3)
				}
			}
			v.Message = "stopping brokers"
			if len(v.nodes) == v.pending {
				v.pending, v.moving = 0, time.Time{}
				v.Phase, v.Message = Ready, ""
			}
		}
	case v.Phase == Scaling:
		// Brokers up, then partitions move on to them.
		if up == len(v.nodes) {
			if v.moving.IsZero() {
				v.moving = d.after(now, 6)
				v.Message = "moving partitions on to the new brokers"
			} else if !now.Before(v.moving) {
				v.Phase, v.Message, v.moving = Ready, "", time.Time{}
			}
		} else {
			v.Message = fmt.Sprintf("%d of %d brokers up", up, len(v.nodes))
		}
	case v.Phase == Rolling:
		v.Phase, v.Message = Ready, ""
	}

	// Load, and autoscaling on it.
	if v.Phase == Provisioning || now.Before(v.next) {
		return
	}
	v.next = now.Add(demoSample)
	l := math.Round(v.load(now))
	v.Load = append(v.Load, Sample{T: now.UnixMilli(), V: l})
	if len(v.Load) > 240 {
		v.Load = v.Load[len(v.Load)-240:]
	}
	if v.Auto == nil {
		return
	}
	rec, why := Recommend(*v.Auto, l)
	v.Recommended, v.Reason = rec, why
	v.marks = Trim(append(v.marks, Mark{At: now, Brokers: rec}), v.Auto.ScaleDownAfter.Duration, now)
	if v.Phase != Ready {
		return
	}
	cur := len(v.nodes)
	if clamped := min(max(cur, v.Auto.Min), v.Auto.Max); clamped != cur {
		v.event(now, "scaled", fmt.Sprintf("%d → %d brokers, to fit the autoscaling bounds", cur, clamped))
		d.resize(v, clamped, now)
		return
	}
	n, why := Decide(*v.Auto, cur, v.marks, now)
	v.Reason = why
	if n != cur {
		v.event(now, "autoscaled", fmt.Sprintf("%d → %d brokers: %s", cur, n, why))
		d.resize(v, n, now)
	}
}
