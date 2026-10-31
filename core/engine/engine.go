// Package engine is the cluster-state engine every KaaS surface shares. It
// keeps one immutable Snapshot per cluster fresh within a request budget,
// refreshes what screens are watching fast and everything else slowly, and
// turns consecutive snapshots into typed events.
//
// Kafka has no metadata watch API, so freshness is a polling schedule. The
// engine's job is to make that schedule cheap for the cluster: one loop owns
// every request, duplicate refresh requests coalesce, offsets are batched
// per leader by kadm, and when the budget runs dry data ages instead of the
// engine queuing more load.
package engine

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

// Config tunes the refresh schedule and the request budget. Zero fields
// take the defaults noted, which meet the Phase 1 freshness target of 5 s.
type Config struct {
	MetadataInterval  time.Duration // default 5s
	FastInterval      time.Duration // offsets of watched topics; default 1s; also the loop tick
	SlowInterval      time.Duration // offsets of every other topic; default 30s
	RequestsPerSecond float64       // default 20
	Burst             int           // default 40
	RequestTimeout    time.Duration // default 10s
}

func (c Config) withDefaults() Config {
	if c.MetadataInterval <= 0 {
		c.MetadataInterval = 5 * time.Second
	}
	if c.FastInterval <= 0 {
		c.FastInterval = time.Second
	}
	if c.SlowInterval <= 0 {
		c.SlowInterval = 30 * time.Second
	}
	if c.RequestsPerSecond <= 0 {
		c.RequestsPerSecond = 20
	}
	if c.Burst <= 0 {
		c.Burst = 40
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 10 * time.Second
	}
	return c
}

// Engine maintains the latest Snapshot of one cluster. Create it with New,
// start it with Run, and read it through Latest or a Subscription.
type Engine struct {
	cl     *kgo.Client
	adm    *kadm.Client
	cfg    Config
	budget *Budget
	kick   chan struct{} // capacity 1: concurrent Refresh calls coalesce

	mu        sync.Mutex
	latest    *snapshot.Snapshot
	gen       uint64
	lastMeta  time.Time
	lastErr   error
	watches   map[uint64][]string
	nextWatch uint64
	subs      map[*Subscription]struct{}
}

func New(cl *kgo.Client, cfg Config) *Engine {
	cfg = cfg.withDefaults()
	return &Engine{
		cl:      cl,
		adm:     kadm.NewClient(cl),
		cfg:     cfg,
		budget:  NewBudget(cfg.RequestsPerSecond, cfg.Burst),
		kick:    make(chan struct{}, 1),
		watches: make(map[uint64][]string),
		subs:    make(map[*Subscription]struct{}),
	}
}

// Config is the engine's configuration with defaults filled in.
func (e *Engine) Config() Config { return e.cfg }

// Run drives refreshes until ctx is done, then returns ctx.Err().
func (e *Engine) Run(ctx context.Context) error {
	t := time.NewTicker(e.cfg.FastInterval)
	defer t.Stop()
	e.step(ctx, true)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			e.step(ctx, false)
		case <-e.kick:
			e.step(ctx, true)
		}
	}
}

// Refresh asks for metadata and watched offsets now, still within budget.
// It never blocks; calls made while one is pending collapse into it.
func (e *Engine) Refresh() {
	select {
	case e.kick <- struct{}{}:
	default:
	}
}

// Watch marks topics as on screen, so their offsets refresh on the fast
// interval. The returned func removes the watch.
func (e *Engine) Watch(topics ...string) (cancel func()) {
	e.mu.Lock()
	id := e.nextWatch
	e.nextWatch++
	e.watches[id] = topics
	e.mu.Unlock()
	e.Refresh()
	return func() {
		e.mu.Lock()
		delete(e.watches, id)
		e.mu.Unlock()
	}
}

// Latest returns the newest published Snapshot, or nil before the first.
func (e *Engine) Latest() *snapshot.Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.latest
}

// Stats is a point-in-time view of engine health for the debug overlay.
type Stats struct {
	Generation uint64
	Budget     BudgetStats
	LastError  error // most recent refresh failure, cleared on success
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Stats{Generation: e.gen, Budget: e.budget.Stats(), LastError: e.lastErr}
}

// step runs one refresh pass. forced skips the interval checks for metadata
// and watched offsets, but never the budget.
func (e *Engine) step(ctx context.Context, forced bool) {
	now := time.Now()
	e.mu.Lock()
	prev := e.latest
	watched := e.watchedLocked()
	metaDue := forced || prev == nil || due(now, e.lastMeta, e.cfg.MetadataInterval)
	e.mu.Unlock()

	next := prev
	metaFresh := false
	if metaDue && e.budget.Take(1) {
		rctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
		s, err := snapshot.Fetch(rctx, e.cl)
		cancel()
		if err != nil {
			e.setErr(err)
		} else {
			carryOffsets(prev, s)
			next, metaFresh = s, true
			e.mu.Lock()
			e.lastMeta = now
			e.mu.Unlock()
		}
	}
	if next == nil {
		return
	}

	offsetsFresh := false
	if topics := e.offsetsDue(next, watched, now, forced); len(topics) > 0 &&
		e.budget.Take(2*leaderCount(next, topics)) {
		rctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
		starts, err := e.adm.ListStartOffsets(rctx, topics...)
		var ends kadm.ListedOffsets
		if err == nil {
			ends, err = e.adm.ListEndOffsets(rctx, topics...)
		}
		cancel()
		if err != nil {
			e.setErr(err)
		} else {
			next = snapshot.WithOffsets(next, topics, starts, ends, now)
			offsetsFresh = true
		}
	}

	if !metaFresh && !offsetsFresh {
		return
	}
	var evs []Event
	if metaFresh {
		evs = Diff(prev, next)
	}
	e.publish(next, evs)
}

func (e *Engine) offsetsDue(s *snapshot.Snapshot, watched map[string]bool, now time.Time, forced bool) []string {
	var out []string
	for i := range s.Topics {
		t := &s.Topics[i]
		switch {
		case watched[t.Name] && (forced || due(now, t.OffsetsAt, e.cfg.FastInterval)):
			out = append(out, t.Name)
		case due(now, t.OffsetsAt, e.cfg.SlowInterval):
			out = append(out, t.Name)
		}
	}
	return out
}

// due reports whether last is at least interval old, with 10% slack so a
// refresh that lands just before the tick is not skipped for a whole tick.
func due(now, last time.Time, interval time.Duration) bool {
	return last.IsZero() || now.Sub(last) >= interval*9/10
}

func (e *Engine) watchedLocked() map[string]bool {
	w := make(map[string]bool)
	for _, ts := range e.watches {
		for _, t := range ts {
			w[t] = true
		}
	}
	return w
}

func (e *Engine) setErr(err error) {
	e.mu.Lock()
	e.lastErr = err
	e.mu.Unlock()
}

func (e *Engine) publish(s *snapshot.Snapshot, evs []Event) {
	e.mu.Lock()
	e.gen++
	s.Generation = e.gen
	e.latest = s
	e.lastErr = nil
	subs := make([]*Subscription, 0, len(e.subs))
	for sub := range e.subs {
		subs = append(subs, sub)
	}
	e.mu.Unlock()
	u := Update{Snapshot: s, Events: evs}
	for _, sub := range subs {
		sub.deliver(u)
	}
}

// carryOffsets copies offsets from prev into a freshly fetched snapshot,
// which is not yet published and so still safe to mutate.
func carryOffsets(prev, next *snapshot.Snapshot) {
	if prev == nil {
		return
	}
	for i := range next.Topics {
		nt := &next.Topics[i]
		pt, ok := prev.Topic(nt.Name)
		if !ok {
			continue
		}
		nt.OffsetsAt = pt.OffsetsAt
		for k := range min(len(pt.Partitions), len(nt.Partitions)) {
			nt.Partitions[k].StartOffset = pt.Partitions[k].StartOffset
			nt.Partitions[k].EndOffset = pt.Partitions[k].EndOffset
		}
	}
}

// leaderCount estimates the ListOffsets requests for topics: kadm sends one
// per partition leader, so cost scales with distinct leaders, not partitions.
func leaderCount(s *snapshot.Snapshot, topics []string) int {
	leaders := make(map[int32]bool)
	for _, name := range topics {
		t, _ := s.Topic(name)
		for _, p := range t.Partitions {
			leaders[p.Leader] = true
		}
	}
	return max(1, len(leaders))
}

// Update is what a subscriber receives: the newest snapshot plus every event
// since its last read.
type Update struct {
	Snapshot      *snapshot.Snapshot
	Events        []Event
	DroppedEvents int // events discarded because the subscriber fell behind
}

// maxPendingEvents bounds a slow subscriber's backlog.
const maxPendingEvents = 4096

// Subscription is a single-slot mailbox. Intermediate snapshots are dropped
// (renderers only want the newest) while events accumulate up to a bound,
// so a slow reader never blocks the engine.
type Subscription struct {
	e       *Engine
	ready   chan struct{}
	mu      sync.Mutex
	pending Update
	has     bool
}

// Subscribe returns a Subscription primed with the latest snapshot, if any.
func (e *Engine) Subscribe() *Subscription {
	s := &Subscription{e: e, ready: make(chan struct{}, 1)}
	e.mu.Lock()
	e.subs[s] = struct{}{}
	latest := e.latest
	e.mu.Unlock()
	if latest != nil {
		s.deliver(Update{Snapshot: latest})
	}
	return s
}

// Ready is signalled when Next has something new.
func (s *Subscription) Ready() <-chan struct{} { return s.ready }

// Next returns and clears the pending update.
func (s *Subscription) Next() (Update, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.pending, s.has
	s.pending, s.has = Update{}, false
	return u, ok
}

func (s *Subscription) Close() {
	s.e.mu.Lock()
	delete(s.e.subs, s)
	s.e.mu.Unlock()
}

func (s *Subscription) deliver(u Update) {
	s.mu.Lock()
	s.pending.Snapshot = u.Snapshot
	room := maxPendingEvents - len(s.pending.Events)
	if len(u.Events) > room {
		s.pending.DroppedEvents += len(u.Events) - room
		u.Events = u.Events[:room]
	}
	s.pending.Events = append(s.pending.Events, u.Events...)
	s.pending.DroppedEvents += u.DroppedEvents
	s.has = true
	s.mu.Unlock()
	select {
	case s.ready <- struct{}{}:
	default:
	}
}
