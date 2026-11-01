package sniff

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// Frame is one node of a latency profile: all, an API, a broker, a topic,
// or a stage. Its Time is the time every request under it spent there.
type Frame struct {
	Name   string
	Time   time.Duration // this frame and everything under it
	Self   time.Duration // in this frame itself; only stages have any
	N      int           // requests through this frame
	Kids   []*Frame
	Parent *Frame

	kids    map[string]*Frame
	samples []time.Duration // a uniform sample of the per-request times here
	seen    int
}

// sampleCap bounds the per-request times each frame keeps for quantiles.
const sampleCap = 1024

func newFrame(name string, parent *Frame) *Frame {
	return &Frame{Name: name, Parent: parent, kids: map[string]*Frame{}}
}

// kid is the child called name, made on first use.
func (f *Frame) kid(name string) *Frame {
	k, ok := f.kids[name]
	if !ok {
		k = newFrame(name, f)
		f.kids[name] = k
		f.Kids = append(f.Kids, k)
	}
	return k
}

// note counts one request's d in this frame.
func (f *Frame) note(d time.Duration, rng *rand.Rand) {
	f.Time += d
	f.N++
	f.seen++
	if len(f.samples) < sampleCap {
		f.samples = append(f.samples, d)
	} else if i := rng.IntN(f.seen); i < sampleCap {
		f.samples[i] = d // reservoir sampling: every request equally likely to stay
	}
}

// Avg is the mean per-request time in the frame.
func (f *Frame) Avg() time.Duration {
	if f.N == 0 {
		return 0
	}
	return f.Time / time.Duration(f.N)
}

// Quantile is the per-request time below which q of requests fall, from
// a sample of up to 1,024.
func (f *Frame) Quantile(q float64) time.Duration {
	if len(f.samples) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), f.samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(q*float64(len(s))))]
}

// Sorted is the children, the most time first.
func (f *Frame) Sorted() []*Frame {
	ks := append([]*Frame(nil), f.Kids...)
	sort.SliceStable(ks, func(i, j int) bool {
		if ks[i].Time != ks[j].Time {
			return ks[i].Time > ks[j].Time
		}
		return ks[i].Name < ks[j].Name
	})
	return ks
}

// Path is the frame's ancestors' names and its own, below all.
func (f *Frame) Path() []string {
	var p []string
	for g := f; g != nil && g.Parent != nil; g = g.Parent {
		p = append([]string{g.Name}, p...)
	}
	return p
}

// Profile adds requests' stages into a tree: API, broker, topic, stage.
type Profile struct {
	Root     *Frame
	Requests int
	rng      *rand.Rand
}

// The stage names in a profile. A fetch's wait can be polling rather than
// serving: all of it when the answer has no records, and the part before
// its first record was written when the records came later.
const (
	StageConnect = "connect"
	StageSend    = "send"
	StageQueue   = "queue"
	StageWait    = "wait"
	StageIdle    = "idle poll" // a fetch that waited and got nothing
	StagePoll    = "long poll" // a fetch waiting for records yet to be written
	StageAck     = "ack"
)

func NewProfile() *Profile {
	return &Profile{Root: newFrame("all", nil), rng: rand.New(rand.NewPCG(1, 2))}
}

// topicLabel names a request's topics in one frame.
func topicLabel(ts []string) string {
	switch len(ts) {
	case 0:
		return ""
	case 1:
		return ts[0]
	}
	return fmt.Sprintf("%s +%d", ts[0], len(ts)-1)
}

// Add counts one answered request.
func (p *Profile) Add(r Request) {
	st := r.Stages
	if st.Total() <= 0 {
		return
	}
	p.Requests++
	wait, poll := StageWait, time.Duration(0)
	switch {
	case r.Key == 1 && r.Records == 0:
		wait = StageIdle
	case r.Key == 1 && r.Records > 0 && !r.FirstRecord.IsZero():
		// Record timestamps are milliseconds: only a whole millisecond
		// after the fetch arrived counts as waiting for it.
		arrived := r.Start.Add(st.Connect + st.Send)
		if gap := r.FirstRecord.Sub(arrived); gap >= time.Millisecond {
			poll = min(gap, st.Wait)
		}
	}
	path := []*Frame{p.Root}
	f := p.Root.kid(r.Name())
	path = append(path, f)
	f = f.kid(fmt.Sprintf("broker %d", r.Broker))
	path = append(path, f)
	if t := topicLabel(r.Topics); t != "" {
		f = f.kid(t)
		path = append(path, f)
	}
	for _, g := range path {
		g.note(st.Total(), p.rng)
	}
	for _, s := range []struct {
		name string
		d    time.Duration
	}{{StageConnect, st.Connect}, {StageSend, st.Send}, {StageQueue, st.Queue}, {StagePoll, poll}, {wait, st.Wait - poll}, {StageAck, st.Ack}} {
		if s.d > 0 {
			leaf := f.kid(s.name)
			leaf.note(s.d, p.rng)
			leaf.Self += s.d
		}
	}
}

// Folded writes the profile as folded stacks, one leaf a line with its
// time in microseconds, as flamegraph.pl and speedscope read them.
func (p *Profile) Folded() string {
	var sb strings.Builder
	var walk func(f *Frame)
	walk = func(f *Frame) {
		if f.Self > 0 {
			fmt.Fprintf(&sb, "%s %d\n", strings.Join(f.Path(), ";"), f.Self.Microseconds())
		}
		for _, k := range f.Sorted() {
			walk(k)
		}
	}
	walk(p.Root)
	return sb.String()
}
