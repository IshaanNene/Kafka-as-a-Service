package sniff

import (
	"strings"
	"testing"
	"time"
)

func TestProfile(t *testing.T) {
	p := NewProfile()
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	for i := range 10 {
		p.Add(Request{Key: 0, Broker: 1, Topics: []string{"orders"}, Records: -1,
			Stages: Stages{Send: ms(1), Queue: ms(i % 2), Wait: ms(3 + i), Ack: ms(1)}})
	}
	p.Add(Request{Key: 1, Broker: 2, Topics: []string{"orders", "payments"}, Records: 0, Stages: Stages{Send: ms(1), Wait: ms(500)}})
	p.Add(Request{Key: 1, Broker: 2, Topics: []string{"orders", "payments"}, Records: 40, Stages: Stages{Send: ms(1), Wait: ms(20), Ack: ms(2)}})
	// Caught up: the record it returns was written 40ms after the fetch arrived.
	t0 := time.Now()
	p.Add(Request{Key: 1, Broker: 2, Topics: []string{"orders"}, Records: 1, Start: t0, FirstRecord: t0.Add(ms(41)),
		Stages: Stages{Send: ms(1), Wait: ms(45), Ack: ms(1)}})
	// Written before it arrived: all of its wait is serving.
	p.Add(Request{Key: 1, Broker: 2, Topics: []string{"orders"}, Records: 1, Start: t0, FirstRecord: t0.Add(-time.Second),
		Stages: Stages{Send: ms(1), Wait: ms(4)}})
	p.Add(Request{Key: 18, Broker: 0, Stages: Stages{Connect: ms(2), Wait: ms(1)}})
	p.Add(Request{Key: 3}) // no time: not counted

	caught := p.Root.kids["Fetch"].kids["broker 2"].kids["orders"]
	if caught.kids[StagePoll].Time != ms(40) || caught.kids[StageWait].Time != ms(5+4) {
		t.Errorf("caught up: poll %v, wait %v", caught.kids[StagePoll].Time, caught.kids[StageWait].Time)
	}
	if p.Requests != 15 {
		t.Errorf("%d requests", p.Requests)
	}
	root := p.Root
	var self time.Duration
	var walk func(f *Frame)
	walk = func(f *Frame) {
		var kids time.Duration
		for _, k := range f.Kids {
			kids += k.Time
			walk(k)
		}
		self += f.Self
		if f.Parent != nil && len(f.Kids) > 0 && kids != f.Time {
			t.Errorf("%v: kids %v of %v", f.Path(), kids, f.Time)
		}
	}
	walk(root)
	if self != root.Time {
		t.Errorf("self %v, all %v", self, root.Time)
	}
	top := root.Sorted()
	if top[0].Name != "Fetch" || top[1].Name != "Produce" {
		t.Errorf("order %v %v", top[0].Name, top[1].Name)
	}
	fetch := top[0].Kids[0].Kids[0]
	if fetch.Name != "orders +1" || fetch.kids[StageIdle] == nil || fetch.kids[StageIdle].Time != ms(500) || fetch.kids[StageWait].Time != ms(20) {
		t.Errorf("fetch frames: %+v", fetch.Kids)
	}
	wait := root.kids["Produce"].kids["broker 1"].kids["orders"].kids[StageWait]
	if wait.N != 10 || wait.Avg() != ms(75)/10 || wait.Quantile(0.99) != ms(12) || wait.Quantile(0) != ms(3) {
		t.Errorf("wait n %d avg %v p99 %v", wait.N, wait.Avg(), wait.Quantile(0.99))
	}
	if got := strings.Join(wait.Path(), ";"); got != "Produce;broker 1;orders;wait" {
		t.Errorf("path %q", got)
	}
	// No topic: stages hang off the broker.
	if root.kids["ApiVersions"].kids["broker 0"].kids[StageConnect] == nil {
		t.Error("ApiVersions has no connect stage under its broker")
	}
	folded := p.Folded()
	for _, want := range []string{"Fetch;broker 2;orders +1;idle poll 500000\n", "Produce;broker 1;orders;wait 75000\n", "ApiVersions;broker 0;connect 2000\n"} {
		if !strings.Contains(folded, want) {
			t.Errorf("folded lacks %q:\n%s", want, folded)
		}
	}
	// The sample stays bounded.
	for range 3 * sampleCap {
		p.Add(Request{Key: 0, Broker: 1, Topics: []string{"orders"}, Stages: Stages{Wait: ms(1)}})
	}
	if n := len(wait.samples); n != sampleCap || wait.N != 10+3*sampleCap {
		t.Errorf("%d samples of %d", n, wait.N)
	}
}
