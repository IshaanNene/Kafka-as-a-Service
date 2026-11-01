package console

import (
	"math"
	"testing"
	"time"
)

func TestUptime(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 30, 20, 0, time.UTC)
	slots, avail := uptime(nil, now)
	if len(slots) != uptimeSlots || avail != -1 {
		t.Fatalf("no samples: %d slots, availability %v", len(slots), avail)
	}
	if last := time.UnixMilli(slots[uptimeSlots-1].T).UTC(); !last.Equal(now.Truncate(time.Minute)) {
		t.Errorf("last slot starts %v, want this minute", last)
	}

	hs := []healthSample{
		{now.Add(-2 * time.Hour), StatusUnreachable}, // before the hour: ignored
		{now.Add(-90 * time.Second), StatusHealthy},
		{now.Add(-80 * time.Second), StatusCritical},
		{now.Add(-10 * time.Second), StatusHealthy},
		{now.Add(-5 * time.Second), StatusDegraded},
	}
	slots, avail = uptime(hs, now)
	if avail != 75 {
		t.Errorf("availability %v, want 75 (healthy and degraded count as up)", avail)
	}
	if got := slots[uptimeSlots-2].S; got != StatusCritical {
		t.Errorf("a minute with healthy and critical samples is %q, want the worse", got)
	}
	if got := slots[uptimeSlots-1].S; got != StatusDegraded {
		t.Errorf("this minute is %q, want degraded", got)
	}
	if got := slots[0].S; got != "" {
		t.Errorf("a minute with no samples is %q, want empty", got)
	}
}

func TestPricing(t *testing.T) {
	var none *Pricing
	if got := none.monthly(3, 1<<30); got != 0 {
		t.Errorf("no pricing costs %v", got)
	}
	p := &Pricing{BrokerHour: 0.2, GiBMonth: 0.1}
	if got := p.monthly(3, 10<<30); math.Abs(got-(3*0.2*730+1)) > 1e-9 {
		t.Errorf("3 brokers and 10 GiB cost %v", got)
	}
}

func TestSortInsights(t *testing.T) {
	is := []Insight{
		{ID: "b", Severity: SevInfo}, {ID: "z", Severity: SevWarning},
		{ID: "a", Severity: SevWarning}, {ID: "c", Severity: SevCritical},
	}
	sortInsights(is)
	got := ""
	for _, in := range is {
		got += in.ID
	}
	if got != "cazb" {
		t.Errorf("order %q, want critical, then warnings by id, then info", got)
	}
}

func TestFleetInsights(t *testing.T) {
	hs, _, _ := fleet(t, Options{})
	eventually(t, "the unreachable and drift insights", func() bool {
		v := get[FleetView](t, hs.URL+"/api/fleet")
		var gone, drift bool
		for _, in := range v.Insights {
			switch {
			case in.Cluster == "gone" && in.Kind == "unreachable":
				gone = in.Severity == SevCritical && in.Command != ""
			case in.Cluster == "dev" && in.Kind == "drift":
				drift = in.Href != ""
			}
		}
		return gone && drift && v.Insights[0].Severity == SevCritical
	})
}
