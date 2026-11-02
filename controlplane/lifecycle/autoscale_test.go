package lifecycle

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

var policy = Autoscale{Min: 3, Max: 9, Target: 10_000, ScaleDownAfter: Duration{10 * time.Minute}}

func TestRecommend(t *testing.T) {
	for _, c := range []struct {
		load float64
		want int
	}{
		{0, 3}, {29_000, 3}, {30_001, 4}, {61_000, 7}, {1e6, 9},
	} {
		if got, why := Recommend(policy, c.load); got != c.want || why == "" {
			t.Errorf("Recommend(%v) = %d %q, want %d", c.load, got, why, c.want)
		}
	}
}

func TestDecide(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	at := func(ago time.Duration, n int) Mark { return Mark{now.Add(-ago), n} }
	for _, c := range []struct {
		name    string
		current int
		marks   []Mark
		want    int
	}{
		{"unmeasured", 5, nil, 5},
		{"up at once", 4, []Mark{at(time.Second, 7)}, 7},
		{"steady", 5, []Mark{at(20*time.Minute, 5), at(0, 5)}, 5},
		{"down needs a full window", 6, []Mark{at(5*time.Minute, 3), at(0, 3)}, 6},
		{"down after the window", 6, []Mark{at(11*time.Minute, 3), at(5*time.Minute, 3), at(0, 3)}, 3},
		{"down only to the window's peak", 8, []Mark{at(11*time.Minute, 3), at(4*time.Minute, 5), at(0, 3)}, 5},
		{"a burst inside the window holds", 6, []Mark{at(11*time.Minute, 3), at(2*time.Minute, 6), at(0, 3)}, 6},
	} {
		if got, why := Decide(policy, c.current, c.marks, now); got != c.want || why == "" {
			t.Errorf("%s: Decide = %d %q, want %d", c.name, got, why, c.want)
		}
	}
}

// Decide never leaves the policy's bounds (given in-bounds input), never
// sheds brokers while any recent mark needed them, and never lags a
// rising recommendation.
func TestDecideProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
		current := rapid.IntRange(policy.Min, policy.Max).Draw(t, "current")
		n := rapid.IntRange(1, 30).Draw(t, "n")
		marks := make([]Mark, n)
		for i := range marks {
			ago := time.Duration(n-i) * time.Duration(rapid.IntRange(1, 90).Draw(t, "gap")) * time.Second
			marks[i] = Mark{now.Add(-ago), rapid.IntRange(policy.Min, policy.Max).Draw(t, "brokers")}
		}
		marks[n-1].At = now
		got, _ := Decide(policy, current, marks, now)
		if got < policy.Min || got > policy.Max {
			t.Fatalf("decided %d, outside %d..%d", got, policy.Min, policy.Max)
		}
		latest := marks[n-1].Brokers
		if latest > current && got != latest {
			t.Fatalf("load wants %d over %d running, decided %d", latest, current, got)
		}
		if got < current {
			for _, m := range marks {
				if !m.At.Before(now.Add(-policy.ScaleDownAfter.Duration)) && m.Brokers > got {
					t.Fatalf("shed to %d while a mark at %v wanted %d", got, m.At, m.Brokers)
				}
			}
		}
	})
}

func TestTrim(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	marks := []Mark{{now.Add(-30 * time.Minute), 3}, {now.Add(-12 * time.Minute), 4}, {now.Add(-5 * time.Minute), 5}, {now, 5}}
	got := Trim(marks, 10*time.Minute, now)
	if len(got) != 3 || got[0].Brokers != 4 {
		t.Errorf("Trim kept %v, want from the last mark before the window", got)
	}
}

func TestValidate(t *testing.T) {
	ok := Spec{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Spec{
		{Name: "Orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"},
		{Name: "orders", Version: "latest", Brokers: 3, Storage: "100Gi"},
		{Name: "orders", Version: "4.2.0", Brokers: 0, Storage: "100Gi"},
		{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "lots"},
		{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi", Autoscale: &Autoscale{Min: 5, Max: 3, Target: 1}},
	} {
		if bad.Validate() == nil {
			t.Errorf("%+v passed validation", bad)
		}
	}
}
