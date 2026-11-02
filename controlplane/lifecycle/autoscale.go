package lifecycle

import (
	"fmt"
	"math"
	"time"
)

// Recommend is the broker count for load: enough that none takes more
// than a.Target records a second, within a.Min and a.Max.
func Recommend(a Autoscale, load float64) (int, string) {
	n := int(math.Ceil(load / float64(a.Target)))
	switch {
	case n > a.Max:
		return a.Max, fmt.Sprintf("%s rec/s wants %d brokers; capped at the maximum %d", rate(load), n, a.Max)
	case n < a.Min:
		return a.Min, fmt.Sprintf("%s rec/s fits on fewer; held at the minimum %d", rate(load), a.Min)
	}
	return n, fmt.Sprintf("%s rec/s at %s per broker", rate(load), rate(float64(a.Target)))
}

// Mark is a recommendation and when it was made.
type Mark struct {
	At      time.Time
	Brokers int
}

// Decide is the broker count to run now. Brokers are added as soon as
// load calls for them. They are removed only when every recommendation
// over the last a.ScaleDownAfter asked for fewer, and then only down to
// the highest of those, so a dip or a gap between bursts doesn't shed
// brokers that are about to be needed again (moving partitions off and
// back costs more than a quiet broker). marks is oldest first and must
// include the latest.
func Decide(a Autoscale, current int, marks []Mark, now time.Time) (int, string) {
	if len(marks) == 0 {
		return current, "not measured yet"
	}
	latest := marks[len(marks)-1].Brokers
	if latest > current {
		return latest, fmt.Sprintf("load needs %d brokers", latest)
	}
	if latest == current {
		return current, "sized for the load"
	}
	// Only shrink once the window is fully observed: a restarted
	// operator has no history and mustn't read that as a quiet hour.
	since := now.Add(-a.ScaleDownAfter.Duration)
	if marks[0].At.After(since) {
		return current, fmt.Sprintf("load fits %d; watching for %s before removing brokers", latest, Short(a.ScaleDownAfter.Duration))
	}
	peak := latest
	for _, m := range marks {
		if !m.At.Before(since) && m.Brokers > peak {
			peak = m.Brokers
		}
	}
	if peak >= current {
		return current, fmt.Sprintf("load fits %d, but needed %d within %s", latest, peak, Short(a.ScaleDownAfter.Duration))
	}
	return peak, fmt.Sprintf("load has fit %d for %s", peak, Short(a.ScaleDownAfter.Duration))
}

// Trim drops marks older than the window, keeping the newest one before
// it so Decide can tell the window has been observed in full.
func Trim(marks []Mark, window time.Duration, now time.Time) []Mark {
	since := now.Add(-window)
	i := 0
	for i+1 < len(marks) && !marks[i+1].At.After(since) {
		i++
	}
	return marks[i:]
}

func rate(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case v >= 1e4:
		return fmt.Sprintf("%.0fk", v/1e3)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	}
	return fmt.Sprintf("%.0f", v)
}
