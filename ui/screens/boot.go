package screens

import (
	"fmt"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Boot lines read like a kernel and init log: a timestamped line with what
// a stage learned, then an [  OK  ] or [FAILED] verdict.

// BootStart is the line printed before probing begins.
func BootStart(k kit.Kit, seeds []string) []string {
	return []string{k.S(kit.Muted, "         Connecting to ") + k.S(kit.Accent2, join(seeds)) + k.S(kit.Muted, "...")}
}

// BootStep renders one finished probe step.
func BootStep(k kit.Kit, s probe.Step) []string {
	ts := k.S(kit.Muted, fmt.Sprintf("[%12.6f]", s.At.Seconds()))
	var out []string
	if s.Detail != "" {
		out = append(out, ts+" "+k.S(kit.FG, s.Stage.String()+": "+s.Detail))
	}
	took := k.S(kit.Muted, " ("+dur(s.Took)+")")
	if s.OK() {
		out = append(out, verdict(k, kit.OK, "  OK  ")+" "+k.S(kit.FG, okText(s))+took)
	} else {
		out = append(out, verdict(k, kit.Err, "FAILED")+" "+k.S(kit.FG, failText(s))+took)
		out = append(out, ts+" "+k.S(kit.Err, s.Stage.String()+": "+s.Err.Error()))
	}
	return out
}

// BootDone is the closing line: the target reached, or not.
func BootDone(k kit.Kit, r probe.Result) []string {
	if !r.OK() {
		return []string{verdict(k, kit.Warn, " WARN ") + " " + k.S(kit.FG, "Cluster unreachable after "+dur(r.Took))}
	}
	return []string{verdict(k, kit.OK, "  OK  ") + " " + k.S(kit.FG, "Reached target ") +
		k.B(kit.Accent, "cluster "+short(r.Cluster, 12)) +
		k.S(kit.Muted, fmt.Sprintf(" · %d brokers · %d topics · %s", r.Brokers, r.Topics, dur(r.Took)))}
}

func verdict(k kit.Kit, r kit.Role, s string) string {
	return k.S(kit.Muted, "[") + k.B(r, s) + k.S(kit.Muted, "]")
}

func okText(s probe.Step) string {
	switch s.Stage {
	case probe.DNS:
		return "Resolved " + s.Seed + "."
	case probe.TCP:
		return "Opened a connection to " + s.Seed + "."
	case probe.API:
		return "Negotiated the Kafka protocol with " + s.Seed + "."
	default:
		return "Fetched cluster metadata."
	}
}

func failText(s probe.Step) string {
	switch s.Stage {
	case probe.DNS:
		return "Could not resolve " + s.Seed + "."
	case probe.TCP:
		return "Could not connect to " + s.Seed + "."
	case probe.API:
		return s.Seed + " did not answer as a Kafka broker."
	default:
		return "Cluster metadata did not come back."
	}
}

func dur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	default:
		return d.Round(10 * time.Millisecond).String()
	}
}

func short(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func join(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += ", "
		}
		out += x
	}
	return out
}

// Unreachable turns a failed probe into the 503 page: one rack row per seed
// with a dot per stage, the deepest failure as the headline, and the next
// things worth trying.
func Unreachable(r probe.Result) Failure {
	f := Failure{
		Code:     "503",
		Title:    "Cluster unreachable",
		Rack:     "seeds",
		Legend:   []string{"d", "t", "a", "m"},
		RackFoot: plural(len(r.Seeds), "seed") + " probed",
	}
	reached := map[probe.Stage]int{}
	for _, seed := range r.Seeds {
		steps := r.Seed(seed)
		n := Node{Name: seed}
		for _, st := range probe.Stages {
			s, ok := steps[st]
			switch {
			case !ok:
				n.Dots = append(n.Dots, kit.Pending)
			case s.OK():
				n.Dots = append(n.Dots, kit.Good)
				reached[st]++
			default:
				n.Dots = append(n.Dots, kit.Down)
			}
		}
		f.Nodes = append(f.Nodes, n)
	}
	err := r.Err()
	var deepest probe.Step
	for _, s := range r.Steps {
		if !s.OK() && s.Stage >= deepest.Stage {
			deepest = s
		}
	}
	switch deepest.Stage {
	case probe.DNS:
		f.Subtitle = "None of the seed brokers resolve. Check the host names in --brokers or KAAS_BROKERS."
	case probe.TCP:
		f.Subtitle = "The seed brokers resolve but refuse connections. Is Kafka running and listening on that port?"
	case probe.API:
		f.Subtitle = "Something accepted the connection but did not speak Kafka. Is this the right port, or does the listener need TLS or SASL?"
	default:
		f.Subtitle = "Brokers answered but cluster metadata did not come back in time."
	}
	f.Facts = [][2]string{{"brokers", join(r.Seeds)}, {"error", err.Error()}, {"after", dur(r.Took)}}
	for _, st := range probe.Stages {
		p := Pill{Label: st.String(), Status: kit.Good, State: fmt.Sprintf("%d/%d", reached[st], len(r.Seeds))}
		switch {
		case st > deepest.Stage:
			p.Status, p.State = kit.Pending, "skipped"
		case reached[st] == 0:
			p.Status, p.State = kit.Down, "down"
		case reached[st] < len(r.Seeds):
			p.Status = kit.Degraded
		}
		f.Pills = append(f.Pills, p)
	}
	f.Actions = []string{"kaas -b host:9092", "task dev"}
	return f
}

func plural(n int, s string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", s)
	}
	return fmt.Sprintf("%d %ss", n, s)
}
