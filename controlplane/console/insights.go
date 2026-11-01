package console

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
)

// Insight is something about a cluster worth acting on, said in terms of
// what it costs the business, with the step that fixes or explains it.
// Insights are recomputed from live state on every read, so one goes away
// as soon as its cause does.
type Insight struct {
	ID       string   `json:"id"` // stable while the cause lasts: kind/cluster/subject
	Cluster  string   `json:"cluster"`
	Severity Severity `json:"severity"` // critical, warning or info
	Kind     string   `json:"kind"`
	Title    string   `json:"title"`
	Detail   string   `json:"detail"`
	Impact   string   `json:"impact"`
	Href     string   `json:"href,omitempty"`    // where in the console to look
	Command  string   `json:"command,omitempty"` // a kaas command that shows or fixes it
}

// Thresholds for the insights.
const (
	staleAfter    = 60.0 // seconds behind before a group's data counts as stale
	skewRatio     = 1.5  // a broker leading this many times its even share
	skewMinLeader = 6    // partitions before skew is worth a word
)

var sevRank = map[Severity]int{SevCritical: 0, SevWarning: 1, SevInfo: 2, SevOK: 3}

func sortInsights(is []Insight) {
	sort.SliceStable(is, func(i, j int) bool {
		if a, b := sevRank[is[i].Severity], sevRank[is[j].Severity]; a != b {
			return a < b
		}
		return is[i].ID < is[j].ID
	})
}

// insights reads the cluster's state for things to act on. The caller
// holds mu and passes the card it computed under the same lock.
func (c *Cluster) insights(k Card) []Insight {
	name := c.Config.Name
	href := clusterPath(name)
	seed := ""
	if len(c.Config.Brokers) > 0 {
		seed = c.Config.Brokers[0]
	}
	out := []Insight{}
	add := func(in Insight) {
		in.Cluster = name
		in.ID = in.Kind + "/" + name + "/" + in.ID
		out = append(out, in)
	}

	switch k.Status {
	case StatusUnreachable:
		add(Insight{Kind: "unreachable", Severity: SevCritical,
			Title:   name + " is not answering",
			Detail:  "No broker has answered for three metadata intervals: " + shorten(k.Error, 120),
			Impact:  "Producers and consumers on this cluster are failing or retrying; nothing here can be observed or changed.",
			Href:    href,
			Command: "kaas snapshot -b " + seed})
		return out
	case StatusConnecting:
		return out
	}

	s := c.snap
	readers := map[string]int{}
	for _, g := range c.groups.Groups {
		for _, t := range g.Topics {
			readers[t]++
		}
	}
	var offline, urp, single []string
	for _, t := range s.Topics {
		if t.Internal {
			continue
		}
		var off, under bool
		for _, p := range t.Partitions {
			if p.Leader < 0 {
				off = true
			} else if p.UnderReplicated() {
				under = true
			}
		}
		switch {
		case off:
			offline = append(offline, t.Name)
		case under:
			urp = append(urp, t.Name)
		}
		if len(t.Partitions) > 0 && len(t.Partitions[0].Replicas) == 1 && k.Brokers > 1 {
			single = append(single, t.Name)
		}
		if r := c.rates[t.Name]; r != nil && r.measured && r.rate >= 1 && readers[t.Name] == 0 {
			add(Insight{ID: t.Name, Kind: "unread-topic", Severity: SevInfo,
				Title:   fmt.Sprintf("%s takes %s records/s and nobody reads it", t.Name, compact(r.rate)),
				Detail:  "No consumer group has committed offsets on this topic.",
				Impact:  "You pay to store data no one uses, or a consumer that should be reading it is down.",
				Href:    clusterPath(name, "topics", t.Name),
				Command: "kaas consume " + t.Name + " --from -5 -b " + seed})
		}
	}
	if len(offline) > 0 {
		add(Insight{ID: "all", Kind: "offline", Severity: SevCritical,
			Title:   fmt.Sprintf("%s offline", plural(k.Offline, "partition")),
			Detail:  "No leader for partitions of " + list(offline) + ".",
			Impact:  "Writes to these partitions fail and their consumers stop: every app on these topics is affected now.",
			Href:    clusterPath(name, "topics"),
			Command: "kaas snapshot -b " + seed})
	}
	if len(urp) > 0 {
		add(Insight{ID: "all", Kind: "under-replicated", Severity: SevWarning,
			Title:   fmt.Sprintf("%s under-replicated", plural(k.URP, "partition")),
			Detail:  "Replicas have fallen out of sync on " + list(urp) + ".",
			Impact:  "One more broker failure could lose acknowledged writes or take these partitions offline.",
			Href:    clusterPath(name, "topics"),
			Command: "kaas watch -b " + seed})
	}
	if len(single) > 0 && c.Config.Env == "production" {
		add(Insight{ID: "all", Kind: "single-copy", Severity: SevWarning,
			Title:   fmt.Sprintf("%s in production keep%s one copy", plural(len(single), "topic"), map[bool]string{true: "s", false: ""}[len(single) == 1]),
			Detail:  list(single) + map[bool]string{true: " has", false: " have"}[len(single) == 1] + " replication factor 1 on a cluster of " + plural(k.Brokers, "broker") + ".",
			Impact:  "Losing that one broker's disk loses this data for good.",
			Href:    clusterPath(name, "topics"),
			Command: "kaas topics describe " + single[0] + " -b " + seed})
	}

	for _, g := range c.groups.Groups {
		b := c.behind(g)
		switch {
		case g.Lag > 0 && g.Members == 0 && g.State == "Empty":
			add(Insight{ID: g.Name, Kind: "abandoned-group", Severity: SevWarning,
				Title:   fmt.Sprintf("%s has stopped with %s unread", g.Name, plural64(g.Lag, "record")),
				Detail:  "The group has no members, and its backlog on " + strings.Join(g.Topics, ", ") + " grows.",
				Impact:  "Whatever this group feeds is not getting new data.",
				Href:    clusterPath(name, "consumers"),
				Command: "kaas groups " + g.Name + " -b " + seed})
		case b >= staleAfter:
			add(Insight{ID: g.Name, Kind: "stale-data", Severity: SevWarning,
				Title:   fmt.Sprintf("%s is reading data %s old", g.Name, ago(b)),
				Detail:  fmt.Sprintf("%s behind on %s.", plural64(g.Lag, "record"), strings.Join(g.Topics, ", ")),
				Impact:  "Anything built on this group, from dashboards to fraud checks, acts on old information.",
				Href:    clusterPath(name, "consumers"),
				Command: "kaas lag -b " + seed})
		}
	}

	bs := brokers.Summarize(s, c.disk)
	online, leaders := 0, 0
	for _, b := range bs {
		if b.Online {
			online++
			leaders += b.Leaders
		}
	}
	if online > 1 && leaders >= skewMinLeader {
		even := float64(leaders) / float64(online)
		for _, b := range bs {
			if b.Online && float64(b.Leaders) >= skewRatio*even {
				add(Insight{ID: fmt.Sprint(b.ID), Kind: "leader-skew", Severity: SevInfo,
					Title:   fmt.Sprintf("Broker %d leads %d%% of partitions", b.ID, int(100*float64(b.Leaders)/float64(leaders)+0.5)),
					Detail:  fmt.Sprintf("%d of %d leaders, where an even share is %.0f.", b.Leaders, leaders, even),
					Impact:  "That broker does most of the work: it saturates first and its loss moves the most traffic.",
					Href:    href,
					Command: "kaas brokers -b " + seed})
			}
		}
	}

	if p := c.plan; p != nil && len(p.Ops) > 0 {
		file := filepath.Base(c.Config.Desired)
		n := p.Counts()
		in := Insight{ID: "plan", Kind: "drift", Severity: SevInfo,
			Title:   fmt.Sprintf("%s differs from %s", name, file),
			Detail:  fmt.Sprintf("%s to apply: %d safe, %d caution, %d destructive.", plural(len(p.Ops), "change"), n[plan.Safe], n[plan.Caution], n[plan.Destructive]),
			Impact:  "What runs in production isn't what was reviewed in git.",
			Href:    clusterPath(name, "gitops"),
			Command: "kaas plan -f " + file}
		if n[plan.Destructive] > 0 {
			in.Severity = SevWarning
		}
		if why := p.Blocked(); why != "" {
			in.Kind, in.Severity = "blocked-plan", SevWarning
			in.Title = name + "'s desired state can't be applied"
			in.Detail = why
			in.Impact = "Changes merged to git aren't reaching the cluster until the file is fixed."
		}
		add(in)
	}
	return out
}

func clusterPath(name string, rest ...string) string {
	p := "/clusters/" + name
	for _, r := range rest {
		p += "/" + r
	}
	return p
}

func list(names []string) string {
	if len(names) <= 3 {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:3], ", ") + fmt.Sprintf(" and %d more", len(names)-3)
}

func shorten(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func plural64(n int64, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return compact(float64(n)) + " " + word + "s"
}

// compact says a number in at most four characters: 950, 12.4k, 3.1M.
func compact(v float64) string {
	switch {
	case v >= 1e9:
		return trim(fmt.Sprintf("%.1f", v/1e9)) + "B"
	case v >= 1e6:
		return trim(fmt.Sprintf("%.1f", v/1e6)) + "M"
	case v >= 1e4:
		return trim(fmt.Sprintf("%.1f", v/1e3)) + "k"
	}
	return fmt.Sprintf("%.0f", v)
}

func trim(s string) string { return strings.TrimSuffix(s, ".0") }

// ago says seconds as the largest whole unit and the next: 4m 10s, 2h 5m.
func ago(s float64) string {
	n := int(s + 0.5)
	switch {
	case n < 60:
		return fmt.Sprintf("%ds", n)
	case n < 3600:
		return two(n/60, "m", n%60, "s")
	}
	return two(n/3600, "h", n%3600/60, "m")
}

func two(a int, au string, b int, bu string) string {
	if b == 0 {
		return fmt.Sprintf("%d%s", a, au)
	}
	return fmt.Sprintf("%d%s %d%s", a, au, b, bu)
}
