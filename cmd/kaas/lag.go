package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// trendBack is how many refreshes ago a row's trend compares against.
const trendBack = 3

func newLagCmd(brokers, themeName *string) *cobra.Command {
	var (
		output   string
		top      int
		interval time.Duration
		once     bool
	)
	cmd := &cobra.Command{
		Use:   "lag [group-pattern]",
		Short: "Consumer lag ranked by group and topic, with total lag over time",
		Long: `Rank every group·topic pair by how many records the group is behind on
that topic, worst first. On a terminal it refreshes every --interval: rows
re-rank as lag moves, the trend shows each pair's lag against three
refreshes ago, and the chart draws total lag over time. When every group
is at the head of its topics, it says so.

The pattern is a glob over group names (billing-*).`,
		Example: `  # Live lag board, every 2s
  kaas lag
  # The billing groups only, top 5
  kaas lag 'billing*' --top 5
  # Exit 0 only when nothing is behind, for a deploy gate
  test "$(kaas lag -o json | jq '[.[].lag] | add // 0')" -eq 0`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if top < 1 {
				return usageError(cmd, "top", fmt.Sprint(top), "must be 1 or more")
			}
			if interval < 100*time.Millisecond {
				return usageError(cmd, "interval", interval.String(), "must be at least 100ms")
			}
			pattern := ""
			if len(args) == 1 {
				pattern = args[0]
				if _, err := path.Match(pattern, ""); err != nil {
					return usageError(cmd, "group-pattern", pattern, "not a valid glob: "+err.Error())
				}
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			lv := &lagView{adm: kadm.NewClient(cl), pattern: pattern, top: top, interval: interval, past: map[string][]int64{}}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if surf.rich && output == "table" && !once {
				return refreshLoop(cmd.Context(), surf, interval,
					func(ctx context.Context) error { lv.err = lv.refresh(ctx); return lv.err },
					func() []string { return lv.frame(surf, true) })
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			if err := lv.refresh(ctx); err != nil {
				return err
			}
			return lv.print(cmd, output, surf)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.IntVar(&top, "top", 10, "rows on the board")
	f.DurationVar(&interval, "interval", 2*time.Second, "refresh interval on a terminal")
	f.BoolVar(&once, "once", false, "read once and exit instead of refreshing")
	return cmd
}

// lagPair is one group's lag on one topic.
type lagPair struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
	Lag   int64  `json:"lag"`
}

func (p lagPair) key() string { return p.Group + "\x00" + p.Topic }

// lagView is kaas lag's state across refreshes: the ranked pairs, each
// pair's recent lag for its trend, and total lag over time.
type lagView struct {
	adm      *kadm.Client
	pattern  string
	top      int
	interval time.Duration

	prev, cur groups.Sample
	groups    int
	pairs     []lagPair // ranked, worst first; pairs with unknown lag are left out
	past      map[string][]int64
	history   []float64
	err       error
}

func (lv *lagView) refresh(ctx context.Context) error {
	s, err := groups.Fetch(ctx, lv.adm)
	if err != nil {
		return err
	}
	lv.prev, lv.cur = lv.cur, s
	lv.pairs, lv.groups = nil, 0
	var total int64
	for _, g := range s.Groups {
		if ok, _ := path.Match(lv.pattern, g.Name); lv.pattern != "" && !ok {
			continue
		}
		lv.groups++
		for topic, lag := range g.TopicLag {
			if lag < 0 {
				continue
			}
			lv.pairs = append(lv.pairs, lagPair{Group: g.Name, Topic: topic, Lag: lag})
			total += lag
		}
	}
	slices.SortFunc(lv.pairs, func(a, b lagPair) int {
		return cmp.Or(cmp.Compare(b.Lag, a.Lag), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Topic, b.Topic))
	})
	seen := map[string]bool{}
	for _, p := range lv.pairs {
		k := p.key()
		seen[k] = true
		lv.past[k] = append(lv.past[k], p.Lag)
		if n := len(lv.past[k]); n > trendBack+1 {
			lv.past[k] = lv.past[k][n-trendBack-1:]
		}
	}
	for k := range lv.past {
		if !seen[k] {
			delete(lv.past, k)
		}
	}
	lv.history = append(lv.history, float64(total))
	if n := len(lv.history); n > historyLen {
		lv.history = lv.history[n-historyLen:]
	}
	return nil
}

// consumed is records per second across the shown groups since the last
// refresh, or -1 before there are two.
func (lv *lagView) consumed() float64 {
	if lv.prev.At.IsZero() {
		return -1
	}
	var sum float64
	for _, g := range lv.cur.Groups {
		if ok, _ := path.Match(lv.pattern, g.Name); lv.pattern != "" && !ok {
			continue
		}
		if r, ok := groups.Rate(lv.prev, lv.cur, g.Name); ok {
			sum += r
		}
	}
	return sum
}

func (lv *lagView) total() int64 {
	var n int64
	for _, p := range lv.pairs {
		n += p.Lag
	}
	return n
}

// frame is the board, or the caught-up card when nothing is behind.
func (lv *lagView) frame(surf surface, live bool) []string {
	if lv.total() == 0 && lv.err == nil {
		return lv.caughtUp(live).Render(surf.kit, surf.width)
	}
	return lv.screen(live).Render(surf.kit, surf.width, surf.height)
}

func (lv *lagView) caughtUp(live bool) screens.CaughtUp {
	topics := map[string]bool{}
	for _, p := range lv.pairs {
		topics[p.Topic] = true
	}
	c := screens.CaughtUp{
		Title:  "You're all caught up",
		Detail: fmt.Sprintf("%s at the head of %s.", plural(lv.groups, "group"), plural(len(topics), "topic")),
		Action: "kaas groups for members and rates →",
	}
	if lv.groups == 1 {
		c.Detail = fmt.Sprintf("1 group, at the head of %s.", plural(len(topics), "topic"))
	}
	switch {
	case lv.groups == 0 && lv.pattern != "":
		c.Title, c.Detail = "Nothing to wait on", fmt.Sprintf("No groups match %s.", lv.pattern)
	case lv.groups == 0:
		c.Title, c.Detail = "Nothing to wait on", "No consumer groups have committed offsets yet."
	case live:
		c.Detail += " Watching every " + lv.interval.String() + "."
	}
	return c
}

func (lv *lagView) screen(live bool) screens.Lag {
	v := screens.Lag{Title: "consumer lag · ranked", Live: live, Chart: "total lag", History: lv.history}
	for i, p := range lv.pairs {
		if i == lv.top {
			v.Hidden = len(lv.pairs) - lv.top
			break
		}
		row := screens.LagRow{Group: p.Group, Topic: p.Topic, Lag: p.Lag}
		if past := lv.past[p.key()]; len(past) > 1 {
			row.Trend, row.TrendKnown = p.Lag-past[0], true
		}
		v.Rows = append(v.Rows, row)
	}
	if len(lv.history) < 2 {
		v.Foot = []screens.Detail{{Label: "total lag", Value: screens.Num(float64(lv.total()))}}
	} else {
		lo, hi, sum := lv.history[0], lv.history[0], 0.0
		for _, h := range lv.history {
			lo, hi, sum = min(lo, h), max(hi, h), sum+h
		}
		v.Foot = []screens.Detail{
			{Label: "min", Value: screens.Num(lo)},
			{Label: "avg", Value: screens.Num(sum / float64(len(lv.history)))},
			{Label: "max", Value: screens.Num(hi)},
		}
	}
	if c := lv.consumed(); c >= 0 {
		v.Foot = append(v.Foot, screens.Detail{Label: "consumed", Value: screens.Num(c) + "/s", Role: kit.Accent})
	}
	hint := []string{"kaas groups for members and rates"}
	if live {
		hint = append([]string{"every " + lv.interval.String(), "ctrl-c to stop"}, hint...)
	}
	v.Hint = strings.Join(hint, " · ")
	if lv.err != nil {
		v.Hint = "refresh failed, showing the last read: " + lv.err.Error()
	}
	return v
}

func (lv *lagView) print(cmd *cobra.Command, output string, surf surface) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		pairs := lv.pairs
		if pairs == nil {
			pairs = []lagPair{}
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(pairs)
	}
	if surf.rich {
		surf.print(lv.frame(surf, false))
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "GROUP\tTOPIC\tLAG")
	for _, p := range lv.pairs {
		fmt.Fprintf(tw, "%s\t%s\t%d\n", p.Group, p.Topic, p.Lag)
	}
	return tw.Flush()
}
