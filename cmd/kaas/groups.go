package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

var groupSorts = []string{"lag", "rate", "members", "name"}

// groupTabs are the screen's sort tabs, in groupSorts order.
var groupTabs = []string{"Lag", "Rate", "Members", "Name"}

// historyLen is how many refreshes of total lag the live chart keeps.
const historyLen = 200

func newGroupsCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, sortBy string
		interval       time.Duration
		once           bool
	)
	cmd := &cobra.Command{
		Use:   "groups [pattern]",
		Short: "Consumer groups with their lag, consume rate and time to catch up, live",
		Long: `List consumer groups: their state, members, the topics they read, how many
records they are behind, how fast they consume and how long they need to
catch up at that rate. On a terminal it refreshes every --interval and
re-sorts as lag moves; the chart shows total lag, red where it grew.

Rate comes from committed offsets between two refreshes, so a single read
(--once, a pipe, or -o json) has lag but no rate. The pattern is a glob
(billing-*); a plain name that matches no group suggests the closest ones.`,
		Example: `  # Every group, worst lag first, refreshing every 2s
  kaas groups
  # Groups that read payments topics, by consume rate
  kaas groups 'payments-*' --sort rate
  # One read and exit
  kaas groups --once
  # Groups more than 10k records behind, for a script
  kaas groups -o json | jq -r '.[] | select(.lag > 10000) | .name'`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if err := oneOf(cmd, "sort", sortBy, groupSorts...); err != nil {
				return err
			}
			if interval < 100*time.Millisecond {
				return usageError(cmd, "interval", interval.String(), "must be at least 100ms")
			}
			pattern := ""
			if len(args) == 1 {
				pattern = args[0]
				if _, err := path.Match(pattern, ""); err != nil {
					return usageError(cmd, "pattern", pattern, "not a valid glob: "+err.Error())
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
			gv := groupView{adm: kadm.NewClient(cl), pattern: pattern, sortBy: sortBy, interval: interval}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if surf.rich && output == "table" && !once {
				return gv.live(cmd.Context(), surf)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			if err := gv.refresh(ctx); err != nil {
				return err
			}
			return gv.print(cmd, output, surf)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.StringVar(&sortBy, "sort", "lag", "sort by: "+strings.Join(groupSorts, "|"))
	f.DurationVar(&interval, "interval", 2*time.Second, "refresh interval on a terminal")
	f.BoolVar(&once, "once", false, "read once and exit instead of refreshing")
	return cmd
}

// groupView is kaas groups' state across refreshes: the last two samples
// for rates, and total lag over time for the chart.
type groupView struct {
	adm      *kadm.Client
	pattern  string
	sortBy   string
	interval time.Duration

	prev, cur groups.Sample
	measured  bool // prev is a real sample, so rates are known
	rows      []groups.Group
	hidden    int
	history   []int64
	err       error // the last refresh failed; the rows are from before it
}

// refresh takes a new sample and filters it. The first plain name that
// matches no group is a 404.
func (gv *groupView) refresh(ctx context.Context) error {
	s, err := groups.Fetch(ctx, gv.adm)
	if err != nil {
		return err
	}
	if gv.pattern != "" && !strings.ContainsAny(gv.pattern, "*?[") {
		names := make([]string, len(s.Groups))
		for i, g := range s.Groups {
			names[i] = g.Name
		}
		if !slices.Contains(names, gv.pattern) {
			describe := func(n string) string {
				i := slices.Index(names, n)
				return fmt.Sprintf("%s · %s", s.Groups[i].State, plural(s.Groups[i].Members, "member"))
			}
			return notFoundError("group", gv.pattern, "the cluster", names, describe, "kaas groups")
		}
	}
	gv.prev, gv.cur = gv.cur, s
	gv.measured = !gv.prev.At.IsZero()
	gv.rows, gv.hidden = nil, 0
	var total int64
	for _, g := range s.Groups {
		if ok, _ := path.Match(gv.pattern, g.Name); gv.pattern != "" && !ok {
			gv.hidden++
			continue
		}
		gv.rows = append(gv.rows, g)
		total += max(g.Lag, 0)
	}
	gv.sort()
	gv.history = append(gv.history, total)
	if len(gv.history) > historyLen {
		gv.history = gv.history[len(gv.history)-historyLen:]
	}
	return nil
}

// rate is a group's consume rate, or -1 before there are two samples.
func (gv *groupView) rate(name string) float64 {
	if !gv.measured {
		return -1
	}
	r, ok := groups.Rate(gv.prev, gv.cur, name)
	if !ok {
		return -1
	}
	return r
}

// sort orders rows by the chosen key, biggest first except names; unknown
// lag sorts last, and ties fall back to the name.
func (gv *groupView) sort() {
	key := func(g groups.Group) float64 {
		switch gv.sortBy {
		case "rate":
			return gv.rate(g.Name)
		case "members":
			return float64(g.Members)
		}
		return float64(g.Lag)
	}
	slices.SortStableFunc(gv.rows, func(a, b groups.Group) int {
		if gv.sortBy == "name" {
			return cmp.Compare(a.Name, b.Name)
		}
		return cmp.Or(cmp.Compare(key(b), key(a)), cmp.Compare(a.Name, b.Name))
	})
}

// live redraws the screen every interval until Ctrl-C. A refresh that fails
// keeps the last rows on screen and says why in the hint.
func (gv *groupView) live(ctx context.Context, surf surface) error {
	first, cancel := context.WithTimeout(ctx, 15*time.Second)
	err := gv.refresh(first)
	cancel()
	if err != nil {
		return err
	}
	lv := &live{s: surf}
	defer lv.done()
	lv.draw(gv.screen(true).Render(surf.kit, surf.width))
	t := time.NewTicker(gv.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Ctrl-C is how a live view ends, not a failure.
			return nil
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, max(gv.interval, 5*time.Second))
			gv.err = gv.refresh(rctx)
			cancel()
			if errors.Is(gv.err, context.Canceled) {
				return nil
			}
			lv.draw(gv.screen(true).Render(surf.kit, surf.width))
		}
	}
}

func (gv *groupView) screen(live bool) screens.Groups {
	v := screens.Groups{
		Tabs: groupTabs, Sorted: slices.Index(groupSorts, gv.sortBy),
		Badge:   plural(len(gv.rows), "group"),
		History: gv.history,
		Empty:   "No consumer groups yet. A group appears once a consumer with a group.id commits offsets.",
	}
	if gv.pattern != "" {
		v.Empty = fmt.Sprintf("No groups match %s.", gv.pattern)
	}
	for _, g := range gv.rows {
		v.Rows = append(v.Rows, screens.GroupRow{
			Name: g.Name, State: g.State, Members: g.Members, Topics: g.Topics,
			Lag: g.Lag, Rate: gv.rate(g.Name), Err: g.Err != nil,
		})
	}
	hint := []string{"sorted by " + gv.sortBy}
	if live {
		v.Badge = "live · every " + gv.interval.String()
		hint = append(hint, "every "+gv.interval.String(), "ctrl-c to stop")
		if !gv.measured {
			hint = append(hint, "rates after the next refresh")
		}
	} else {
		hint = append(hint, "--sort "+strings.Join(groupSorts, "|"), "-o json for scripts")
	}
	if gv.hidden > 0 {
		hint = append(hint, plural(gv.hidden, "group")+" hidden by the pattern")
	}
	v.Hint = strings.Join(hint, " · ")
	if gv.err != nil {
		v.Hint = "refresh failed, showing the last read: " + gv.err.Error()
	}
	return v
}

type groupJSON struct {
	Name         string   `json:"name"`
	State        string   `json:"state"`
	ProtocolType string   `json:"protocolType"`
	Members      int      `json:"members"`
	Topics       []string `json:"topics"`
	Partitions   int      `json:"partitions"`
	Lag          *int64   `json:"lag"` // null when unknown
	Committed    int64    `json:"committed"`
	Error        string   `json:"error,omitempty"`
}

func (gv *groupView) json() []groupJSON {
	out := []groupJSON{}
	for _, g := range gv.rows {
		j := groupJSON{
			Name: g.Name, State: g.State, ProtocolType: g.ProtocolType, Members: g.Members,
			Topics: g.Topics, Partitions: g.Partitions, Lag: known(g.Lag), Committed: g.Committed,
		}
		if j.Topics == nil {
			j.Topics = []string{}
		}
		if g.Err != nil {
			j.Error = g.Err.Error()
		}
		out = append(out, j)
	}
	return out
}

func (gv *groupView) print(cmd *cobra.Command, output string, surf surface) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(gv.json())
	}
	if surf.rich {
		surf.print(gv.screen(false).Render(surf.kit, surf.width))
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "GROUP\tSTATE\tMEMBERS\tTOPICS\tLAG")
	for _, g := range gv.rows {
		topics := strings.Join(g.Topics, ",")
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", g.Name, cmp.Or(g.State, "-"), g.Members, cmp.Or(topics, "-"), offsetText(g.Lag))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, g := range gv.rows {
		if g.Err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "kaas: warning: group %s: %v\n", g.Name, g.Err)
		}
	}
	return nil
}
