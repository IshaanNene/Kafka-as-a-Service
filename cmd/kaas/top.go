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

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/top"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newTopCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, sortBy string
		limit          int
		interval       time.Duration
		once, internal bool
	)
	cmd := &cobra.Command{
		Use:   "top [topic-pattern]",
		Short: "The busiest partitions by records in and out, with a meter per broker",
		Long: `Show which partitions are working hardest, like htop for a cluster. Each
broker gets a meter of its share of the records written; below them every
partition's rates: IN/s from its log-end offset, OUT/s from consumer
groups' commits, BYTES/s from the growth of its log on the leader.

Rates are differences between two readings, so kaas top reads twice,
--interval apart, before it shows anything; on a terminal it then goes on
refreshing. --once, a pipe and -o json print after the second reading.

The pattern is a glob over topic names (payments.*); a plain name must
exist.`,
		Example: `  # Live, busiest writers first
  kaas top
  # The payments topics, by disk growth
  kaas top 'payments.*' --sort bytes
  # Partitions nobody reads, for a script
  kaas top -o json | jq -r '.[] | select(.out_per_sec == 0) | "\(.topic)/\(.partition)"'`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if err := oneOf(cmd, "sort", sortBy, screens.SortIn, screens.SortOut, screens.SortBytes, screens.SortSize, screens.SortPartition); err != nil {
				return err
			}
			if limit < 0 {
				return usageError(cmd, "top", fmt.Sprint(limit), "must be 0 (fit the terminal) or more")
			}
			if interval < 100*time.Millisecond {
				return usageError(cmd, "interval", interval.String(), "must be at least 100ms")
			}
			pattern := ""
			if len(args) == 1 {
				pattern = args[0]
				if _, err := path.Match(pattern, ""); err != nil {
					return usageError(cmd, "topic-pattern", pattern, "not a valid glob: "+err.Error())
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
			tv := &topView{cl: cl, adm: kadm.NewClient(cl), pattern: pattern, internal: internal,
				sort: sortBy, limit: limit, interval: interval}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if surf.rich && output == "table" && !once {
				// The first frame has one reading and no rates yet; the
				// next refresh fills them in.
				return refreshLoop(cmd.Context(), surf, interval,
					func(ctx context.Context) error { tv.err = tv.refresh(ctx); return tv.err },
					func() []string { return tv.frame(surf, true) })
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second+interval)
			defer cancel()
			// Rates need two readings, an interval apart.
			if err := tv.refresh(ctx); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
			if err := tv.refresh(ctx); err != nil {
				return err
			}
			return tv.print(cmd, output, surf)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.StringVar(&sortBy, "sort", screens.SortIn, "sort by: in|out|bytes|size|partition")
	f.IntVar(&limit, "top", 0, "rows to show; 0 fits the terminal (and prints every row on a pipe)")
	f.DurationVar(&interval, "interval", 2*time.Second, "time between readings")
	f.BoolVar(&once, "once", false, "read twice, print, and exit instead of refreshing")
	f.BoolVar(&internal, "internal", false, "include internal topics such as __consumer_offsets")
	return cmd
}

// topView is kaas top's state across refreshes: the last two readings and
// the rates between them.
type topView struct {
	cl       *kgo.Client
	adm      *kadm.Client
	pattern  string
	internal bool
	sort     string
	limit    int
	interval time.Duration

	snap      *snapshot.Snapshot
	topics    int
	prev, cur top.Sample
	rows      []top.Row // sorted
	partial   error     // sizes or commits the cluster would not give
	err       error
}

func (tv *topView) refresh(ctx context.Context) error {
	s, err := snapshot.Fetch(ctx, tv.cl)
	if err != nil {
		return err
	}
	// A plain name is a lookup: missing means 404 with suggestions.
	if tv.pattern != "" && !strings.ContainsAny(tv.pattern, "*?[") {
		if err := missingTopic(s, []string{tv.pattern}); err != nil {
			return err
		}
	}
	var names []string
	leaders := map[top.Key]int32{}
	for _, t := range s.Topics {
		if t.Internal && !tv.internal && !strings.HasPrefix(tv.pattern, "_") {
			continue
		}
		if ok, _ := path.Match(tv.pattern, t.Name); tv.pattern != "" && !ok {
			continue
		}
		names = append(names, t.Name)
		for _, p := range t.Partitions {
			leaders[top.Key{Topic: t.Name, Partition: p.ID}] = p.Leader
		}
	}
	sample, err := top.Take(ctx, tv.adm, names, leaders)
	if len(sample.End) == 0 && err != nil {
		return err
	}
	tv.snap, tv.topics, tv.partial = s, len(names), err
	tv.prev, tv.cur = tv.cur, sample
	tv.rows = top.Rates(tv.prev, tv.cur)
	tv.sortRows()
	return nil
}

// sortRows orders by the --sort column, busiest first; unknown values
// sink, and ties fall back to topic and partition.
func (tv *topView) sortRows() {
	key := func(r top.Row) float64 {
		switch tv.sort {
		case screens.SortOut:
			return r.Out
		case screens.SortBytes:
			return r.Bytes
		case screens.SortSize:
			return float64(r.Size)
		}
		return r.In
	}
	slices.SortStableFunc(tv.rows, func(a, b top.Row) int {
		byName := cmp.Or(cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
		if tv.sort == screens.SortPartition {
			return byName
		}
		return cmp.Or(cmp.Compare(key(b), key(a)), byName)
	})
}

// totals sums the known rates over the rows.
func (tv *topView) totals() (in, out, bytes float64) {
	for _, r := range tv.rows {
		in += max(0, r.In)
		out += max(0, r.Out)
		bytes += max(0, r.Bytes)
	}
	return in, out, bytes
}

func (tv *topView) frame(surf surface, live bool) []string {
	return tv.screen(live).Render(surf.kit, surf.width, max(12, surf.height-1))
}

func (tv *topView) screen(live bool) screens.Sorter {
	in, out, bytes := tv.totals()
	v := screens.Sorter{Sort: tv.sort}

	// One meter per broker: its share of the records written.
	share := map[int32]float64{}
	for _, r := range tv.rows {
		share[r.Leader] += max(0, r.In)
	}
	ids := make([]int32, 0, len(tv.snap.Brokers))
	for _, b := range tv.snap.Brokers {
		ids = append(ids, b.ID)
	}
	slices.Sort(ids)
	for _, id := range ids {
		f := 0.0
		if in > 0 {
			f = share[id] / in
		}
		v.Meters = append(v.Meters, screens.Meter{Label: fmt.Sprint(id), Fill: f, Text: fmt.Sprintf("%.1f%%", f*100)})
	}

	// The gauge is reads against writes, summed over groups, so two groups
	// keeping pace read 2.0× and the bar is full from 1×.
	v.Gauge = screens.Meter{Label: "Read", Text: "no writes"}
	switch {
	case tv.prev.At.IsZero():
		v.Gauge.Text = "first reading"
		for i := range v.Meters {
			v.Meters[i].Text = "-"
		}
	case in > 0:
		v.Gauge.Fill = out / in
		v.Gauge.Text = fmt.Sprintf("%.1f× writes", min(99.9, out/in))
	case out > 0:
		v.Gauge.Fill, v.Gauge.Text = 1, "catching up"
	}

	parts := len(tv.rows)
	v.Stats = []screens.Stat{
		{Label: "Partitions", Value: fmt.Sprintf("%d in %s; %s", parts, plural(tv.topics, "topic"), plural(len(ids), "broker"))},
		{Label: "Write", Value: fmt.Sprintf("%s rec/s · %s/s", screens.Num(in), screens.Bytes(int64(bytes+0.5)))},
		{Label: "Read", Value: fmt.Sprintf("%s rec/s by %s", screens.Num(out), plural(len(tv.cur.Read), "group"))},
	}
	if tv.prev.At.IsZero() {
		v.Stats[1].Value, v.Stats[2].Value = "-", "-"
	}
	if tv.partial != nil {
		v.Stats = append(v.Stats, screens.Stat{Label: "Partial", Value: "some sizes or commits were refused"})
	}

	shown := tv.rows
	if tv.limit > 0 && len(shown) > tv.limit {
		v.Hidden = len(shown) - tv.limit
		shown = shown[:tv.limit]
	}
	for _, r := range shown {
		leader := fmt.Sprintf("b%d", r.Leader)
		if r.Leader < 0 {
			leader = "none"
		}
		v.Rows = append(v.Rows, screens.SortRow{Partition: r.Partition, Leader: leader, In: r.In, Out: r.Out,
			Bytes: r.Bytes, Size: r.Size, Topic: r.Topic, Note: "end " + screens.Num(float64(r.End))})
	}
	v.Empty = "No topics yet."
	if tv.pattern != "" {
		v.Empty = "No topics match " + tv.pattern + "."
	}

	v.Hints = []screens.Hint{{Key: "--sort ", Label: tv.sort, Hot: true}}
	if live {
		v.Hints = append([]screens.Hint{{Key: "^C", Label: "Quit"}}, v.Hints...)
		v.Hints = append(v.Hints, screens.Hint{Key: "every ", Label: tv.interval.String()})
	} else {
		v.Hints = append(v.Hints, screens.Hint{Key: "over ", Label: tv.interval.String()})
	}
	if tv.err != nil {
		v.Hints = []screens.Hint{{Key: "refresh failed: ", Label: tv.err.Error(), Hot: true}}
	}
	return v
}

// topRow is one partition in -o json; a negative rate or size is unknown.
type topRow struct {
	Topic       string  `json:"topic"`
	Partition   int32   `json:"partition"`
	Leader      int32   `json:"leader"`
	InPerSec    float64 `json:"in_per_sec"`
	OutPerSec   float64 `json:"out_per_sec"`
	BytesPerSec float64 `json:"bytes_per_sec"`
	SizeBytes   int64   `json:"size_bytes"`
	EndOffset   int64   `json:"end_offset"`
}

func (tv *topView) print(cmd *cobra.Command, output string, surf surface) error {
	out := cmd.OutOrStdout()
	rows := tv.rows
	if tv.limit > 0 && len(rows) > tv.limit {
		rows = rows[:tv.limit]
	}
	if output == "json" {
		js := make([]topRow, 0, len(rows))
		for _, r := range rows {
			js = append(js, topRow{Topic: r.Topic, Partition: r.Partition, Leader: r.Leader, InPerSec: r.In,
				OutPerSec: r.Out, BytesPerSec: r.Bytes, SizeBytes: r.Size, EndOffset: r.End})
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(js)
	}
	if surf.rich {
		surf.print(tv.frame(surf, false))
		return nil
	}
	num := func(v float64) string {
		if v < 0 {
			return "-"
		}
		return fmt.Sprintf("%.1f", v)
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tPARTITION\tLEADER\tIN/S\tOUT/S\tBYTES/S\tSIZE")
	for _, r := range rows {
		size := "-"
		if r.Size >= 0 {
			size = fmt.Sprint(r.Size)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%s\n", r.Topic, r.Partition, r.Leader, num(r.In), num(r.Out), num(r.Bytes), size)
	}
	return tw.Flush()
}
