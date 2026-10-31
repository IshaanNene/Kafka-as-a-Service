package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

var snapshotSorts = []string{"status", "name", "partitions", "records"}

// topicRow is one topic's summary, shared by the screen and the plain table.
type topicRow struct {
	name                     string
	partitions, rf, urp, off int
	records                  int64 // -1 until offsets are known
	status                   kit.Status
}

func (r topicRow) statusText() string {
	switch r.status {
	case kit.Down:
		return "offline"
	case kit.Degraded:
		return "under-replicated"
	}
	return "healthy"
}

// topicRows summarises the non-internal topics, sorted by key; it also
// reports how many internal topics it left out.
func topicRows(s *snapshot.Snapshot, key string) (rows []topicRow, internal int) {
	for _, t := range s.Topics {
		if t.Internal {
			internal++
			continue
		}
		r := topicRow{name: t.Name, partitions: len(t.Partitions)}
		for _, p := range t.Partitions {
			r.rf = max(r.rf, len(p.Replicas))
			if p.UnderReplicated() {
				r.urp++
			}
			if p.Leader < 0 || len(p.Offline) > 0 {
				r.off++
			}
			if p.StartOffset == snapshot.UnknownOffset || p.EndOffset == snapshot.UnknownOffset {
				r.records = -1
			} else if r.records >= 0 {
				r.records += p.EndOffset - p.StartOffset
			}
		}
		switch {
		case r.off > 0:
			r.status = kit.Down
		case r.urp > 0:
			r.status = kit.Degraded
		default:
			r.status = kit.Good
		}
		rows = append(rows, r)
	}
	slices.SortStableFunc(rows, func(a, b topicRow) int {
		switch key {
		case "name":
			return cmp.Compare(a.name, b.name)
		case "partitions":
			return cmp.Or(cmp.Compare(b.partitions, a.partitions), cmp.Compare(a.name, b.name))
		case "records":
			return cmp.Or(cmp.Compare(b.records, a.records), cmp.Compare(a.name, b.name))
		}
		// status: offline first, then under-replicated, then the biggest.
		sev := func(r topicRow) int { return map[kit.Status]int{kit.Down: 2, kit.Degraded: 1}[r.status] }
		return cmp.Or(cmp.Compare(sev(b), sev(a)), cmp.Compare(b.urp, a.urp), cmp.Compare(b.records, a.records), cmp.Compare(a.name, b.name))
	})
	return rows, internal
}

func printSnapshot(cmd *cobra.Command, s *snapshot.Snapshot, output, sortBy, themeName string) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(s)
	}
	rows, internal := topicRows(s, sortBy)
	if surf := detect(out, themeName); surf.rich {
		surf.print(snapshotScreen(s, rows, internal, sortBy).Render(surf.kit, surf.width))
		return nil
	}
	fmt.Fprintf(out, "cluster %s · controller %d · %d brokers · %d topics · %d partitions\n\n",
		s.ClusterID, s.Controller, len(s.Brokers), len(s.Topics), s.PartitionCount())
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tPARTITIONS\tUNDER-REPLICATED")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%d\t%d\n", r.name, r.partitions, r.urp)
	}
	return w.Flush()
}

// snapshotScreen is kaas snapshot as the data-table piece: one row per
// topic with a status dot, the sort column in blue, and the cluster's
// totals and leader spread in the strip below.
func snapshotScreen(s *snapshot.Snapshot, rows []topicRow, internal int, sortBy string) screens.Table {
	t := screens.Table{
		Title: "Topics",
		Badge: fmt.Sprintf("cluster %s · %s", short(s.ClusterID), plural(len(rows), "topic")),
		Columns: []screens.Column{
			{Name: "Topic"},
			{Name: "Parts", Right: true},
			{Name: "RF", Right: true, Drop: 2},
			{Name: "URP", Right: true, Drop: 1},
			{Name: "Offline", Right: true, Drop: 3},
			{Name: "Records", Right: true, Drop: 4},
			{Name: "Status"},
		},
		Sorted: map[string]int{"name": 0, "partitions": 1, "records": 5, "status": 6}[sortBy],
		Asc:    sortBy == "name",
		Empty:  "No topics yet. Create one with kafka-topics --create, or produce to a topic with auto-create on.",
		Foot:   "sorted by " + map[string]string{"status": "status, worst first", "name": "name", "partitions": "partitions, most first", "records": "records, most first"}[sortBy],
	}
	if internal > 0 {
		t.FootRight = plural(internal, "internal topic") + " hidden"
	}
	var urp, off int
	var records int64
	for _, r := range rows {
		urp, off = urp+r.urp, off+r.off
		if r.records > 0 {
			records += r.records
		}
		num := func(n int, bad kit.Role) screens.Cell {
			if n > 0 {
				return screens.Cell{Text: strconv.Itoa(n), Role: bad}
			}
			return screens.Cell{Text: strconv.Itoa(n)}
		}
		rec := "-"
		if r.records >= 0 {
			rec = screens.Num(float64(r.records))
		}
		t.Rows = append(t.Rows, []screens.Cell{
			{Text: r.name},
			{Text: strconv.Itoa(r.partitions)},
			{Text: strconv.Itoa(r.rf)},
			num(r.urp, kit.Warn),
			num(r.off, kit.Err),
			{Text: rec},
			{Text: "● " + r.statusText(), Role: map[kit.Status]kit.Role{kit.Good: kit.OK, kit.Degraded: kit.Warn, kit.Down: kit.Err}[r.status]},
		})
	}

	// Leaders per broker as one bar each, so an unbalanced cluster shows.
	leaders := map[int32]int{}
	for _, tp := range s.Topics {
		for _, p := range tp.Partitions {
			leaders[p.Leader]++
		}
	}
	peak := 0
	for _, b := range s.Brokers {
		peak = max(peak, leaders[b.ID])
	}
	bars, ids := "", ""
	levels := []rune("▁▂▃▄▅▆▇█")
	for _, b := range s.Brokers {
		i := 0
		if peak > 0 {
			i = leaders[b.ID] * (len(levels) - 1) / peak
		}
		bars += string(levels[i])
	}
	if len(s.Brokers) > 0 {
		ids = "leaders · " + plural(len(s.Brokers), "broker")
	}
	bad := func(n int, r kit.Role) kit.Role {
		if n > 0 {
			return r
		}
		return kit.FG
	}
	t.Tiles = []screens.Tile{
		{Label: "Brokers", Value: fmt.Sprintf("%d · ctrl %d", len(s.Brokers), s.Controller)},
		{Label: "Partitions", Value: strconv.Itoa(s.PartitionCount())},
		{Label: "Under-replicated", Value: strconv.Itoa(urp), Role: bad(urp, kit.Warn)},
		{Label: "Offline", Value: strconv.Itoa(off), Role: bad(off, kit.Err)},
		{Label: "Records", Value: screens.Num(float64(records))},
	}
	if ids != "" {
		t.Tiles = append(t.Tiles, screens.Tile{Label: ids, Value: bars, Role: kit.Accent})
	}
	return t
}

// short cuts a cluster ID to 8 characters, like a short commit hash: enough
// to tell clusters apart, and never mistaken for clipped text.
func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
