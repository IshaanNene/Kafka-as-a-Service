package screens

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Part is one partition row of the Partitions screen.
type Part struct {
	ID         int32
	Leader     int32 // -1 when offline
	Replicas   []int32
	ISR        []int32
	Start, End int64 // -1 unknown
	Bytes      int64 // on the leader; -1 unknown
}

// Detail is one "Label:  value" line in the details block.
type Detail struct {
	Label string
	Value string
	Role  kit.Role
}

// Partitions is the disk-partitioner piece for one topic: a centred header,
// an fdisk-style table with the partition that needs attention marked >>,
// a layout of the replicas each broker holds sized by bytes, details,
// buttons for the next commands and a status line.
type Partitions struct {
	Topic   string
	Info    []string // muted lines under the title
	Parts   []Part
	Focus   int     // index into Parts drawn with >>; -1 for none
	Brokers []int32 // every broker, so an empty one still shows
	// ReplicaBytes is broker → partition → bytes. Without it the layout
	// gives every replica the same width.
	ReplicaBytes map[int32]map[int32]int64
	Details      []Detail
	Actions      []string // first is highlighted
	Status       string
	StatusRole   kit.Role
	Hint         string
}

// partColors cycles through the piece's accents so neighbouring partitions
// stand apart in the layout.
var partColors = []kit.Role{kit.Accent, kit.Accent2, kit.Warn, kit.OK, kit.Info, kit.Err}

// Render draws the screen at width.
func (p Partitions) Render(k kit.Kit, width int) []string {
	w := min(width, 90)
	center := func(s string) string { return strings.Repeat(" ", max(0, (w-kit.Width(s))/2)) + s }
	out := []string{center(k.S(kit.FG, "Topic: "+p.Topic))}
	for _, l := range p.Info {
		for _, wl := range wrapParts(l, w-4) {
			out = append(out, center(k.S(kit.Muted, wl)))
		}
	}
	out = append(out, "")
	out = append(out, p.table(k, w)...)
	layoutNote := " · replicas per broker, sized by bytes"
	if w < 50 {
		layoutNote = " · by broker"
	}
	out = append(out, "", "    "+k.S(kit.Muted, "Layout")+k.S(kit.Rule, layoutNote))
	out = append(out, p.layout(k, w)...)
	out = append(out, "", k.S(kit.Faint, strings.Repeat("─", w)))

	lw := 0
	for _, d := range p.Details {
		lw = max(lw, kit.Width(d.Label)+1)
	}
	for _, d := range p.Details {
		for i, l := range kit.Wrap(d.Value, max(16, w-lw-6)) {
			label := ""
			if i == 0 {
				label = d.Label + ":"
			}
			out = append(out, "  "+k.S(kit.Muted, kit.Fit(label, lw))+"  "+k.S(d.Role, l))
		}
	}
	if len(p.Actions) > 0 {
		out = append(out, "")
		var line []string
		used := 4
		for i, a := range p.Actions {
			b := k.S(kit.Accent, "["+a+"]")
			if i == 0 {
				b = k.S(kit.Notice, "▐["+a+"]▌")
			}
			if used > 4 && used+kit.Width(b)+4 > w {
				out = append(out, "    "+strings.Join(line, "    "))
				line, used = nil, 4
			}
			line = append(line, b)
			used += kit.Width(b) + 4
		}
		out = append(out, "    "+strings.Join(line, "    "))
	}
	if p.Status != "" {
		out = append(out, "")
		for _, l := range kit.Wrap(p.Status, w-6) {
			out = append(out, "    "+k.S(p.StatusRole, l))
		}
	}
	if p.Hint != "" {
		out = append(out, "")
		for _, l := range wrapParts(p.Hint, w-4) {
			out = append(out, "  "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}

// wrapParts wraps a " · " separated line between its parts, so a value
// like "609 MiB" never splits; a part longer than w wraps on its own.
func wrapParts(s string, w int) []string {
	var out []string
	line := ""
	for _, part := range strings.Split(s, " · ") {
		switch {
		case line == "":
			line = part
		case kit.Width(line)+3+kit.Width(part) <= w:
			line += " · " + part
		default:
			out = append(out, kit.Wrap(line, w)...)
			line = part
		}
	}
	return append(out, kit.Wrap(line, w)...)
}

func ids(xs []int32) string {
	s := make([]string, len(xs))
	for i, x := range xs {
		s[i] = strconv.Itoa(int(x))
	}
	return strings.Join(s, ",")
}

// table is the fdisk-style listing. Narrow terminals lose the offsets, then
// the size, before anything is cut.
func (p Partitions) table(k kit.Kit, w int) []string {
	type column struct {
		name  string
		cell  func(Part) string
		right bool
		drop  int
	}
	num := func(n int64) string {
		if n < 0 {
			return "-"
		}
		return Num(float64(n))
	}
	cols := []column{
		{name: "Partition", cell: func(x Part) string { return strconv.Itoa(int(x.ID)) }},
		{name: "Leader", cell: func(x Part) string {
			if x.Leader < 0 {
				return "none"
			}
			return strconv.Itoa(int(x.Leader))
		}, right: true},
		{name: "Replicas", cell: func(x Part) string { return ids(x.Replicas) }},
		{name: "ISR", cell: func(x Part) string { return ids(x.ISR) }},
		{name: "Start", cell: func(x Part) string { return num(x.Start) }, right: true, drop: 4},
		{name: "End", cell: func(x Part) string { return num(x.End) }, right: true, drop: 3},
		{name: "Records", cell: func(x Part) string {
			if x.Start < 0 || x.End < 0 {
				return "-"
			}
			return num(x.End - x.Start)
		}, right: true, drop: 1},
		{name: "Size", cell: func(x Part) string { return Bytes(x.Bytes) }, right: true, drop: 2},
	}
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = kit.Width(c.name)
		for _, x := range p.Parts {
			widths[i] = max(widths[i], kit.Width(c.cell(x)))
		}
	}
	total := func() int {
		n := 4
		for i := range cols {
			n += widths[i] + 2
		}
		return n
	}
	for total() > w {
		drop := -1
		for i, c := range cols {
			if c.drop > 0 && (drop < 0 || c.drop > cols[drop].drop) {
				drop = i
			}
		}
		if drop < 0 {
			break
		}
		cols = slices.Delete(cols, drop, drop+1)
		widths = slices.Delete(widths, drop, drop+1)
	}
	pad := func(i int, s string) string {
		if cols[i].right {
			return lpad(s, widths[i])
		}
		return kit.Fit(s, widths[i])
	}
	var head []string
	for i, c := range cols {
		head = append(head, pad(i, c.name))
	}
	out := []string{"    " + k.S(kit.FG, strings.Join(head, "  "))}
	for r, x := range p.Parts {
		focus := r == p.Focus
		lead, base := "    ", kit.Muted
		if focus {
			lead, base = " "+k.S(kit.Notice, ">>")+" ", kit.FG
		}
		var cells []string
		for i, c := range cols {
			role := base
			switch {
			case c.name == "Leader" && x.Leader < 0:
				role = kit.Err
			case c.name == "ISR" && len(x.ISR) < len(x.Replicas):
				role = kit.Warn
			}
			cells = append(cells, k.S(role, pad(i, c.cell(x))))
		}
		out = append(out, lead+strings.Join(cells, "  "))
	}
	return out
}

// layout draws one bar per broker inside a box: each replica it holds is a
// segment in its partition's colour, sized by bytes against the fullest
// broker, so skew shows as uneven bars. █ leader, ▒ in-sync follower,
// ░ out of sync.
func (p Partitions) layout(k kit.Kit, w int) []string {
	type seg struct {
		part  int
		bytes int64
		glyph string
	}
	lw := 0
	for _, b := range p.Brokers {
		lw = max(lw, len(strconv.Itoa(int(b))))
	}
	inner := w - 6
	barW := inner - lw - 3
	known := len(p.ReplicaBytes) > 0
	bars := map[int32][]seg{}
	totals := map[int32]int64{}
	var peak int64
	for i, x := range p.Parts {
		for _, b := range x.Replicas {
			n := int64(1)
			if known {
				n = p.ReplicaBytes[b][x.ID]
			}
			g := "▒"
			switch {
			case b == x.Leader:
				g = "█"
			case !slices.Contains(x.ISR, b):
				g = "░"
			}
			bars[b] = append(bars[b], seg{part: i, bytes: n, glyph: g})
			totals[b] += n
		}
	}
	for _, t := range totals {
		peak = max(peak, t)
	}
	// Every replica gets at least one cell, so a broker that holds empty
	// partitions still shows them; when sizes are unknown or all zero, every
	// replica weighs the same.
	if peak == 0 {
		for b, ss := range bars {
			for i := range ss {
				ss[i].bytes = 1
			}
			totals[b] = int64(len(ss))
			peak = max(peak, totals[b])
		}
	}
	out := []string{"   " + k.S(kit.Rule, "┌"+strings.Repeat("─", inner)+"┐")}
	for _, b := range p.Brokers {
		line := k.S(kit.Muted, lpad(strconv.Itoa(int(b)), lw)) + " "
		// Segment edges are rounded from running totals, so the fullest
		// broker's bar fills the box exactly; every replica keeps a cell.
		used := 0
		var sum int64
		for _, s := range bars[b] {
			sum += s.bytes
			edge := int(math.Round(float64(barW) * float64(sum) / float64(peak)))
			cells := min(max(1, edge-used), barW-used)
			if cells <= 0 {
				break
			}
			role := partColors[s.part%len(partColors)]
			if s.glyph == "░" {
				role = kit.Err
			}
			line += k.S(role, strings.Repeat(s.glyph, cells))
			used += cells
		}
		line += k.S(kit.Faint, strings.Repeat("░", max(0, barW-used)))
		out = append(out, "   "+k.S(kit.Rule, "│")+" "+line+" "+k.S(kit.Rule, "│"))
	}
	out = append(out, "   "+k.S(kit.Rule, "└"+strings.Repeat("─", inner)+"┘"))

	// Legend: partition colours, then what the glyphs mean.
	var legend []string
	for i, x := range p.Parts {
		legend = append(legend, k.S(partColors[i%len(partColors)], "■")+k.S(kit.Muted, fmt.Sprintf(" p%d", x.ID)))
	}
	legend = append(legend, k.S(kit.Muted, "█ leader"), k.S(kit.Muted, "▒ follower"), k.S(kit.Muted, "░ out of sync"))
	line := ""
	for _, l := range legend {
		if line != "" && kit.Width(line)+kit.Width(l)+2 > w-7 {
			out = append(out, "       "+line)
			line = ""
		}
		if line != "" {
			line += "  "
		}
		line += l
	}
	return append(out, "       "+line)
}

// Bytes prints a size in binary units; -1 is unknown.
func Bytes(n int64) string {
	if n < 0 {
		return "-"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 || v >= 100 {
		return fmt.Sprintf("%.0f %s", v, units[i])
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}
