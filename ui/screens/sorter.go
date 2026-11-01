package screens

import (
	"fmt"
	"slices"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Meter is one bracketed bar of the Sorter's header: a label, a fill from
// 0 to 1, and text over the bar's right end.
type Meter struct {
	Label string
	Fill  float64
	Text  string
}

// Stat is one line of the Sorter's info block: a cyan label and its value.
type Stat struct{ Label, Value string }

// SortRow is one partition in the Sorter's table; a negative rate or size
// is unknown.
type SortRow struct {
	Partition int32
	Leader    string // "b1", or "none" when offline
	In, Out   float64
	Bytes     float64
	Size      int64
	Topic     string
	Note      string // muted after the topic, as the piece's command args
}

// Hint is one footer key and what it does.
type Hint struct {
	Key, Label string
	Hot        bool // yellow, as the piece's SortBy
}

// Sort columns, which are also kaas top's --sort values.
const (
	SortIn        = "in"
	SortOut       = "out"
	SortBytes     = "bytes"
	SortSize      = "size"
	SortPartition = "partition"
)

// Sorter is the process-sorter piece for partitions: htop's meter bars,
// one per broker, an info block beside a gauge, a table sorted by the
// active column with its ▐ and ▼, and a footer of keys.
type Sorter struct {
	Meters []Meter
	Gauge  Meter
	Stats  []Stat
	Sort   string
	Rows   []SortRow // in display order
	Hidden int       // rows left out by the caller
	Empty  string
	Hints  []Hint
}

// sorterCol is one table column; drop orders which go first when narrow.
type sorterCol struct {
	name, sort string
	w, drop    int
	cell       func(SortRow) (string, kit.Role)
}

// Render draws the screen at width × height.
func (s Sorter) Render(k kit.Kit, width, height int) []string {
	w := min(width, 132)
	var out []string
	two := w >= 72 && len(s.Meters) > 1
	colW := w - 2
	if two {
		colW = (w - 4) / 2
	}
	for i := 0; i < len(s.Meters); i++ {
		line := "  " + s.meter(k, s.Meters[i], colW, s.labelW(), false)
		if two && i+1 < len(s.Meters) {
			i++
			line = kit.Fit(line, colW+4) + s.meter(k, s.Meters[i], colW, s.labelW(), false)
		}
		out = append(out, line)
	}
	left := []string{"  " + s.meter(k, s.Gauge, colW, kit.Width(s.Gauge.Label), true)}
	var right []string
	for _, st := range s.Stats {
		// A value too wide wraps under itself rather than losing its end.
		lw := kit.Width(st.Label) + 2
		for i, part := range kit.Wrap(st.Value, max(8, colW-lw)) {
			lead := k.S(kit.Info, st.Label+": ")
			if i > 0 {
				lead = strings.Repeat(" ", lw)
			}
			right = append(right, lead+k.S(kit.FG, part))
		}
	}
	if two {
		for i := range max(len(left), len(right)) {
			l, r := "", ""
			if i < len(left) {
				l = left[i]
			}
			if i < len(right) {
				r = right[i]
			}
			out = append(out, kit.Fit(l, colW+4)+r)
		}
	} else {
		out = append(out, left...)
		for _, r := range right {
			out = append(out, "  "+r)
		}
	}
	out = append(out, k.S(kit.Faint, strings.Repeat("─", w)))

	cols := s.fit(w)
	out = append(out, s.header(k, cols, w))
	var peakIn, peakOut float64
	for _, r := range s.Rows {
		peakIn, peakOut = max(peakIn, r.In), max(peakOut, r.Out)
	}
	room := max(1, height-len(out)-2) // the footer and its gap
	rows, hidden := s.Rows, s.Hidden
	if len(rows) > room {
		hidden += len(rows) - (room - 1)
		rows = rows[:room-1]
	}
	for i, r := range rows {
		out = append(out, s.row(k, cols, r, i == 0, peakIn, peakOut, w))
	}
	if len(s.Rows) == 0 {
		for _, l := range kit.Wrap(s.Empty, w-4) {
			out = append(out, "  "+k.S(kit.Muted, l))
		}
	}
	if hidden > 0 {
		out = append(out, "  "+k.S(kit.Rule, fmt.Sprintf("+ %d more · raise --top to see them", hidden)))
	}
	out = append(out, "", s.footer(k, w))
	return kit.Clip(width, out)
}

func (s Sorter) labelW() int {
	n := 0
	for _, m := range s.Meters {
		n = max(n, kit.Width(m.Label))
	}
	return n
}

// meter is "1[|||||      42.0%]": the piece paints the first quarter of the
// fill blue, then green up to 80% of it, red beyond. The gauge, labelled,
// is the piece's Mem bar: green, blue past 60%, yellow past 80%.
func (s Sorter) meter(k kit.Kit, m Meter, w, labelW int, gauge bool) string {
	bw := max(4, w-labelW-2)
	n := int(min(1, max(0, m.Fill))*float64(bw) + 0.5)
	cells := make([]string, bw)
	for i := range bw {
		if i >= n {
			cells[i] = " "
			continue
		}
		role := kit.Err
		switch f := float64(i); {
		case gauge && f < float64(n)*0.6:
			role = kit.OK
		case gauge && f < float64(n)*0.8:
			role = kit.Accent
		case gauge:
			role = kit.Notice
		case i < int(float64(n)*0.25+0.5):
			role = kit.Accent
		case f < float64(n)*0.8:
			role = kit.OK
		}
		cells[i] = k.S(role, "|")
	}
	text := []rune(m.Text)
	if len(text) > bw {
		text = nil
	}
	for i, r := range text {
		cells[bw-len(text)+i] = k.S(kit.Muted, string(r))
	}
	label := k.S(kit.Info, lpad(m.Label, labelW))
	return label + k.S(kit.FG, "[") + strings.Join(cells, "") + k.S(kit.FG, "]")
}

func (s Sorter) columns() []sorterCol {
	rate := func(v float64) string {
		if v < 0 {
			return "-"
		}
		if v > 0 && v < 10 {
			return trimZero(fmt.Sprintf("%.1f", v))
		}
		return Short(v)
	}
	return []sorterCol{
		{name: "PART", sort: SortPartition, w: 5, cell: func(r SortRow) (string, kit.Role) {
			return fmt.Sprint(r.Partition), kit.Muted
		}},
		{name: "LEADER", w: 6, drop: 3, cell: func(r SortRow) (string, kit.Role) {
			if r.Leader == "none" {
				return r.Leader, kit.Err
			}
			return r.Leader, kit.Muted
		}},
		{name: "IN/s", sort: SortIn, w: 6, cell: func(r SortRow) (string, kit.Role) { return rate(r.In), kit.Rule }},
		{name: "OUT/s", sort: SortOut, w: 6, drop: 4, cell: func(r SortRow) (string, kit.Role) { return rate(r.Out), kit.Rule }},
		{name: "BYTES/s", sort: SortBytes, w: 9, drop: 2, cell: func(r SortRow) (string, kit.Role) {
			if r.Bytes < 0 {
				return "-", kit.Rule
			}
			return Bytes(int64(r.Bytes + 0.5)), kit.Rule
		}},
		{name: "SIZE", sort: SortSize, w: 9, drop: 1, cell: func(r SortRow) (string, kit.Role) { return Bytes(r.Size), kit.Rule }},
	}
}

// fit drops columns, least useful first but never the sorted one, until
// the longest topic name fits (up to 24 cells of it).
func (s Sorter) fit(w int) []sorterCol {
	need := 8
	for _, r := range s.Rows {
		need = max(need, min(24, kit.Width(r.Topic)))
	}
	cols := s.columns()
	used := func() int {
		n := 1 + 2 // the margin and the gap before the topic
		for _, c := range cols {
			n += c.w + 2
		}
		return n
	}
	for drop := 1; used()+need > w && drop <= 4; drop++ {
		cols = slices.DeleteFunc(cols, func(c sorterCol) bool { return c.drop == drop && c.sort != s.Sort })
	}
	return cols
}

// header is the column names in white, the sorted one blue with the
// piece's ▐ before it and ▼ after (▲ for partition order).
func (s Sorter) header(k kit.Kit, cols []sorterCol, w int) string {
	var b strings.Builder
	b.WriteString(" ")
	mark := func(active bool) {
		if active {
			b.WriteString(k.S(kit.Accent, "▐"))
		} else {
			b.WriteString(" ")
		}
	}
	arrow := "▼"
	if s.Sort == SortPartition {
		arrow = "▲"
	}
	after := false
	for _, c := range cols {
		active := c.sort == s.Sort
		if after {
			b.WriteString(k.S(kit.Accent, arrow))
		} else {
			b.WriteString(" ")
		}
		mark(active)
		role := kit.FG
		if active {
			role = kit.Accent
		}
		name := lpad(c.name, c.w)
		if c.name == "LEADER" {
			name = kit.Fit(c.name, c.w)
		}
		b.WriteString(k.S(role, name))
		after = active
	}
	if after {
		b.WriteString(k.S(kit.Accent, arrow))
	} else {
		b.WriteString(" ")
	}
	b.WriteString(" " + k.S(kit.FG, "TOPIC"))
	return kit.Fit(b.String(), w)
}

// row colours as the piece does: the top row's id and name yellow, the
// write rate red above half the busiest and orange above a tenth, the
// read rate purple above a tenth of the busiest reader.
func (s Sorter) row(k kit.Kit, cols []sorterCol, r SortRow, top bool, peakIn, peakOut float64, w int) string {
	var b strings.Builder
	b.WriteString(" ")
	for _, c := range cols {
		text, role := c.cell(r)
		switch {
		case c.name == "PART" && top:
			role = kit.Notice
		case c.name == "IN/s" && r.In > 0 && r.In > peakIn*0.5:
			role = kit.Err
		case c.name == "IN/s" && r.In > 0 && r.In > peakIn*0.1:
			role = kit.Warn
		case c.name == "OUT/s" && r.Out > 0 && r.Out > peakOut*0.1:
			role = kit.Accent2
		}
		cell := lpad(text, c.w)
		if c.name == "LEADER" {
			cell = kit.Fit(text, c.w)
		}
		b.WriteString("  " + k.S(role, cell))
	}
	b.WriteString("  ")
	room := w - kit.Width(b.String())
	name := kit.Fit(r.Topic, min(room, kit.Width(r.Topic)))
	role := kit.FG
	if top {
		role = kit.Notice
	}
	b.WriteString(k.S(role, name))
	if r.Note != "" && kit.Width(name)+1+kit.Width(r.Note) <= room {
		b.WriteString(" " + k.S(kit.Muted, r.Note))
	}
	return b.String()
}

// footer is the piece's key strip: each key white, its label cyan padded
// to eight, the hot one yellow; it stops before the edge.
func (s Sorter) footer(k kit.Kit, w int) string {
	var b strings.Builder
	used := 0
	for _, h := range s.Hints {
		label := h.Label
		if kit.Width(label) < 8 {
			label += strings.Repeat(" ", 8-kit.Width(label))
		} else {
			label += " "
		}
		n := kit.Width(h.Key) + kit.Width(label)
		if used+n > w {
			break
		}
		role := kit.Info
		if h.Hot {
			role = kit.Notice
		}
		b.WriteString(k.S(kit.FG, h.Key) + k.S(role, label))
		used += n
	}
	return b.String()
}
