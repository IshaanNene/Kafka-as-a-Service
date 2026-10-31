package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Column is one column of a Table.
type Column struct {
	Name  string
	Right bool // right-align numbers
	Drop  int  // when the table is too wide, higher Drop goes first; 0 never
}

// Cell is one value; Role defaults to Muted, as data-table greys every
// column but the first.
type Cell struct {
	Text string
	Role kit.Role
}

// Tile is one figure in the summary strip under the table.
type Tile struct {
	Label string
	Value string
	Role  kit.Role // defaults to FG
}

// Table is the data-table piece: a titled frame with a live badge, sortable
// columns (the sorted one blue with ▼), status dots, a footer and a strip of
// summary tiles. kaas snapshot, topics, groups and clients draw with it.
type Table struct {
	Title     string
	Badge     string // "cluster abc · 12 topics", drawn green after ●
	Columns   []Column
	Rows      [][]Cell
	Sorted    int  // index of the sorted column
	Asc       bool // ▲ instead of ▼
	Empty     string
	Foot      string // "sorted by records descending"
	FootRight string
	Tiles     []Tile
}

// Render draws the table at width, dropping low-priority columns before it
// truncates anything but the first one.
func (t Table) Render(k kit.Kit, width int) []string {
	w := min(width, 100)
	inner := w - 2
	cols := t.fit(inner - 2)

	head := make([]string, 0, len(cols))
	for _, c := range cols {
		name := t.Columns[c.i].Name
		role := kit.Muted
		if c.i == t.Sorted {
			name += map[bool]string{true: " ▲", false: " ▼"}[t.Asc]
			role = kit.Accent
		}
		head = append(head, k.S(role, c.pad(name)))
	}
	body := []string{" " + strings.Join(head, "  "), k.S(kit.Rule, strings.Repeat("─", inner))}
	for _, row := range t.Rows {
		cells := make([]string, 0, len(cols))
		for j, c := range cols {
			cell := Cell{}
			if c.i < len(row) {
				cell = row[c.i]
			}
			role := cell.Role
			if role == kit.FG && j > 0 {
				role = kit.Muted
			}
			text := c.pad(cell.Text)
			if j == 0 {
				text = c.pad(kit.Fit(cell.Text, c.w))
			}
			cells = append(cells, k.S(role, text))
		}
		body = append(body, " "+strings.Join(cells, "  "))
	}
	if len(t.Rows) == 0 {
		for _, l := range kit.Wrap(t.Empty, inner-2) {
			body = append(body, " "+k.S(kit.Muted, l))
		}
	}
	body = append(body, "", k.S(kit.Rule, strings.Repeat("─", inner)))
	if kit.Width(t.Foot)+kit.Width(t.FootRight)+4 <= inner {
		body = append(body, " "+spread(k.S(kit.Muted, t.Foot), k.S(kit.Muted, t.FootRight), inner-2))
	} else {
		for _, part := range []string{t.Foot, t.FootRight} {
			if part == "" {
				continue
			}
			for _, l := range kit.Wrap(part, inner-2) {
				body = append(body, " "+k.S(kit.Muted, l))
			}
		}
	}
	if len(t.Tiles) > 0 {
		body = append(body, "")
		body = append(body, t.tiles(k, inner-2)...)
	}
	badge := ""
	if t.Badge != "" {
		badge = k.S(kit.OK, "● "+t.Badge)
		if kit.Width(t.Title)+kit.Width(t.Badge)+12 > w {
			badge = k.S(kit.OK, "●")
		}
	}
	return kit.Clip(width, k.Draw(kit.Box{Title: k.S(kit.FG, t.Title), Right: badge, Role: kit.Rule, Width: w}, body))
}

type col struct {
	i, w  int
	right bool
}

func (c col) pad(s string) string {
	if c.right {
		return lpad(s, c.w)
	}
	return kit.Fit(s, c.w)
}

// fit picks the columns that fit in w and their widths. Names matter more
// than numbers: low-priority columns go before the first column is cut,
// so two long names never shorten into the same text.
func (t Table) fit(w int) []col {
	widths := make([]int, len(t.Columns))
	for i, c := range t.Columns {
		widths[i] = kit.Width(c.Name)
		if i == t.Sorted {
			widths[i] += 2
		}
		for _, row := range t.Rows {
			if i < len(row) {
				widths[i] = max(widths[i], kit.Width(row[i].Text))
			}
		}
	}
	keep := make([]bool, len(t.Columns))
	for i := range keep {
		keep[i] = true
	}
	total := func() int {
		n := -2
		for i, k := range keep {
			if k {
				n += widths[i] + 2
			}
		}
		return n
	}
	// Drop columns until the first one fits whole; only when nothing is
	// left to drop does the first column shrink.
	for total() > w {
		drop, worst := -1, 0
		for i, c := range t.Columns {
			if keep[i] && c.Drop > worst {
				drop, worst = i, c.Drop
			}
		}
		if drop < 0 {
			break
		}
		keep[drop] = false
	}
	widths[0] = max(1, min(widths[0], w-(total()-widths[0])))
	var out []col
	for i, k := range keep {
		if k {
			out = append(out, col{i: i, w: widths[i], right: t.Columns[i].Right})
		}
	}
	return out
}

// tiles lays the summary figures out label over value, wrapping onto more
// rows when they pass w.
func (t Table) tiles(k kit.Kit, w int) []string {
	var out []string
	var labels, values []string
	used := 0
	flush := func() {
		if len(labels) > 0 {
			out = append(out, " "+strings.Join(labels, ""), " "+strings.Join(values, ""))
			labels, values, used = nil, nil, 0
		}
	}
	for _, tl := range t.Tiles {
		tw := max(kit.Width(tl.Label), kit.Width(tl.Value)) + 4
		if used > 0 && used+tw > w {
			flush()
			out = append(out, "")
		}
		tw = min(tw, w)
		labels = append(labels, k.S(kit.Muted, kit.Fit(tl.Label, tw)))
		values = append(values, k.S(tl.Role, kit.Fit(tl.Value, tw)))
		used += tw
	}
	flush()
	return out
}
