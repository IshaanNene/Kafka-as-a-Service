package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// LogKinds are what a cell of a log map holds, with their colours, in
// the order HeapCell.Kind counts them.
var LogKinds = []struct {
	Name string
	Role kit.Role
}{
	{"records", kit.Accent},
	{"compacted", kit.OK},
	{"committed", kit.Accent2},
	{"aborted", kit.Err},
	{"open txn", kit.Notice},
	{"markers", kit.Warn},
}

// HeapCell is one cell of the map.
type HeapCell struct {
	Hole    bool    // no record batch covers it
	Density float64 // the share of its offsets still holding a record
	Kind    int     // index into LogKinds
}

// HeapStat is one line of the stats panel.
type HeapStat struct {
	Label, Value string
	Role         kit.Role
}

// HeapLine is one line of the reads log.
type HeapLine struct {
	Text string
	Role kit.Role
}

// HeapMap is the heap-map piece: a framed grid of cells, each a range of
// the log, with a stats panel, a legend of kinds, and a log of reads.
type HeapMap struct {
	Title   string // on the frame: "log · orders p0 · leader 1"
	Range   string // under it: "offsets 0 – 48,213 · 50 a cell"
	Cells   []HeapCell
	Stats   []HeapStat
	Bar     float64 // the panel's bar: the share of offsets with no record
	Reads   []HeapLine
	Legend  string
	Foot    string
	Heading [2]string // the panel's headings: "LOG STATS", "READS"
}

// HeapGrid is how many cells across and down a map gets at width × height.
func HeapGrid(width, height int) (int, int) {
	return min(48, max(width, 64)-36), min(20, max(height, 24)-8)
}

// glyph is a cell's mark: solid when full, lighter as compaction thins it.
func glyph(c HeapCell) string {
	switch {
	case c.Density >= 0.9:
		return "█"
	case c.Density >= 0.6:
		return "▓"
	case c.Density >= 0.3:
		return "▒"
	}
	return "░"
}

// Render draws the map at width × height; the grid takes what the panel
// leaves, up to the piece's 48 × 20.
func (h HeapMap) Render(k kit.Kit, width, height int) []string {
	w, ht := max(width, 64), max(height, 24)
	mw, mh := HeapGrid(width, height)
	c := newCanvas(w, ht)
	// The frame around the grid.
	bw, bh := mw+4, mh+3
	c.put(0, 0, "╭"+strings.Repeat("─", bw-2)+"╮", kit.Rule)
	c.put(0, bh-1, "╰"+strings.Repeat("─", bw-2)+"╯", kit.Rule)
	for y := 1; y < bh-1; y++ {
		c.put(0, y, "│", kit.Rule)
		c.put(bw-1, y, "│", kit.Rule)
	}
	c.put(2, 0, clipTo(" "+h.Title+" ", bw-4), kit.Muted)
	c.put(2, 1, clipTo(h.Range, mw), kit.Rule)
	for i, cell := range h.Cells {
		if i >= mw*mh {
			break
		}
		x, y := 2+i%mw, 2+i/mw
		if cell.Hole {
			c.put(x, y, "·", kit.Faint)
			continue
		}
		role := LogKinds[min(len(LogKinds)-1, max(0, cell.Kind))].Role
		g := glyph(cell)
		if g == "█" && i+1 < len(h.Cells) && (i+1)%mw != 0 {
			next := h.Cells[i+1]
			if next.Hole || next.Kind != cell.Kind || glyph(next) != g {
				g = "▌" // the end of a run
			}
		}
		c.put(x, y, g, role)
	}

	sx := bw + 2
	heads := h.Heading
	if heads[0] == "" {
		heads = [2]string{"LOG STATS", "READS"}
	}
	c.put(sx, 1, heads[0], kit.FG)
	for i, s := range h.Stats {
		y := 3 + i
		c.put(sx, y, s.Label, kit.Muted)
		c.put(sx+13, y, clipTo(s.Value, w-sx-13), s.Role)
	}
	by := 3 + len(h.Stats)
	const barW = 20
	n := int(h.Bar*barW + 0.5)
	role := kit.Notice
	if h.Bar > 0.6 {
		role = kit.Err
	}
	c.put(sx, by, strings.Repeat("█", n), role)
	c.put(sx+n, by, strings.Repeat("░", barW-n), kit.Faint)
	ky := by + 2
	for i, kd := range LogKinds {
		x, y := sx+(i%2)*12, ky+i/2
		c.put(x, y, "█", kd.Role)
		c.put(x+2, y, kd.Name, kit.Muted)
	}
	ry := ky + (len(LogKinds)+1)/2 + 1
	c.put(sx, ry, heads[1], kit.FG)
	for i, l := range h.Reads {
		if ry+1+i >= ht-4 {
			break
		}
		c.put(sx, ry+1+i, clipTo(l.Text, w-sx-1), l.Role)
	}
	c.put(2, ht-4, clipTo(h.Legend, w-4), kit.Rule)
	c.put(2, ht-2, clipTo(h.Foot, w-4), kit.Muted)
	return kit.Clip(width, c.lines(k))
}
