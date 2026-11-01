package screens

import (
	"fmt"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Flow endpoint states.
const (
	FlowLive = iota // records moving
	FlowIdle        // open, nothing lately
	FlowDone        // read to its end
	FlowFailed
)

// FlowEnd is one source or sink of a data flow.
type FlowEnd struct {
	Name   string  // "kafka:orders"
	Sub    string  // "group etl · from start"
	Count  string  // "12,408 read"
	State  int     // FlowLive …
	Fill   float64 // a sink's fill, 0 to 1, when it's drawn as a store
	Detail string  // the error, when failed
}

// FlowStep is one line of the transform stage.
type FlowStep struct {
	Text   string
	Active bool
}

// FlowStat is one counter of the footer.
type FlowStat struct {
	Label, Value string
	Role         kit.Role
}

// DataFlow is the data-flow piece: up to three sources down the left,
// paths joining into a transform stage, and the sinks on the right, a
// store when there is one and boxes when it fans out, with the run's
// counters along the bottom. Frame moves the records along the paths.
type DataFlow struct {
	Sources  []FlowEnd
	Sinks    []FlowEnd
	Steps    []FlowStep
	InFlight int // records read but not yet written
	Batch    int // the most a batch holds, for the queue's scale
	Stats    []FlowStat
	Job      string // at the bottom right: "pipe orders-to-lake  00:42"
	Frame    int
}

// The piece's source colours, in order: orange, blue, purple.
var flowRoles = []kit.Role{kit.Warn, kit.Accent, kit.Accent2}

const flowRows = 24

// FlowHeight is the rows the piece takes.
func FlowHeight() int { return flowRows }

// flowCorner is the corner a path turns at, by the way it came in and
// the way it goes out.
var flowCorner = map[string]string{"RD": "╮", "RU": "╯", "LD": "╭", "LU": "╰", "DR": "╰", "DL": "╯", "UR": "╭", "UL": "╮"}

type cell struct{ x, y int }

func dirOf(a, b cell) string {
	switch {
	case b.x > a.x:
		return "R"
	case b.x < a.x:
		return "L"
	case b.y > a.y:
		return "D"
	}
	return "U"
}

// pathCells walks the corners pts one cell at a time.
func pathCells(pts ...cell) []cell {
	out := []cell{pts[0]}
	for _, p := range pts[1:] {
		c := out[len(out)-1]
		for c != p {
			c.x += sign(p.x - c.x)
			c.y += sign(p.y - c.y)
			out = append(out, c)
		}
	}
	return out
}

func sign(n int) int {
	switch {
	case n > 0:
		return 1
	case n < 0:
		return -1
	}
	return 0
}

func drawPath(c *canvas, cells []cell, r kit.Role) {
	for n, p := range cells {
		in, out := "R", "R"
		if n > 0 {
			in = dirOf(cells[n-1], p)
		}
		if n < len(cells)-1 {
			out = dirOf(p, cells[n+1])
		}
		g := "─"
		switch {
		case in != out:
			g = flowCorner[in+out]
		case in == "U" || in == "D":
			g = "│"
		}
		c.put(p.x, p.y, g, r)
	}
}

// moving puts records on a path: one every few cells, stepping with the
// frame, as the piece's records travel.
func moving(c *canvas, cells []cell, frame int, r kit.Role) {
	for n := 1; n < len(cells)-1; n++ {
		turn := dirOf(cells[n-1], cells[n]) != dirOf(cells[n], cells[n+1])
		if (n-frame%6+6)%6 == 0 && !turn { // corners keep their shape
			c.put(cells[n].x, cells[n].y, "■", r)
		}
	}
}

// rows are where n boxes of height h sit, spread around the middle row.
func flowRowsFor(n int) []int {
	switch n {
	case 0:
		return nil
	case 1:
		return []int{9}
	case 2:
		return []int{4, 14}
	}
	return []int{1, 9, 17}
}

// shown is ends cut to three boxes, the third saying how many more.
func shown(ends []FlowEnd) []FlowEnd {
	if len(ends) <= 3 {
		return ends
	}
	out := append([]FlowEnd(nil), ends[:2]...)
	more := FlowEnd{Name: fmt.Sprintf("+%d more", len(ends)-2), State: FlowIdle}
	for _, e := range ends[2:] {
		if e.State == FlowFailed {
			more.State, more.Detail = FlowFailed, e.Name+" failed"
		} else if e.State == FlowLive && more.State != FlowFailed {
			more.State = FlowLive
		}
	}
	more.Sub = "see --stats"
	return append(out, more)
}

func endBox(c *canvas, x, y, w int, e FlowEnd, role kit.Role) {
	if e.State == FlowFailed {
		role = kit.Err
	}
	c.put(x, y, "╭"+strings.Repeat("─", w-2)+"╮", role)
	for i := 1; i < 4; i++ {
		c.put(x, y+i, "│", role)
		c.put(x+w-1, y+i, "│", role)
	}
	c.put(x, y+4, "╰"+strings.Repeat("─", w-2)+"╯", role)
	c.put(x+2, y+1, clipTo(e.Name, w-4), kit.FG)
	sub := e.Sub
	if e.Count != "" {
		sub = e.Count
	}
	c.put(x+2, y+2, clipTo(sub, w-4), kit.Muted)
	status, sr := "○ idle", kit.Rule
	switch e.State {
	case FlowLive:
		status, sr = "● streaming", role
	case FlowDone:
		status, sr = "✓ done", kit.OK
	case FlowFailed:
		status, sr = "✗ "+orDefault(e.Detail, "failed"), kit.Err
	}
	c.put(x+2, y+3, clipTo(status, w-4), sr)
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

var flowStore = []string{
	"  ______________  ",
	" /              \\ ",
	"|\\______________/|",
	"|                |",
	"|                |",
	"|                |",
	"|                |",
	" \\______________/ ",
}

// Render draws the flow width wide, at least the piece's 80, and 24 rows.
func (d DataFlow) Render(k kit.Kit, width int) []string {
	w := max(width, 80)
	c := newCanvas(w, flowRows)
	const mid, sx, sw = 11, 1, 18
	tw := 21 // the piece's transform stage, wider when there's room
	switch {
	case w >= 120:
		tw = 33
	case w >= 90:
		tw = 27
	}
	wx := w - 20 // the sinks' column
	tx := (sx+sw+wx)/2 - tw/2
	jin := sx + sw + (tx-sx-sw)/2  // where the inbound paths join
	jout := tx + tw + (wx-tx-tw)/2 // where the outbound ones split

	srcs, sinks := shown(d.Sources), shown(d.Sinks)
	live := func(e FlowEnd) bool { return e.State == FlowLive }

	// Inbound paths, then the records on them.
	for i, y := range flowRowsFor(len(srcs)) {
		p := pathCells(cell{sx + sw, y + 2}, cell{jin, y + 2}, cell{jin, mid}, cell{tx - 1, mid})
		drawPath(c, p, kit.Rule)
		if live(srcs[i]) {
			moving(c, p, d.Frame, flowRoles[i%3])
		}
	}
	if len(srcs) == 3 {
		c.put(jin, mid, "┼", kit.Rule)
	} else if len(srcs) == 2 {
		c.put(jin, mid, "├", kit.Rule)
	}
	c.put(tx-1, mid, "▶", kit.Rule)

	// Outbound: straight into one store, or split to each sink.
	store := len(sinks) == 1
	anyLive := false
	for _, e := range sinks {
		anyLive = anyLive || live(e)
	}
	if store {
		p := pathCells(cell{tx + tw, mid}, cell{wx - 1, mid})
		drawPath(c, p, kit.Rule)
		if anyLive {
			moving(c, p, d.Frame, kit.OK)
		}
		c.put(wx-1, mid, "▶", kit.Rule)
	} else {
		for i, y := range flowRowsFor(len(sinks)) {
			p := pathCells(cell{tx + tw, mid}, cell{jout, mid}, cell{jout, y + 2}, cell{wx - 1, y + 2})
			drawPath(c, p, kit.Rule)
			if live(sinks[i]) {
				moving(c, p, d.Frame, kit.OK)
			}
			c.put(wx-1, y+2, "▶", kit.Rule)
		}
		if len(sinks) == 3 {
			c.put(jout, mid, "┼", kit.Rule)
		} else if len(sinks) == 2 {
			c.put(jout, mid, "┤", kit.Rule)
		}
	}

	for i, y := range flowRowsFor(len(srcs)) {
		endBox(c, sx, y, sw, srcs[i], flowRoles[i%3])
	}

	// The transform stage.
	c.put(tx, 4, "┏"+strings.Repeat("━", tw-2)+"┓", kit.Notice)
	for y := 5; y < 18; y++ {
		c.put(tx, y, "┃", kit.Notice)
		c.put(tx+tw-1, y, "┃", kit.Notice)
	}
	c.put(tx, 18, "┗"+strings.Repeat("━", tw-2)+"┛", kit.Notice)
	c.put(tx+2, 5, "transform", kit.FG)
	c.hline(tx+1, tx+tw-2, 6, kit.Faint)
	for i, s := range d.Steps {
		if i >= 4 {
			break
		}
		y := 8 + i*2
		r := kit.Muted
		if s.Active {
			r = kit.Notice
			c.put(tx+tw-4, y, "◀", kit.Notice)
		}
		c.put(tx+2, y, clipTo(fmt.Sprintf("%d %s", i+1, s.Text), tw-7), r)
	}
	qw := tw - 4
	q := 0
	if d.InFlight > 0 {
		q = max(1, min(qw, d.InFlight*qw/max(1, d.Batch)))
	}
	c.put(tx+2, 16, strings.Repeat("■", q), kit.Warn)
	c.put(tx+2+q, 16, strings.Repeat("·", qw-q), kit.Faint)
	c.put(tx+2, 17, clipTo(fmt.Sprintf("in flight %d", d.InFlight), qw), kit.Muted)

	// The sinks.
	if store {
		e := sinks[0]
		role := kit.Info
		if e.State == FlowFailed {
			role = kit.Err
		}
		for i, l := range flowStore {
			for j, ch := range l {
				if ch != ' ' {
					c.put(wx+j, 7+i, string(ch), role)
				}
			}
		}
		c.put(wx+3, 11, clipTo(e.Name, 13), kit.FG)
		c.put(wx+3, 12, clipTo(orDefault(e.Count, e.Sub), 13), kit.Muted)
		if e.State == FlowFailed {
			c.put(wx+3, 13, clipTo("✗ "+orDefault(e.Detail, "failed"), 13), kit.Err)
		} else {
			n := int(min(1, max(0, e.Fill))*14 + 0.5)
			c.put(wx+2, 13, strings.Repeat("▄", n), kit.OK)
		}
		if e.Sub != "" && e.Count != "" {
			c.put(wx, 16, clipTo(e.Sub, 19), kit.Rule)
		}
	} else {
		for i, y := range flowRowsFor(len(sinks)) {
			endBox(c, wx, y, 19, sinks[i], []kit.Role{kit.OK, kit.Info, kit.Accent2}[i%3])
		}
	}

	// The counters.
	c.hline(0, w-1, flowRows-2, kit.Faint)
	x := 1
	for _, s := range d.Stats {
		x = c.put(x, flowRows-1, s.Label, kit.Muted) + 1
		x = c.put(x, flowRows-1, s.Value, s.Role) + 3
	}
	if d.Job != "" && x < w-kit.Width(d.Job)-1 {
		c.put(w-1-kit.Width(d.Job), flowRows-1, d.Job, kit.Muted)
	}
	return c.lines(k)
}
