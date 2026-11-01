package screens

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// The stages of a request on the waterfall, in the order they happen.
var WaterStages = []struct {
	Name string
	Role kit.Role
	Mark string
}{
	{"connect", kit.Info, "█"},
	{"send", kit.Warn, "█"},
	{"queue", kit.Rule, "▫"},
	{"wait", kit.OK, "█"},
	{"ack", kit.Accent, "█"},
}

// WaterRow is one request on the waterfall.
type WaterRow struct {
	Name   string // the API
	Key    int16
	Status string // "ok", "pending", "no ack", or an error such as NOT_COORDINATOR
	Info   string
	Size   int64         // the answer's bytes; -1 while pending
	Start  time.Duration // since the trace began
	Stages [5]time.Duration
	Done   bool
}

// Total is the row's time so far.
func (r WaterRow) Total() time.Duration {
	var t time.Duration
	for _, s := range r.Stages {
		t += s
	}
	return t
}

// WaterMark is a moment on the waterfall: a line down it and a label.
type WaterMark struct {
	At    time.Duration
	Label string // on the line: "joined billing"
	Text  string // in the footer: "joined billing: 212ms"
	Role  kit.Role
}

// Waterfall is the network-waterfall piece: requests as they arrive with
// their stages on a shared time axis, lines at the moments that matter,
// and a footer with the totals and the selected request's stages.
type Waterfall struct {
	Title   string // on the frame: "Trace"
	Note    string // beside it: "● billing-svc · 127.0.0.1:53422"
	Tabs    []string
	Tab     int
	Rows    []WaterRow
	Sel     int           // index into Rows; the newest when out of range
	Now     time.Duration // the live cursor; 0 when the trace is over
	Span    time.Duration // the axis' length
	Marks   []WaterMark
	Summary string // "24 requests | 18 KiB transferred | Finish: 1.24 s"
	Detail  string // the selected request's stages
	Hint    string
}

// niceStep is the axis' tick: 1, 2 or 5 of a power of ten, at most four
// ticks across span.
func niceStep(span time.Duration) time.Duration {
	if span <= 0 {
		return time.Millisecond
	}
	step := time.Duration(math.Pow(10, math.Floor(math.Log10(float64(span)/4))))
	for _, m := range []time.Duration{1, 2, 5, 10} {
		if span/(step*m) <= 4 {
			return step * m
		}
	}
	return step * 10
}

func tickLabel(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return strings.TrimSuffix(fmt.Sprintf("%.1f", d.Seconds()), ".0") + "s"
}

// Render draws the waterfall at width × height.
func (f Waterfall) Render(k kit.Kit, width, height int) []string {
	w, h := max(width, 72), max(height, 16)
	c := newCanvas(w, h)
	c.box(kit.Rule)
	x := c.put(2, 0, " "+f.Title+" ", kit.FG)
	c.put(x+1, 0, clipTo(" "+f.Note+" ", w-x-4), kit.Muted)
	x = 2
	for i, t := range f.Tabs {
		role := kit.Muted
		if i == f.Tab {
			role = kit.Accent
		}
		x = c.put(x, 1, t, role) + 2
	}

	wx := max(52, w-40) // the waterfall's left edge
	ww := w - wx - 2
	cStatus, cInfo := 18, 26
	cSize, cTime := wx-18, wx-9
	c.put(2, 2, "Name", kit.FG)
	c.put(cStatus, 2, "Status", kit.FG)
	c.put(cInfo, 2, "Info", kit.FG)
	c.put(cSize+4, 2, "Size", kit.FG)
	c.put(cTime+3, 2, "Time", kit.FG)
	c.put(wx, 2, "Waterfall", kit.FG)
	span := max(f.Span, time.Millisecond)
	col := func(d time.Duration) float64 { return float64(d) / float64(span) * float64(ww) }
	step := niceStep(span)
	for d := time.Duration(0); d < span; d += step {
		lab := tickLabel(d)
		c.put(min(w-2-len(lab), wx+int(math.Round(col(d)))), 3, lab, kit.Rule)
	}
	c.hline(1, w-2, 4, kit.Faint)

	top, bottom := 5, h-5 // the rows' lines
	list := bottom - top
	sel := f.Sel
	if sel < 0 || sel >= len(f.Rows) {
		sel = len(f.Rows) - 1
	}
	first := max(0, len(f.Rows)-list)
	if sel >= 0 && sel < first {
		first = sel
	}
	for i := range list {
		n := first + i
		if n >= len(f.Rows) {
			break
		}
		r, y := f.Rows[n], top+i
		if n == sel {
			c.put(1, y, "▶", kit.FG)
		}
		failed := r.Status != "ok" && r.Status != "pending" && r.Status != "no ack"
		nameRole, statusRole := APIRole(r.Key), kit.OK
		switch {
		case failed:
			statusRole = kit.Err
		case r.Status == "pending":
			statusRole = kit.Notice
		case r.Status == "no ack":
			statusRole = kit.Muted
		}
		status := r.Status
		info := r.Info
		if failed {
			status, info = "error", r.Status+" · "+r.Info
		}
		c.put(2, y, clipTo(r.Name, cStatus-3), nameRole)
		c.put(cStatus, y, status, statusRole)
		infoRole := kit.Muted
		if failed {
			infoRole = kit.Err
		}
		c.put(cInfo, y, clipTo(info, cSize-cInfo-1), infoRole)
		if r.Size >= 0 {
			c.put(cSize, y, lpad(Bytes(r.Size), 8), kit.Muted)
		}
		if r.Done {
			tr := kit.Muted
			if r.Total() > span/4 {
				tr = kit.Warn // a long one
			}
			c.put(cTime, y, lpad(Dur(r.Total()), 7), tr)
		}
		// Each stage takes its cells; where stages share a cell, the
		// longest of them shows, so a short request reads as its main stage.
		at := r.Start
		took := map[int]time.Duration{}
		for s, d := range r.Stages {
			if d <= 0 {
				continue
			}
			a, b := col(at), col(at+d)
			x0 := wx + int(math.Floor(a))
			x1 := max(x0, wx+int(math.Ceil(b))-1)
			for xx := x0; xx <= min(x1, wx+ww-1); xx++ {
				if d > took[xx] {
					took[xx] = d
					c.put(xx, y, WaterStages[s].Mark, WaterStages[s].Role)
				}
			}
			at += d
		}
	}

	// The moments: a line down the rows, then each label beside its
	// line on the lowest row with room, or only in the footer when no
	// row has any.
	xs := make([]int, len(f.Marks))
	for i, m := range f.Marks {
		xs[i] = wx + int(math.Round(col(m.At)))
		for y := top; y < bottom; y++ {
			if c.blank(xs[i], y) {
				c.put(xs[i], y, "│", m.Role)
			}
		}
	}
	free := func(x0, n, y int) bool {
		for x := x0; x < x0+n; x++ {
			if x >= w-1 || !c.blank(x, y) {
				return false
			}
		}
		return x0 > wx
	}
	for i, m := range f.Marks {
		n := kit.Width(m.Label)
		for y := bottom - 1; y >= top; y-- {
			if free(xs[i]+1, n, y) {
				c.put(xs[i]+1, y, m.Label, m.Role)
				break
			}
			if free(xs[i]-n, n, y) {
				c.put(xs[i]-n, y, m.Label, m.Role)
				break
			}
		}
	}
	if f.Now > 0 && f.Now < span {
		c.put(wx+int(math.Round(col(f.Now))), 3, "▼", kit.Notice)
	}

	c.hline(1, w-2, h-5, kit.Faint)
	c.put(2, h-4, clipTo(f.Detail, w-4), kit.FG)
	c.put(2, h-3, clipTo(f.Summary, w-4), kit.FG)
	x = 2
	for _, m := range f.Marks {
		x = c.put(x, h-2, m.Text, m.Role) + 3
	}
	lx := w - 2
	for _, s := range WaterStages {
		lx -= len(s.Name) + 4
	}
	if lx > x {
		for _, s := range WaterStages {
			c.put(lx, h-2, s.Mark, s.Role)
			lx = c.put(lx+2, h-2, s.Name, kit.Muted) + 2
		}
	}
	out := c.lines(k)
	if f.Hint != "" {
		out = append(out, " "+k.S(kit.Rule, f.Hint))
	}
	return kit.Clip(width, out)
}
