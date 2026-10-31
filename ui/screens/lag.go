package screens

import (
	"fmt"
	"math"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// LagRow is one group reading one topic on the Lag board.
type LagRow struct {
	Group, Topic string
	Lag          int64
	Trend        int64 // lag now minus a few refreshes ago
	TrendKnown   bool
}

// Lag is the leaderboard piece for consumer lag: group·topic pairs ranked
// worst first with medals for the top three, a bar for each one's share of
// the worst and a trend, above the line-chart-live piece drawing total lag
// over time.
type Lag struct {
	Title   string
	Live    bool
	Rows    []LagRow  // ranked, worst first
	Hidden  int       // rows past --top
	History []float64 // charted from two readings up
	Chart   string    // the chart's caption, "total lag"
	Foot    []Detail  // min, avg, max, consumed
	Hint    string
}

// lagBars cycles the piece's bar colours down the board.
var lagBars = []kit.Role{kit.Accent, kit.Accent2, kit.Info, kit.Accent2, kit.OK}

// Render draws the board at width, giving the chart whatever height is
// left of height, between 3 and 8 rows.
func (l Lag) Render(k kit.Kit, width, height int) []string {
	w := min(width, 100)
	inner := w - 2
	var peak int64
	nameW := kit.Width("group · topic")
	lagW := kit.Width("lag")
	trendW := 0
	for _, r := range l.Rows {
		if r.TrendKnown {
			trendW = kit.Width("trend")
		}
	}
	for _, r := range l.Rows {
		peak = max(peak, r.Lag)
		nameW = max(nameW, kit.Width(r.Group+" · "+r.Topic))
		lagW = max(lagW, kit.Width(Num(float64(r.Lag))))
		if trendW > 0 {
			trendW = max(trendW, kit.Width(trendText(r)))
		}
	}
	// Each row is " NN ● " (6 cells), the name, then lag, bar and trend,
	// each after two spaces. The bar takes what is left, up to 20 cells;
	// narrow frames lose the bar, then the trend, before cutting a name.
	base := 6 + 2 + lagW
	barW := min(20, inner-base-nameW-2-trendW-2)
	if barW < 6 {
		barW = 0
	}
	used := func() int {
		n := base + nameW
		if barW > 0 {
			n += 2 + barW
		}
		if trendW > 0 {
			n += 2 + trendW
		}
		return n
	}
	if used() > inner {
		trendW = 0
	}
	if used() > inner {
		nameW = max(8, nameW-(used()-inner))
	}
	// The trend sits at the right edge, as in the piece.
	gap := max(0, inner-used()-1)

	head := " " + k.S(kit.Muted, " #   "+kit.Fit("group · topic", nameW)+"  "+lpad("lag", lagW))
	if barW > 0 {
		head += "  " + k.S(kit.Muted, kit.Fit("share", barW))
	}
	if trendW > 0 {
		head += strings.Repeat(" ", gap) + "  " + k.S(kit.Muted, lpad("trend", trendW))
	}
	body := []string{head, k.S(kit.Faint, strings.Repeat("─", inner))}
	for i, r := range l.Rows {
		medal := kit.Muted
		medalHex := ""
		switch i {
		case 0:
			medal = kit.Notice
		case 1:
			medalHex = "#c9d1d9" // silver, as the piece
		case 2:
			medalHex = "#e3a26b" // bronze
		}
		paint := func(s string) string {
			if medalHex != "" {
				return k.Hex(medalHex, s)
			}
			return k.S(medal, s)
		}
		line := " " + paint(lpad(fmt.Sprint(i+1), 2))
		if i < 3 {
			line += " " + paint("●") + " "
		} else {
			line += "   "
		}
		nameRole := kit.FG
		if i >= 3 {
			nameRole = kit.Muted
		}
		name := r.Group + " · " + r.Topic
		if kit.Width(name) <= nameW {
			line += k.S(nameRole, r.Group) + k.S(kit.Rule, " · ") + k.S(kit.Muted, r.Topic) + strings.Repeat(" ", nameW-kit.Width(name))
		} else {
			line += k.S(nameRole, kit.Fit(name, nameW))
		}
		lagRole := kit.FG
		switch {
		case i == 0 && r.Lag > 0:
			lagRole = kit.Notice
		case r.Lag == 0:
			lagRole = kit.OK
		}
		line += "  " + k.S(lagRole, lpad(Num(float64(r.Lag)), lagW))
		if barW > 0 {
			n := 0
			if peak > 0 {
				n = int(math.Round(float64(barW) * float64(r.Lag) / float64(peak)))
				if r.Lag > 0 {
					n = max(n, 1)
				}
			}
			line += "  " + k.S(lagBars[i%len(lagBars)], strings.Repeat("█", n)) + k.S(kit.Faint, strings.Repeat("░", barW-n))
		}
		if trendW > 0 {
			role := kit.Muted
			switch {
			case r.Trend > 0:
				role = kit.Err
			case r.Trend < 0:
				role = kit.OK
			}
			line += strings.Repeat(" ", gap) + "  " + k.S(role, lpad(trendText(r), trendW))
		}
		body = append(body, line)
	}
	if l.Hidden > 0 {
		body = append(body, "      "+k.S(kit.Rule, fmt.Sprintf("+ %d more · --top to show them", l.Hidden)))
	}

	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	// One reading is a point, not a line: the chart starts with the second.
	if len(l.History) >= 2 {
		chartH := min(8, max(3, height-len(body)-8))
		body = append(body, l.chart(k, inner, chartH)...)
	}
	var foot []string
	for _, d := range l.Foot {
		foot = append(foot, k.S(kit.Muted, d.Label+" ")+k.S(d.Role, d.Value))
	}
	body = append(body, joinWrap(k, foot, inner-2, " ")...)

	badge := ""
	if l.Live {
		badge = k.S(kit.OK, "● live")
	}
	out := k.Draw(kit.Box{Title: k.S(kit.FG, l.Title), Right: badge, Role: kit.Faint, Width: w}, body)
	if l.Hint != "" {
		for _, h := range wrapParts(l.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, h))
		}
	}
	return kit.Clip(width, out)
}

func trendText(r LagRow) string {
	switch {
	case !r.TrendKnown:
		return ""
	case r.Trend > 0:
		return "▲ " + Short(float64(r.Trend))
	case r.Trend < 0:
		return "▼ " + Short(float64(-r.Trend))
	}
	return "─"
}

// Short prints a count in at most five cells: 950, 12k, 1.2M.
func Short(v float64) string {
	switch {
	case v < 1000:
		return fmt.Sprintf("%.0f", v)
	case v < 10_000:
		return trimZero(fmt.Sprintf("%.1fk", v/1000))
	case v < 1_000_000:
		return fmt.Sprintf("%.0fk", v/1000)
	case v < 10_000_000:
		return trimZero(fmt.Sprintf("%.1fM", v/1_000_000))
	case v < 1_000_000_000:
		return fmt.Sprintf("%.0fM", v/1_000_000)
	}
	return trimZero(fmt.Sprintf("%.1fG", v/1_000_000_000))
}

func trimZero(s string) string { return strings.Replace(s, ".0", "", 1) }

// braille dot bits for a 2×4 cell, [row][column].
var brailleBits = [4][2]rune{{0x01, 0x08}, {0x02, 0x10}, {0x04, 0x20}, {0x40, 0x80}}

// chart is the line-chart-live piece: a caption with the latest value, a
// y axis, the series as a braille line with a shaded fill below it, newest
// on the right. A short series is widened so it fills the plot.
func (l Lag) chart(k kit.Kit, w, h int) []string {
	last := "-"
	if n := len(l.History); n > 0 {
		last = Num(l.History[n-1])
	}
	out := []string{" " + spread(k.S(kit.FG, l.Chart), k.S(kit.FG, last), w-2)}
	labelW := 5
	plotW := w - labelW - 4
	vals := l.History
	if len(vals) > plotW*2 {
		vals = vals[len(vals)-plotW*2:]
	}
	// The axis spans the series, from zero unless every value sits well
	// above it, so movement in a large backlog still shows.
	lo, hi := math.Inf(1), 0.0
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	if len(vals) == 0 || lo < 0.5*hi || lo == hi {
		lo = 0
	}
	hi = niceCeil(hi)
	if lo > 0 {
		lo = niceFloor(lo, hi)
	}
	dotsH := h * 4
	toDy := func(v float64) int {
		if hi <= lo {
			return dotsH - 1
		}
		return min(dotsH-1, max(0, int(math.Round((1-(v-lo)/(hi-lo))*float64(dotsH-1)))))
	}
	// Spread the samples over the dot columns, right-aligned, drawing
	// straight segments between them so a short series reads as a line.
	per := 1
	if len(vals) > 1 {
		per = max(1, min(8, (plotW*2-1)/(len(vals)-1)))
	}
	cols := 1
	if len(vals) > 1 {
		cols = (len(vals)-1)*per + 1
	}
	start := plotW*2 - cols
	at := func(i int) float64 {
		if len(vals) == 1 {
			return vals[0]
		}
		j, f := i/per, float64(i%per)/float64(per)
		if j >= len(vals)-1 {
			return vals[len(vals)-1]
		}
		return vals[j] + (vals[j+1]-vals[j])*f
	}
	dots := make([][]rune, h)
	top := make([]int, plotW)
	for y := range dots {
		dots[y] = make([]rune, plotW)
	}
	for i := range top {
		top[i] = dotsH
	}
	prev := -1
	for i := 0; len(vals) > 0 && i < cols; i++ {
		dx := start + i
		if dx < 0 {
			continue
		}
		dy := toDy(at(i))
		a, b := dy, dy
		if prev >= 0 {
			a, b = min(prev, dy), max(prev, dy)
		}
		for yy := a; yy <= b; yy++ {
			dots[yy/4][dx/2] |= brailleBits[yy%4][dx%2]
		}
		top[dx/2] = min(top[dx/2], dy)
		prev = dy
	}
	heat := k.Theme.Heat
	shade := func(glyph string, i int) string {
		if len(heat) == 0 {
			return k.S(kit.Faint, glyph)
		}
		return k.Hex(heat[min(i, len(heat)-1)], glyph)
	}
	for y := range h {
		label := ""
		switch y {
		case 0:
			label = Short(hi)
		case h / 2:
			label = Short((hi + lo) / 2)
		case h - 1:
			label = Short(lo)
		}
		tick := "│"
		if label != "" {
			tick = "┤"
		}
		line := " " + k.S(kit.Muted, lpad(label, labelW-1)) + " " + k.S(kit.Muted, tick)
		for x := range plotW {
			switch {
			case dots[y][x] != 0:
				line += k.S(kit.Accent, string(0x2800+dots[y][x]))
			case top[x] < dotsH && y > top[x]/4:
				d := float64(y-top[x]/4) / float64(h)
				switch {
				case d < 0.25:
					line += shade("▓", 1)
				case d < 0.6:
					line += shade("▒", 0)
				default:
					line += shade("░", 0)
				}
			case label != "" && x%2 == 0:
				line += k.S(kit.Faint, "┄")
			default:
				line += " "
			}
		}
		out = append(out, line)
	}
	out = append(out, " "+strings.Repeat(" ", labelW)+k.S(kit.Muted, "└"+strings.Repeat("─", plotW)))
	return out
}

// niceCeil rounds v up to a tenth of its order of magnitude, so axis
// labels read as round numbers without wasting the top of the plot.
func niceCeil(v float64) float64 {
	if v <= 0 {
		return 0
	}
	step := math.Pow(10, math.Floor(math.Log10(v))) / 10
	return math.Ceil(v/step) * step
}

// niceFloor rounds v down to a multiple of a round step of hi, so the
// axis bottom is a round number below the series.
func niceFloor(v, hi float64) float64 {
	step := math.Pow(10, math.Floor(math.Log10(hi))) / 10
	return math.Floor(v/step) * step
}

// CaughtUp is the empty-all-caught-up piece: a green ring with a check,
// a headline, a detail line and a link-blue next step.
type CaughtUp struct {
	Title  string
	Detail string
	Action string
}

// Render draws the card at width.
func (c CaughtUp) Render(k kit.Kit, width int) []string {
	w := min(width, 60)
	inner := w - 2
	const cx, cy, rx, ry = 13.0, 5.5, 10.0, 4.4
	check := [][2]float64{{-4.5, 0}, {-1.2, 2}, {5, -2.2}}
	grid := make([][]string, 12)
	for y := range grid {
		grid[y] = make([]string, 27)
		for x := range grid[y] {
			dx, dy := (float64(x)-cx)/rx, (float64(y)-cy)/ry
			switch d := math.Hypot(dx, dy); {
			case d >= 0.78 && d <= 1.02:
				grid[y][x] = k.S(kit.OK, "█")
			case d > 1.02 && d <= 1.16:
				grid[y][x] = k.Hex("#2ea043", "▓")
			default:
				grid[y][x] = " "
			}
		}
	}
	for s := range 2 {
		a, b := check[s], check[s+1]
		for i := 0; i <= 24; i++ {
			u := float64(i) / 24
			x := int(math.Round(cx + a[0] + (b[0]-a[0])*u))
			y := int(math.Round(cy + a[1] + (b[1]-a[1])*u))
			if y >= 0 && y < len(grid) && x >= 0 && x+1 < len(grid[y]) {
				grid[y][x], grid[y][x+1] = k.S(kit.FG, "█"), k.S(kit.FG, "█")
			}
		}
	}
	center := func(s string) string {
		return strings.Repeat(" ", max(0, (inner-kit.Width(s))/2)) + s
	}
	var body []string
	if inner >= 29 {
		body = append(body, "")
		for _, row := range grid {
			body = append(body, center(strings.Join(row, "")))
		}
	}
	body = append(body, "", center(k.S(kit.FG, c.Title)))
	for _, l := range kit.Wrap(c.Detail, inner-4) {
		body = append(body, center(k.S(kit.Muted, l)))
	}
	if c.Action != "" {
		body = append(body, "")
		for _, l := range kit.Wrap(c.Action, inner-4) {
			body = append(body, center(k.S(kit.Accent, l)))
		}
	}
	body = append(body, "")
	return kit.Clip(width, k.Draw(kit.Box{Role: kit.Faint, Width: w}, body))
}
