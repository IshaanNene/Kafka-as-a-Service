package screens

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// GroupRow is one consumer group in the Groups screen.
type GroupRow struct {
	Name    string
	State   string // Stable, Empty, PreparingRebalance, CompletingRebalance, Dead
	Members int
	Topics  []string
	Lag     int64   // records behind; -1 unknown
	Rate    float64 // records consumed per second; -1 until a second sample
	Err     bool    // the broker would not describe it
}

// Groups is the process-task-manager piece for consumer groups: an
// "Activity"-style frame with sort tabs, one row per group with its lag as a
// bar, and a bottom panel of totals beside a chart of total lag over time.
type Groups struct {
	Tabs    []string // Lag, Rate, Members, Name
	Sorted  int      // index into Tabs, which is also the sorted column
	Badge   string   // drawn green after ●
	Rows    []GroupRow
	History []int64 // total lag per refresh, oldest first
	Empty   string
	Hint    string
}

// groupCol is one column the screen may show; drop as in Column.
type groupCol struct {
	name  string
	tab   string // the tab that sorts by it
	right bool
	drop  int
	cell  func(GroupRow) (string, kit.Role)
}

const lagBar = 6

// Render draws the screen at width.
func (g Groups) Render(k kit.Kit, width int) []string {
	w := min(width, 100)
	inner := w - 2
	var peak int64
	for _, r := range g.Rows {
		peak = max(peak, r.Lag)
	}
	cols, widths := g.fit(inner-2, peak)
	sortedTab := ""
	if g.Sorted >= 0 && g.Sorted < len(g.Tabs) {
		sortedTab = g.Tabs[g.Sorted]
	}

	var head []string
	for i, c := range cols {
		name, role := c.name, kit.Muted
		if c.tab != "" && c.tab == sortedTab {
			name += map[bool]string{true: " ▲", false: " ▼"}[c.tab == "Name"]
			role = kit.Accent
		}
		head = append(head, k.S(role, pad(name, widths[i], c.right)))
	}
	body := []string{" " + strings.Join(head, "  "), k.S(kit.Rule, strings.Repeat("─", inner))}
	for r, row := range g.Rows {
		var cells []string
		for i, c := range cols {
			text, role := c.cell(row)
			if c.name == "" {
				cells = append(cells, g.bar(k, row, peak, r == 0))
				continue
			}
			if i == 0 {
				text = kit.Fit(text, widths[i])
			}
			cells = append(cells, k.S(role, pad(text, widths[i], c.right)))
		}
		body = append(body, " "+strings.Join(cells, "  "))
	}
	if len(g.Rows) == 0 {
		for _, l := range kit.Wrap(g.Empty, inner-2) {
			body = append(body, " "+k.S(kit.Muted, l))
		}
	}
	body = append(body, "", k.S(kit.Rule, strings.Repeat("─", inner)))
	body = append(body, g.panel(k, inner-2)...)

	title, badge := g.header(k, w)
	out := k.Draw(kit.Box{Title: title, Right: badge, Role: kit.Rule, Width: w}, body)
	if g.Hint != "" {
		for _, l := range wrapParts(g.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}

func pad(s string, w int, right bool) string {
	if right {
		return lpad(s, w)
	}
	return kit.Fit(s, w)
}

// header is the frame's title and badge: "Groups" and the sort tabs, the
// active one blue, as the piece's "Activity  CPU ─ Memory ─ Energy".
func (g Groups) header(k kit.Kit, w int) (title, badge string) {
	return tabHeader(k, "Groups", g.Tabs, g.Sorted, g.Badge, w)
}

// tabHeader is a process-task-manager title: name, then the sort tabs with
// the active one blue, and a green badge. When the frame is too narrow the
// badge shrinks to its dot, then the tabs shrink to the active one.
func tabHeader(k kit.Kit, name string, tabs []string, sorted int, badgeText string, w int) (title, badge string) {
	fits := func(title, badge string) bool { return kit.Width(title)+kit.Width(badge)+8 <= w }
	var parts []string
	active := k.S(kit.FG, name)
	for i, t := range tabs {
		role := kit.Rule
		if i == sorted {
			role = kit.Accent
			active += "  " + k.S(kit.Accent, t)
		}
		parts = append(parts, k.S(role, t))
	}
	full := k.S(kit.FG, name) + "  " + strings.Join(parts, k.S(kit.Rule, " ─ "))
	if len(tabs) == 0 {
		full = active
	}
	dot := ""
	if badgeText != "" {
		badge, dot = k.S(kit.OK, "● "+badgeText), k.S(kit.OK, "●")
	}
	for _, c := range [][2]string{{full, badge}, {full, dot}, {active, badge}, {active, dot}} {
		if fits(c[0], c[1]) {
			return c[0], c[1]
		}
	}
	return active, ""
}

func (g Groups) columns(peak int64) []groupCol {
	return []groupCol{
		{name: "Group", tab: "Name", cell: func(r GroupRow) (string, kit.Role) { return r.Name, kit.FG }},
		{name: "State", cell: stateCell},
		{name: "Members", tab: "Members", right: true, drop: 2, cell: func(r GroupRow) (string, kit.Role) {
			return strconv.Itoa(r.Members), kit.Muted
		}},
		{name: "Topics", drop: 5, cell: func(r GroupRow) (string, kit.Role) {
			switch len(r.Topics) {
			case 0:
				return "-", kit.Muted
			case 1:
				return r.Topics[0], kit.Muted
			}
			return fmt.Sprintf("%s +%d", r.Topics[0], len(r.Topics)-1), kit.Muted
		}},
		{name: "Lag", tab: "Lag", right: true, cell: func(r GroupRow) (string, kit.Role) {
			if r.Lag < 0 {
				return "-", kit.Muted
			}
			return Num(float64(r.Lag)), lagRole(r.Lag, peak)
		}},
		{name: "", drop: 3, cell: func(GroupRow) (string, kit.Role) { return strings.Repeat(" ", lagBar), kit.FG }},
		{name: "Rate", tab: "Rate", right: true, drop: 1, cell: func(r GroupRow) (string, kit.Role) {
			if r.Rate < 0 {
				return "-", kit.Muted
			}
			role := kit.Muted
			if r.Rate > 0 {
				role = kit.Accent
			}
			return Num(r.Rate) + "/s", role
		}},
		{name: "Catch up", right: true, drop: 4, cell: catchUp},
	}
}

func stateCell(r GroupRow) (string, kit.Role) {
	switch {
	case r.Err:
		return "error", kit.Err
	case r.State == "Stable":
		return "Stable", kit.Muted
	case r.State == "Dead":
		return "Dead", kit.Err
	case strings.HasSuffix(r.State, "Rebalance"):
		return "Rebalancing", kit.Accent2
	case r.State == "":
		return "-", kit.Muted
	}
	return r.State, kit.Rule
}

// lagRole colours a group's lag by its share of the worst: the top
// offenders red, the middle orange, caught-up groups green.
func lagRole(lag, peak int64) kit.Role {
	switch {
	case lag == 0:
		return kit.OK
	case peak > 0 && float64(lag) >= 0.5*float64(peak):
		return kit.Err
	case peak > 0 && float64(lag) >= 0.15*float64(peak):
		return kit.Warn
	}
	return kit.FG
}

// catchUp is how long the group needs to clear its lag at its current rate:
// "stalled" when its members are not committing, "no members" when nobody
// is consuming at all.
func catchUp(r GroupRow) (string, kit.Role) {
	switch {
	case r.Lag < 0 || r.Rate < 0:
		return "-", kit.Muted
	case r.Lag == 0:
		return "caught up", kit.OK
	case r.Rate == 0 && r.Members == 0:
		return "no members", kit.Warn
	case r.Rate == 0:
		return "stalled", kit.Warn
	}
	return Span(float64(r.Lag) / r.Rate), kit.FG
}

// Span prints seconds as the coarsest unit that keeps it short: 40s, 12m,
// 3h, 2d.
func Span(sec float64) string {
	switch {
	case sec < 1:
		return "<1s"
	case sec < 60:
		return fmt.Sprintf("%.0fs", sec)
	case sec < 3600:
		return fmt.Sprintf("%.0fm", sec/60)
	case sec < 86400:
		return fmt.Sprintf("%.0fh", sec/3600)
	}
	return fmt.Sprintf("%.0fd", sec/86400)
}

// bar is the piece's six-cell CPU meter: ■ for the group's share of the
// worst lag, red on the top row, blue otherwise, faint dots for the rest.
func (g Groups) bar(k kit.Kit, r GroupRow, peak int64, top bool) string {
	n := 0
	if peak > 0 && r.Lag > 0 {
		n = max(1, int(math.Ceil(lagBar*float64(r.Lag)/float64(peak))))
	}
	role := kit.Accent
	if top && g.Sorted >= 0 && g.Sorted < len(g.Tabs) && g.Tabs[g.Sorted] == "Lag" {
		role = kit.Err
	}
	return k.S(role, strings.Repeat("■", n)) + k.S(kit.Faint, strings.Repeat("·", lagBar-n))
}

// fit picks the columns that fit in w, dropping the highest Drop first.
func (g Groups) fit(w int, peak int64) ([]groupCol, []int) {
	cols := g.columns(peak)
	sortedTab := ""
	if g.Sorted >= 0 && g.Sorted < len(g.Tabs) {
		sortedTab = g.Tabs[g.Sorted]
	}
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = kit.Width(c.name)
		if c.tab != "" && c.tab == sortedTab {
			widths[i] += 2
		}
		for _, r := range g.Rows {
			text, _ := c.cell(r)
			widths[i] = max(widths[i], kit.Width(text))
		}
	}
	total := func() int {
		n := -2
		for _, wd := range widths {
			n += wd + 2
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
	widths[0] = max(1, min(widths[0], w-(total()-widths[0])))
	return cols, widths
}

// panel is the piece's bottom strip: totals on the left, the lag history
// chart in the middle, and counts on the right; narrow frames stack them.
func (g Groups) panel(k kit.Kit, w int) []string {
	var lag int64 = -1
	var rate float64 = -1
	members, rebalancing, failed := 0, 0, 0
	topics := map[string]bool{}
	for _, r := range g.Rows {
		if r.Lag >= 0 {
			lag = max(lag, 0) + r.Lag
		}
		if r.Rate >= 0 {
			rate = max(rate, 0) + r.Rate
		}
		members += r.Members
		if strings.HasSuffix(r.State, "Rebalance") {
			rebalancing++
		}
		if r.Err {
			failed++
		}
		for _, t := range r.Topics {
			topics[t] = true
		}
	}
	lagText, lagRole := "-", kit.Muted
	if lag >= 0 {
		lagText, lagRole = Num(float64(lag)), kit.FG
		if n := len(g.History); n >= 2 && g.History[n-1] > g.History[n-2] {
			lagRole = kit.Err
		} else if lag == 0 {
			lagRole = kit.OK
		}
	}
	rateText, rateRole := "-", kit.Muted
	if rate >= 0 {
		rateText, rateRole = Num(rate)+"/s", kit.Accent
	}
	groups := k.S(kit.FG, strconv.Itoa(len(g.Rows)))
	if rebalancing > 0 {
		groups += k.S(kit.Rule, " · ") + k.S(kit.Accent2, strconv.Itoa(rebalancing)+" rebalancing")
	}
	if failed > 0 {
		groups += k.S(kit.Rule, " · ") + k.S(kit.Err, strconv.Itoa(failed)+" failed")
	}
	left := []string{
		k.S(kit.Muted, "Lag     ") + k.S(lagRole, lagText),
		k.S(kit.Muted, "Rate    ") + k.S(rateRole, rateText),
		k.S(kit.Muted, "Groups  ") + groups,
	}
	right := []string{
		k.S(kit.Muted, "Members ") + k.S(kit.FG, lpad(strconv.Itoa(members), 5)),
		k.S(kit.Muted, "Topics  ") + k.S(kit.FG, lpad(strconv.Itoa(len(topics)), 5)),
		"",
	}
	lw, rw := kit.BlockWidth(left), kit.BlockWidth(right)
	if chartW := w - lw - rw - 6; chartW >= 8 {
		return kit.Indent(1, kit.Beside(3, left, g.chart(k, chartW), right))
	}
	if lw+rw+3 <= w {
		return kit.Indent(1, kit.Beside(3, left, right))
	}
	return kit.Indent(1, append(left, right[:2]...))
}

// chart draws total lag over the last refreshes: blue, red where lag grew
// since the refresh before.
func (g Groups) chart(k kit.Kit, w int) []string {
	h := make([]float64, len(g.History))
	for i, v := range g.History {
		h[i] = float64(v)
	}
	return blockChart(k, h, w, kit.Accent, kit.Err)
}

// blockChart draws a series three rows tall in eighth blocks, in role, or
// rise where a value grew on the one before. A short history is widened,
// up to four columns a point, so the chart fills its space from the start;
// the newest point is on the right.
func blockChart(k kit.Kit, h []float64, w int, role, rise kit.Role) []string {
	if len(h) > w {
		h = h[len(h)-w:]
	}
	per := 1
	if len(h) > 0 {
		per = max(1, min(4, w/len(h)))
	}
	var peak float64
	for _, v := range h {
		peak = max(peak, v)
	}
	const rows, steps = 3, 8
	levels := []rune(" ▁▂▃▄▅▆▇█")
	out := make([]string, rows)
	for i, v := range h {
		r0 := role
		if i > 0 && v > h[i-1] {
			r0 = rise
		}
		filled := 0
		if peak > 0 {
			filled = int(math.Round(float64(rows*steps) * v / peak))
		}
		for r := range rows {
			level := min(max(filled-(rows-1-r)*steps, 0), steps)
			switch {
			case level > 0:
				out[r] += k.S(r0, strings.Repeat(string(levels[level]), per))
			case r == rows-1:
				out[r] += k.S(kit.Faint, strings.Repeat("▁", per))
			default:
				out[r] += strings.Repeat(" ", per)
			}
		}
	}
	for r := range out {
		out[r] = lpad(out[r], w)
	}
	return out
}
