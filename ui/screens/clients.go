package screens

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// ClientRow is one client in the Clients screen.
type ClientRow struct {
	Name     string  // client.id, or "pid 4012" for a producer
	Producer bool    // else a consumer group member
	Where    string  // the member's group, or the topics a producer writes
	Host     string  // a member's host
	Parts    int     // partitions assigned or written
	Rate     float64 // records per second; -1 until a second sample
	Lag      int64   // a member's records behind; -1 unknown
	Idle     time.Duration
	Txn      bool // a producer inside an open transaction
}

// Clients is the process-task-manager piece for clients: the same frame as
// Groups, one row per consumer member or producer with its records/s as
// the piece's CPU bar, and a panel of totals beside the total rate.
type Clients struct {
	Tabs    []string // Rate, Parts, Name
	Sorted  int
	Badge   string
	Rows    []ClientRow
	History []float64 // total records/s per refresh, oldest first
	Empty   string
	Hint    string
}

type clientCol struct {
	name  string
	tab   string
	right bool
	drop  int
	cell  func(ClientRow) (string, kit.Role)
}

const rateBar = 6

// Render draws the screen at width.
func (c Clients) Render(k kit.Kit, width int) []string {
	w := min(width, 110)
	inner := w - 2
	peak := 0.0
	for _, r := range c.Rows {
		peak = max(peak, r.Rate)
	}
	cols, widths := c.fit(inner - 2)
	sortedTab := ""
	if c.Sorted >= 0 && c.Sorted < len(c.Tabs) {
		sortedTab = c.Tabs[c.Sorted]
	}
	var head []string
	for i, col := range cols {
		name, role := col.name, kit.Muted
		if col.tab != "" && col.tab == sortedTab {
			name += map[bool]string{true: " ▲", false: " ▼"}[col.tab == "Name"]
			role = kit.Accent
		}
		head = append(head, k.S(role, pad(name, widths[i], col.right)))
	}
	body := []string{" " + strings.Join(head, "  "), k.S(kit.Rule, strings.Repeat("─", inner))}
	for r, row := range c.Rows {
		var cells []string
		for i, col := range cols {
			if col.name == "" {
				cells = append(cells, c.bar(k, row, peak, r == 0 && sortedTab == "Rate"))
				continue
			}
			text, role := col.cell(row)
			if i == 0 {
				text = kit.Fit(text, widths[i])
			}
			cells = append(cells, k.S(role, pad(text, widths[i], col.right)))
		}
		body = append(body, " "+strings.Join(cells, "  "))
	}
	if len(c.Rows) == 0 {
		for _, l := range kit.Wrap(c.Empty, inner-2) {
			body = append(body, " "+k.S(kit.Muted, l))
		}
	}
	body = append(body, "", k.S(kit.Rule, strings.Repeat("─", inner)))
	body = append(body, c.panel(k, inner-2)...)

	title, badge := tabHeader(k, "Clients", c.Tabs, c.Sorted, c.Badge, w)
	out := k.Draw(kit.Box{Title: title, Right: badge, Role: kit.Rule, Width: w}, body)
	if c.Hint != "" {
		for _, l := range wrapParts(c.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}

func (c Clients) columns() []clientCol {
	return []clientCol{
		{name: "Client", tab: "Name", cell: func(r ClientRow) (string, kit.Role) { return r.Name, kit.FG }},
		{name: "Kind", cell: func(r ClientRow) (string, kit.Role) {
			if r.Producer {
				return "producer", kit.Accent2
			}
			return "consumer", kit.Info
		}},
		{name: "Group / topics", drop: 3, cell: func(r ClientRow) (string, kit.Role) { return orDash(r.Where), kit.Muted }},
		{name: "Host", drop: 5, cell: func(r ClientRow) (string, kit.Role) { return orDash(r.Host), kit.Rule }},
		{name: "Parts", tab: "Parts", right: true, drop: 2, cell: func(r ClientRow) (string, kit.Role) {
			return strconv.Itoa(r.Parts), kit.Muted
		}},
		{name: "Rate", tab: "Rate", right: true, cell: func(r ClientRow) (string, kit.Role) {
			if r.Rate < 0 {
				return "-", kit.Muted
			}
			if r.Rate == 0 {
				return "0/s", kit.Muted
			}
			return Num(r.Rate) + "/s", kit.Accent
		}},
		{name: "", drop: 4, cell: func(ClientRow) (string, kit.Role) { return strings.Repeat(" ", rateBar), kit.FG }},
		{name: "Status", drop: 1, cell: clientStatus},
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// clientStatus is a member's lag, or what a producer is doing: inside a
// transaction, writing, or idle for how long.
func clientStatus(r ClientRow) (string, kit.Role) {
	switch {
	case r.Producer && r.Txn:
		return "in txn", kit.Notice
	case r.Producer && r.Idle < 10*time.Second:
		return "writing", kit.OK
	case r.Producer:
		return "idle " + Span(r.Idle.Seconds()), kit.Rule
	case r.Lag < 0:
		return "-", kit.Muted
	case r.Lag == 0:
		return "caught up", kit.OK
	}
	return Num(float64(r.Lag)) + " behind", kit.Warn
}

// bar is the piece's six-cell CPU meter for records/s: red on the busiest
// row when sorted by rate, blue otherwise, faint dots for the rest.
func (c Clients) bar(k kit.Kit, r ClientRow, peak float64, top bool) string {
	n := 0
	if peak > 0 && r.Rate > 0 {
		n = max(1, int(math.Ceil(rateBar*r.Rate/peak)))
	}
	role := kit.Accent
	if top {
		role = kit.Err
	}
	return k.S(role, strings.Repeat("■", n)) + k.S(kit.Faint, strings.Repeat("·", rateBar-n))
}

// fit picks the columns that fit in w, dropping the highest drop first.
func (c Clients) fit(w int) ([]clientCol, []int) {
	cols := c.columns()
	sortedTab := ""
	if c.Sorted >= 0 && c.Sorted < len(c.Tabs) {
		sortedTab = c.Tabs[c.Sorted]
	}
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = kit.Width(col.name)
		if col.tab != "" && col.tab == sortedTab {
			widths[i] += 2
		}
		for _, r := range c.Rows {
			text, _ := col.cell(r)
			widths[i] = max(widths[i], kit.Width(text))
		}
		if col.name == "Group / topics" {
			widths[i] = min(widths[i], 24)
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
		for i, col := range cols {
			if col.drop > 0 && (drop < 0 || col.drop > cols[drop].drop) {
				drop = i
			}
		}
		if drop < 0 {
			break
		}
		cols = append(cols[:drop:drop], cols[drop+1:]...)
		widths = append(widths[:drop:drop], widths[drop+1:]...)
	}
	widths[0] = max(1, min(widths[0], w-(total()-widths[0])))
	return cols, widths
}

// panel is the bottom strip: totals on the left, total records/s over the
// last refreshes in the middle, and counts on the right.
func (c Clients) panel(k kit.Kit, w int) []string {
	rate := -1.0
	consumers, producers := 0, 0
	groups, hosts := map[string]bool{}, map[string]bool{}
	for _, r := range c.Rows {
		if r.Rate >= 0 {
			rate = max(rate, 0) + r.Rate
		}
		if r.Producer {
			producers++
			continue
		}
		consumers++
		groups[r.Where] = true
		if r.Host != "" {
			hosts[r.Host] = true
		}
	}
	rateText, rateRole := "-", kit.Muted
	if rate >= 0 {
		rateText, rateRole = Num(rate)+"/s", kit.Accent
	}
	left := []string{
		k.S(kit.Muted, "Rate      ") + k.S(rateRole, rateText),
		k.S(kit.Muted, "Consumers ") + k.S(kit.Info, strconv.Itoa(consumers)),
		k.S(kit.Muted, "Producers ") + k.S(kit.Accent2, strconv.Itoa(producers)),
	}
	right := []string{
		k.S(kit.Muted, "Groups ") + k.S(kit.FG, lpad(strconv.Itoa(len(groups)), 5)),
		k.S(kit.Muted, "Hosts  ") + k.S(kit.FG, lpad(strconv.Itoa(len(hosts)), 5)),
		"",
	}
	lw, rw := kit.BlockWidth(left), kit.BlockWidth(right)
	if chartW := w - lw - rw - 6; chartW >= 8 {
		return kit.Indent(1, kit.Beside(3, left, blockChart(k, c.History, chartW, kit.Accent, kit.Accent), right))
	}
	if lw+rw+3 <= w {
		return kit.Indent(1, kit.Beside(3, left, right))
	}
	return kit.Indent(1, append(left, right[:2]...))
}

// ProducerName is how a producer without a client.id shows: its ID.
func ProducerName(id string) string { return fmt.Sprintf("pid %s", id) }
