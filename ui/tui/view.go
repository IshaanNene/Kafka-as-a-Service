package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/asciipack"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

type styles struct {
	fg, muted, accent, accent2, ok, warn, err, info, rule lipgloss.Style
	sel                                                   lipgloss.Style
}

func newStyles(t theme.Theme) styles {
	c := func(hex string) lipgloss.Style { return lipgloss.NewStyle().Foreground(theme.C(hex)) }
	return styles{
		fg: c(t.FG), muted: c(t.Muted), accent: c(t.Accent), accent2: c(t.Accent2),
		ok: c(t.OK), warn: c(t.Warn), err: c(t.Err), info: c(t.Info), rule: c(t.Rule),
		sel: lipgloss.NewStyle().Foreground(theme.C(t.FG)).Background(theme.C(t.Selection)),
	}
}

// staleAfter is when the header starts badging data as old: two metadata
// intervals at the engine's default.
const staleAfter = 10 * time.Second

func (m Model) View() tea.View {
	w, h := m.w, m.h
	if w == 0 || h == 0 {
		w, h = 100, 30
	}
	var body string
	switch {
	case m.help:
		body = m.viewHelp()
	case m.screen() == screenNotFound:
		body = m.viewNotFound(w, h-6)
	case m.snap == nil:
		body = m.st.muted.Render("  connecting · waiting for the first snapshot …")
		if err := m.lastError(); err != nil {
			body += "\n\n  " + m.st.err.Render("● "+err.Error()) + "\n  " + m.st.muted.Render("retrying on the metadata interval")
		}
	default:
		rows := h - 6 - m.feedHeight()
		switch m.screen() {
		case screenTopics:
			body = m.viewTopics(w, rows)
		case screenTopic:
			body = m.viewTopic(w, rows)
		case screenBrokers:
			body = m.viewBrokers(w, rows)
		}
	}

	parts := []string{m.viewHeader(w), m.ruleLine(w), body}
	used := lipgloss.Height(strings.Join(parts, "\n"))
	feed := ""
	if m.screen() != screenNotFound && !m.help {
		feed = m.viewFeed(w)
	}
	footer := m.viewFooter(w)
	pad := h - used - lipgloss.Height(feed) - 2
	if feed == "" {
		pad = h - used - 2
	}
	if pad > 0 {
		parts = append(parts, strings.Repeat("\n", pad-1))
	}
	if feed != "" {
		parts = append(parts, feed)
	}
	parts = append(parts, m.ruleLine(w), footer)

	// Last line of defence for narrow terminals: no screen may draw past
	// the right edge, whatever its minimum column widths.
	lines := strings.Split(strings.Join(parts, "\n"), "\n")
	for i, l := range lines {
		lines[i] = ansi.Truncate(l, w, "…")
	}
	v := tea.NewView(strings.Join(lines, "\n"))
	v.AltScreen = true
	v.BackgroundColor = theme.C(m.th.BG)
	v.WindowTitle = "kaas"
	return v
}

func (m Model) ruleLine(w int) string {
	return m.st.rule.Render(strings.Repeat("· ", w/2))
}

func (m Model) viewHeader(w int) string {
	title := m.st.accent.Bold(true).Render("K A A S")
	crumb := m.st.muted.Render("  ·  ") + m.st.accent2.Render(m.breadcrumb())
	left := title + crumb
	right := ""
	if s := m.snap; s != nil {
		age := m.now.Sub(s.TakenAt).Round(time.Second)
		fresh := m.st.ok.Render("●")
		if age > staleAfter {
			fresh = m.st.warn.Render("● stale " + age.String())
			if err := m.lastError(); err != nil {
				fresh = m.st.err.Render("● stale " + age.String() + " · " + err.Error())
			}
		}
		right = m.st.muted.Render(fmt.Sprintf("%s · %d brokers · ctrl %d · gen %d ",
			short(s.ClusterID, 12), len(s.Brokers), s.Controller, s.Generation)) + fresh
		// Narrow terminals keep only what tells you whether to trust the screen.
		if ansi.StringWidth(left)+ansi.StringWidth(right)+1 > w {
			right = m.st.muted.Render(fmt.Sprintf("gen %d ", s.Generation)) + fresh
		}
	}
	gap := max(1, w-ansi.StringWidth(left)-ansi.StringWidth(right))
	return ansi.Truncate(left+strings.Repeat(" ", gap)+right, w, "…")
}

// lastError is why the engine's latest refresh failed, if it did.
func (m Model) lastError() error {
	if m.eng == nil {
		return nil
	}
	return m.eng.Stats().LastError
}

func (m Model) breadcrumb() string {
	switch m.screen() {
	case screenTopic:
		return "topics › " + m.topic
	case screenBrokers:
		return "brokers"
	case screenNotFound:
		return "not found"
	}
	return "topics"
}

// column describes one table column; the flex column absorbs spare width.
type column struct {
	title string
	width int
	right bool
	flex  bool
}

func (m Model) table(w, rows int, cols []column, cells [][]string, styleCell func(r, c int, s string) string) string {
	fixed := 0
	for _, c := range cols {
		fixed += c.width + 2
	}
	for i := range cols {
		if cols[i].flex {
			cols[i].width = max(cols[i].width, w-fixed+cols[i].width-2)
		}
	}
	pad := func(s string, c column) string {
		s = ansi.Truncate(s, c.width, "…")
		gap := strings.Repeat(" ", max(0, c.width-ansi.StringWidth(s)))
		if c.right {
			return gap + s
		}
		return s + gap
	}
	var b strings.Builder
	b.WriteString("  ")
	for _, c := range cols {
		b.WriteString(m.st.muted.Render(pad(strings.ToUpper(c.title), c)) + "  ")
	}
	cur := m.cursor[m.screen()]
	top := 0
	if rows > 1 && cur >= rows-1 {
		top = cur - (rows - 2)
	}
	for r := top; r < len(cells) && r < top+rows-1; r++ {
		b.WriteString("\n")
		var line strings.Builder
		for ci, c := range cols {
			line.WriteString(pad(cells[r][ci], c) + "  ")
		}
		if r == cur {
			b.WriteString(m.st.sel.Render("▸ " + line.String()))
			continue
		}
		b.WriteString("  ")
		for ci, c := range cols {
			b.WriteString(styleCell(r, ci, pad(cells[r][ci], c)) + "  ")
		}
	}
	if len(cells) == 0 {
		b.WriteString("\n" + m.st.muted.Render("  nothing here"))
	}
	return b.String()
}

func (m Model) viewTopics(w, rows int) string {
	ts := m.visibleTopics()
	cols := []column{
		{title: "topic", width: 20, flex: true},
		{title: "parts", width: 5, right: true},
		{title: "urp", width: 4, right: true},
		{title: "records", width: 9, right: true},
		{title: "offsets age", width: 11, right: true},
	}
	cells := make([][]string, len(ts))
	for i, t := range ts {
		urp := 0
		for _, p := range t.Partitions {
			if p.UnderReplicated() || p.Leader < 0 {
				urp++
			}
		}
		recs, age := "—", "—"
		if n, ok := retained(t); ok {
			recs = human(n)
		}
		if !t.OffsetsAt.IsZero() {
			age = m.now.Sub(t.OffsetsAt).Round(time.Second).String()
		}
		cells[i] = []string{t.Name, strconv.Itoa(len(t.Partitions)), strconv.Itoa(urp), recs, age}
	}
	head := ""
	if m.filtering || m.filter != "" {
		cursor := ""
		if m.filtering {
			cursor = m.st.accent.Render("▏")
		}
		head = m.st.accent.Render("  / ") + m.st.fg.Render(m.filter) + cursor +
			m.st.muted.Render(fmt.Sprintf("   %d of %d", len(ts), len(m.snap.Topics))) + "\n"
		rows--
	}
	return head + m.table(w, rows, cols, cells, func(r, c int, s string) string {
		switch c {
		case 0:
			return m.st.fg.Render(s)
		case 2:
			if strings.TrimSpace(s) != "0" {
				return m.st.err.Render(s)
			}
			return m.st.muted.Render(s)
		case 3:
			return m.st.info.Render(s)
		}
		return m.st.muted.Render(s)
	})
}

func (m Model) viewTopic(w, rows int) string {
	t, ok := m.snap.Topic(m.topic)
	if !ok {
		return m.st.err.Render("  topic " + m.topic + " no longer exists")
	}
	cols := []column{
		{title: "", width: 1},
		{title: "part", width: 4, right: true},
		{title: "leader", width: 6, right: true},
		{title: "replicas", width: 10, flex: true},
		{title: "isr", width: 10},
		{title: "start", width: 10, right: true},
		{title: "end", width: 10, right: true},
	}
	cells := make([][]string, len(t.Partitions))
	for i, p := range t.Partitions {
		glyph := "✓"
		switch {
		case p.Leader < 0:
			glyph = "✗"
		case p.UnderReplicated():
			glyph = "!"
		}
		cells[i] = []string{glyph, strconv.Itoa(int(p.ID)), strconv.Itoa(int(p.Leader)),
			ids(p.Replicas), ids(p.ISR), offset(p.StartOffset), offset(p.EndOffset)}
	}
	return m.table(w, rows, cols, cells, func(r, c int, s string) string {
		switch c {
		case 0:
			switch strings.TrimSpace(s) {
			case "✗":
				return m.st.err.Render(s)
			case "!":
				return m.st.warn.Render(s)
			}
			return m.st.ok.Render(s)
		case 5, 6:
			return m.st.info.Render(s)
		}
		return m.st.fg.Render(s)
	})
}

func (m Model) viewBrokers(w, rows int) string {
	leads := map[int32]int{}
	for _, t := range m.snap.Topics {
		for _, p := range t.Partitions {
			leads[p.Leader]++
		}
	}
	cols := []column{
		{title: "id", width: 4, right: true},
		{title: "address", width: 24, flex: true},
		{title: "rack", width: 10},
		{title: "leads", width: 7, right: true},
		{title: "role", width: 10},
	}
	cells := make([][]string, len(m.snap.Brokers))
	for i, b := range m.snap.Brokers {
		role := ""
		if b.ID == m.snap.Controller {
			role = "controller"
		}
		rack := b.Rack
		if rack == "" {
			rack = "—"
		}
		cells[i] = []string{strconv.Itoa(int(b.ID)), fmt.Sprintf("%s:%d", b.Host, b.Port), rack, strconv.Itoa(leads[b.ID]), role}
	}
	return m.table(w, rows, cols, cells, func(_, c int, s string) string {
		if c == 4 {
			return m.st.accent.Render(s)
		}
		return m.st.fg.Render(s)
	})
}

func (m Model) feedHeight() int {
	if len(m.feed) == 0 {
		return 0
	}
	return len(m.feed) + 1
}

func (m Model) viewFeed(w int) string {
	if len(m.feed) == 0 {
		return ""
	}
	lines := []string{m.st.muted.Render("  E V E N T S")}
	for _, f := range m.feed {
		style := m.st.info
		switch f.ev.Kind {
		case engine.PartitionOffline, engine.ISRShrunk, engine.BrokerRemoved, engine.TopicDeleted:
			style = m.st.warn
		case engine.TopicCreated, engine.ISRExpanded, engine.BrokerAdded:
			style = m.st.ok
		}
		lines = append(lines, ansi.Truncate(
			m.st.muted.Render("  "+f.at.Format("15:04:05")+"  ")+style.Render(f.ev.String()), w, "…"))
	}
	return strings.Join(lines, "\n")
}

func (m Model) viewFooter(w int) string {
	if m.commandOn {
		return m.st.accent.Render(" : ") + m.st.fg.Render(m.command) + m.st.accent.Render("▏") +
			m.st.muted.Render("   topics · brokers · topic <name> · quit")
	}
	hints := [][2]string{{"q", "quit"}, {":", "command"}, {"/", "filter"}, {"enter", "open"}, {"esc", "back"}, {"?", "help"}}
	var parts []string
	for _, h := range hints {
		parts = append(parts, m.st.accent.Render(h[0])+" "+m.st.muted.Render(h[1]))
	}
	line := " " + strings.Join(parts, m.st.muted.Render(" · "))
	if m.debug && m.eng != nil {
		st := m.eng.Stats()
		dbg := m.st.muted.Render(fmt.Sprintf("   budget %.1f · %d granted · %d denied", st.Budget.Tokens, st.Budget.Granted, st.Budget.Denied))
		line += dbg
	}
	return ansi.Truncate(line, w, "…")
}

func (m Model) viewHelp() string {
	rows := [][2]string{
		{":topics", "list topics"}, {":brokers", "list brokers"}, {":topic <name>", "open a topic (or just :<name>)"},
		{"/", "filter the list; start with _ to include internal topics"}, {"enter", "open the selected topic"},
		{"esc", "clear filter, then go back"}, {"j k ↑ ↓ g G", "move"}, {"ctrl+d", "debug overlay: request budget"},
		{"q", "quit"},
	}
	var b strings.Builder
	b.WriteString(m.st.accent.Render("  H E L P") + "\n\n")
	for _, r := range rows {
		b.WriteString("  " + m.st.accent2.Render(fmt.Sprintf("%-16s", r[0])) + m.st.fg.Render(r[1]) + "\n")
	}
	b.WriteString("\n" + m.st.muted.Render("  CLI twins: kaas snapshot · kaas watch -t <topic>    any key closes"))
	return b.String()
}

func (m Model) viewNotFound(w, h int) string {
	var art string
	if m.art != nil {
		elapsed := m.now.Sub(m.nf.since)
		if m.reduced {
			elapsed = 0
		}
		artH := max(0, h-5)
		art = m.art.Render(m.art.FrameAt(elapsed), asciipack.RenderOptions{Width: w, Height: artH, Recolor: m.th.Tint, Default: m.th.FG})
	}
	msg := m.st.fg.Render(fmt.Sprintf("No %s named ", m.nf.what)) + m.st.accent.Render(m.nf.name) + m.st.fg.Render(".")
	hint := m.st.muted.Render("esc to go back")
	if len(m.nf.suggestions) > 0 {
		var s []string
		for _, x := range m.nf.suggestions {
			s = append(s, m.st.accent2.Render(x))
		}
		hint = m.st.muted.Render("Did you mean ") + strings.Join(s, m.st.muted.Render(", ")) + m.st.muted.Render("?   esc to go back")
	}
	return lipgloss.JoinVertical(lipgloss.Center, art, "", msg, hint)
}

func retained(t snapshot.Topic) (int64, bool) {
	if t.OffsetsAt.IsZero() {
		return 0, false
	}
	var n int64
	for _, p := range t.Partitions {
		if p.StartOffset < 0 || p.EndOffset < 0 {
			return 0, false
		}
		n += p.EndOffset - p.StartOffset
	}
	return n, true
}

func human(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 10_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func ids(xs []int32) string {
	s := make([]string, len(xs))
	for i, x := range xs {
		s[i] = strconv.Itoa(int(x))
	}
	return strings.Join(s, ",")
}

func offset(o int64) string {
	if o < 0 {
		return "—"
	}
	return strconv.FormatInt(o, 10)
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
