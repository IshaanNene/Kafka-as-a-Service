package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/IshaanNene/Kafka-as-a-Service/core/produce"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// buffer is one editor tab's text and cursor; col counts runes.
type buffer struct {
	lines     []string
	line, col int
	dirty     bool
}

func newBuffer(text string) buffer { return buffer{lines: strings.Split(text, "\n")} }

func (b buffer) text() string { return strings.Join(b.lines, "\n") }

func (b *buffer) insert(s string) {
	for i, part := range strings.Split(s, "\n") {
		if i > 0 {
			b.split("")
		}
		rs := []rune(b.lines[b.line])
		b.lines[b.line] = string(rs[:b.col]) + part + string(rs[b.col:])
		b.col += len([]rune(part))
	}
	b.dirty = true
}

// split breaks the line at the cursor, starting the new line with indent.
func (b *buffer) split(indent string) {
	rs := []rune(b.lines[b.line])
	rest := indent + string(rs[b.col:])
	b.lines[b.line] = string(rs[:b.col])
	b.lines = append(b.lines[:b.line+1], append([]string{rest}, b.lines[b.line+1:]...)...)
	b.line, b.col = b.line+1, len([]rune(indent))
	b.dirty = true
}

// newline keeps the line's indent, adds two spaces after an opening { or
// [, and moves a closing bracket right after the cursor to its own line.
func (b *buffer) newline() {
	rs := []rune(b.lines[b.line])
	indent := 0
	for indent < len(rs) && rs[indent] == ' ' {
		indent++
	}
	base := strings.Repeat(" ", indent)
	opens := b.col > 0 && (rs[b.col-1] == '{' || rs[b.col-1] == '[')
	closes := b.col < len(rs) && (rs[b.col] == '}' || rs[b.col] == ']')
	switch {
	case opens && closes:
		// {|} becomes the bracket on its own line, the cursor indented
		// on the line between.
		b.split(base + "  ")
		b.split(base)
		b.line, b.col = b.line-1, len(base)+2
	case opens:
		b.split(base + "  ")
	default:
		b.split(base)
	}
}

// closeBracket types } or ], first taking back one indent level when the
// line is only indentation so far, as editors do.
func (b *buffer) closeBracket(s string) {
	before := string([]rune(b.lines[b.line])[:b.col])
	if b.col >= 2 && strings.TrimLeft(before, " ") == "" {
		rs := []rune(b.lines[b.line])
		b.lines[b.line] = string(rs[2:])
		b.col -= 2
	}
	b.insert(s)
}

func (b *buffer) backspace() {
	switch {
	case b.col > 0:
		rs := []rune(b.lines[b.line])
		b.lines[b.line] = string(rs[:b.col-1]) + string(rs[b.col:])
		b.col--
	case b.line > 0:
		prev := []rune(b.lines[b.line-1])
		b.lines[b.line-1] = string(prev) + b.lines[b.line]
		b.lines = append(b.lines[:b.line], b.lines[b.line+1:]...)
		b.line, b.col = b.line-1, len(prev)
	default:
		return
	}
	b.dirty = true
}

func (b *buffer) del() {
	rs := []rune(b.lines[b.line])
	switch {
	case b.col < len(rs):
		b.lines[b.line] = string(rs[:b.col]) + string(rs[b.col+1:])
	case b.line < len(b.lines)-1:
		b.lines[b.line] += b.lines[b.line+1]
		b.lines = append(b.lines[:b.line+1], b.lines[b.line+2:]...)
	default:
		return
	}
	b.dirty = true
}

func (b *buffer) move(dl, dc int) {
	if dc != 0 {
		b.col += dc
		switch n := len([]rune(b.lines[b.line])); {
		case b.col < 0 && b.line > 0:
			b.line--
			b.col = len([]rune(b.lines[b.line]))
		case b.col > n && b.line < len(b.lines)-1:
			b.line, b.col = b.line+1, 0
		}
	}
	b.line = min(max(0, b.line+dl), len(b.lines)-1)
	b.col = min(max(0, b.col), len([]rune(b.lines[b.line])))
}

// Editor tabs, in order.
const (
	tabValue = iota
	tabKey
	tabHeaders
)

var editorTabs = []string{"value", "key", "headers"}

// editorModel is kaas produce's editor.
type editorModel struct {
	p      producer
	k      kit.Kit
	bg     string
	w, h   int
	bufs   [3]buffer
	active int
	notice string
	busy   bool
	sent   []string // "orders/2 @ 1043", printed on exit
	now    func() time.Time
}

// sentMsg is a send's outcome.
type sentMsg struct {
	res produce.Result
	err error
}

func newEditor(p producer, k kit.Kit) editorModel {
	m := editorModel{p: p, k: k, now: time.Now}
	m.bufs[tabValue] = newBuffer("")
	m.bufs[tabKey] = newBuffer("")
	var hs []string
	for _, h := range p.headers {
		hs = append(hs, h.Key+"="+h.Value)
	}
	m.bufs[tabHeaders] = newBuffer(strings.Join(hs, "\n"))
	return m
}

func (m editorModel) Init() tea.Cmd { return nil }

func (m editorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.PasteMsg:
		m.bufs[m.active].insert(strings.ReplaceAll(msg.Content, "\r\n", "\n"))
	case sentMsg:
		m.busy = false
		if msg.err != nil {
			m.notice = "✗ " + msg.err.Error()
			break
		}
		where := fmt.Sprintf("%s/%d @ %d", m.p.topic, msg.res.Partition, msg.res.Offset)
		m.sent = append(m.sent, "sent to "+where)
		m.notice = fmt.Sprintf("↑ sent to p%d @ %s", msg.res.Partition, screens.Num(float64(msg.res.Offset)))
		for i := range m.bufs {
			m.bufs[i].dirty = false
		}
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m editorModel) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	b := &m.bufs[m.active]
	switch k.String() {
	case "ctrl+c", "esc":
		return m, tea.Quit
	case "ctrl+s":
		return m.send()
	case "tab":
		m.active = (m.active + 1) % len(m.bufs)
	case "shift+tab":
		m.active = (m.active + len(m.bufs) - 1) % len(m.bufs)
	case "enter":
		if m.active == tabKey {
			return m, nil // a key is one line
		}
		b.newline()
	case "backspace":
		b.backspace()
	case "delete":
		b.del()
	case "left":
		b.move(0, -1)
	case "right":
		b.move(0, 1)
	case "up":
		b.move(-1, 0)
	case "down":
		b.move(1, 0)
	case "home", "ctrl+a":
		b.col = 0
	case "end", "ctrl+e":
		b.move(0, len([]rune(b.lines[b.line]))-b.col)
	default:
		switch {
		case k.Text == "}" || k.Text == "]":
			b.closeBracket(k.Text)
		case k.Text != "":
			b.insert(k.Text)
		}
	}
	// An edit answers a refused send; the last good send stays in view.
	if b.dirty && strings.HasPrefix(m.notice, "✗") {
		m.notice = ""
	}
	return m, nil
}

// record is the buffers as a record, or what stops them being one.
func (m editorModel) record() (produce.Record, string) {
	r := produce.Record{Partition: m.p.partition}
	value := m.bufs[tabValue].text()
	if produce.LooksJSON(value) {
		if p := produce.CheckJSON(value); p != nil {
			return r, fmt.Sprintf("value line %d, col %d: %s", p.Line, p.Col, p.Msg)
		}
		r.Value = produce.Compact(value)
	} else {
		r.Value = []byte(value)
	}
	if key := m.bufs[tabKey].text(); key != "" {
		r.Key = []byte(key)
	}
	hs, line, err := produce.ParseHeaders(m.bufs[tabHeaders].text())
	if err != nil {
		return r, fmt.Sprintf("headers line %d: want key=value", line)
	}
	r.Headers = hs
	return r, ""
}

func (m editorModel) send() (tea.Model, tea.Cmd) {
	if m.busy {
		return m, nil
	}
	r, problem := m.record()
	if problem != "" {
		m.notice = "✗ fix " + problem + " first"
		return m, nil
	}
	m.busy, m.notice = true, "sending…"
	p := m.p
	return m, func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res := produce.Send(ctx, p.cl, p.topic, []produce.Record{r})[0]
		return sentMsg{res: res, err: res.Err}
	}
}

// screen is the editor as the code-editor piece draws it.
func (m editorModel) screen() screens.Editor {
	b := m.bufs[m.active]
	e := screens.Editor{
		Title:  m.p.topic + " · kaas produce",
		Active: m.active, Path: m.p.topic + " / " + editorTabs[m.active],
		Lines: b.lines, Line: b.line, Col: b.col, Cursor: true, Branch: m.p.topic,
		Notice: m.notice,
		Hint:   "ctrl-s send · tab next field · ctrl-c quit",
	}
	if m.p.partition >= 0 {
		e.Branch += fmt.Sprintf(" · p%d", m.p.partition)
	}
	for i, name := range editorTabs {
		e.Tabs = append(e.Tabs, screens.EditorTab{Name: name, Dirty: m.bufs[i].dirty})
	}
	switch m.active {
	case tabValue:
		e.Lang = screens.LangText
		text := b.text()
		if produce.LooksJSON(text) {
			e.Lang = screens.LangJSON
			if p := produce.CheckJSON(text); p != nil {
				e.Problem, e.ProblemLine = fmt.Sprintf("line %d, col %d: %s", p.Line, p.Col, p.Msg), p.Line
			} else {
				e.Fine = "valid JSON"
			}
		} else if text == "" {
			e.Fine = "empty value"
		}
	case tabKey:
		e.Lang = screens.LangText
		e.Fine = "no key: the partitioner spreads records"
		if b.text() != "" {
			e.Fine = "key: same key, same partition"
		}
	case tabHeaders:
		e.Lang = screens.LangHeaders
		if hs, line, err := produce.ParseHeaders(b.text()); err != nil {
			e.Problem, e.ProblemLine = fmt.Sprintf("line %d: want key=value", line), line
		} else {
			e.Fine = plural(len(hs), "header")
		}
	}
	if strings.HasPrefix(m.notice, "✗") {
		e.Problem, e.Notice, e.Fine = strings.TrimPrefix(m.notice, "✗ "), "", ""
	}
	return e
}

func (m editorModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 || h == 0 {
		w, h = 100, 24
	}
	v := tea.NewView(kit.Join(m.screen().Render(m.k, w, h-1)))
	v.AltScreen = true
	if m.bg != "" {
		v.BackgroundColor = theme.C(m.bg)
	}
	v.WindowTitle = "kaas produce " + m.p.topic
	return v
}
