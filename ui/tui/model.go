// Package tui is the kaas terminal UI. Screens render the engine's latest
// Snapshot and never talk to Kafka themselves; navigation is k9s-style
// (: commands, / filter, enter, esc, ?), and every screen has a CLI twin.
package tui

import (
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/fuzzy"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/asciipack"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

type screen int

const (
	screenTopics screen = iota
	screenTopic
	screenBrokers
	screenNotFound
)

// commands the palette accepts; also the "did you mean" candidates.
var commands = []string{"topics", "brokers", "topic", "quit"}

const maxFeed = 6 // events kept for the feed at the bottom

type (
	updateMsg engine.Update
	clockMsg  time.Time
	artMsg    time.Time
)

type feedItem struct {
	at time.Time
	ev engine.Event
}

// notFound describes what the 404 screen is apologising for.
type notFound struct {
	what        string // "command", "topic"
	name        string
	suggestions []string
	since       time.Time
}

// Model is the root Bubble Tea model.
type Model struct {
	eng *engine.Engine // nil in tests; updates then arrive as updateMsg
	sub *engine.Subscription
	th  theme.Theme
	st  styles

	snap   *snapshot.Snapshot
	feed   []feedItem
	now    time.Time
	w, h   int
	stack  []screen
	cursor map[screen]int
	topic  string // open topic on screenTopic

	filtering bool
	filter    string
	commandOn bool
	command   string
	help      bool
	debug     bool

	nf      notFound
	art     *asciipack.Pack
	unwatch func()
	reduced bool // KAAS_REDUCED_MOTION
}

// Options configures the TUI.
type Options struct {
	ReducedMotion bool
}

// New builds the TUI over a running engine.
func New(e *engine.Engine, th theme.Theme, o Options) Model {
	m := newModel(th, o)
	m.eng = e
	m.sub = e.Subscribe()
	return m
}

func newModel(th theme.Theme, o Options) Model {
	art, _ := asciipack.Load("page-404") // embedded; absence only loses the art
	return Model{
		th:      th,
		st:      newStyles(th),
		stack:   []screen{screenTopics},
		cursor:  map[screen]int{},
		now:     time.Now(),
		art:     art,
		reduced: o.ReducedMotion,
		unwatch: func() {},
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.waitUpdate(), clock())
}

func (m Model) waitUpdate() tea.Cmd {
	if m.sub == nil {
		return nil
	}
	sub := m.sub
	return func() tea.Msg {
		<-sub.Ready()
		u, _ := sub.Next()
		return updateMsg(u)
	}
}

func clock() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return clockMsg(t) })
}

func (m Model) artTick() tea.Cmd {
	if m.art == nil || m.reduced {
		return nil
	}
	return tea.Tick(time.Second/time.Duration(m.art.FPS), func(t time.Time) tea.Msg { return artMsg(t) })
}

func (m Model) screen() screen { return m.stack[len(m.stack)-1] }

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
		return m, nil
	case updateMsg:
		m.snap = msg.Snapshot
		for _, ev := range msg.Events {
			m.feed = append(m.feed, feedItem{at: msg.Snapshot.TakenAt, ev: ev})
		}
		if n := len(m.feed); n > maxFeed {
			m.feed = m.feed[n-maxFeed:]
		}
		m.clampCursor()
		return m, m.waitUpdate()
	case clockMsg:
		m.now = time.Time(msg)
		return m, clock()
	case artMsg:
		m.now = time.Time(msg)
		if m.screen() == screenNotFound {
			return m, m.artTick()
		}
		return m, nil
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m Model) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	s := k.String()
	if s == "ctrl+c" {
		m.unwatch()
		return m, tea.Quit
	}
	switch {
	case m.commandOn:
		return m.commandKey(k)
	case m.filtering:
		return m.filterKey(k), nil
	case m.help:
		m.help = false
		return m, nil
	}

	switch s {
	case "q":
		m.unwatch()
		return m, tea.Quit
	case "?":
		m.help = true
	case ":":
		m.commandOn, m.command = true, ""
	case "/":
		if m.screen() != screenNotFound {
			m.filtering = true
		}
	case "ctrl+d":
		m.debug = !m.debug
	case "esc":
		if m.filter != "" {
			m.filter = ""
			m.clampCursor()
		} else {
			m = m.pop()
		}
	case "up", "k":
		m.cursor[m.screen()]--
		m.moved()
	case "down", "j":
		m.cursor[m.screen()]++
		m.moved()
	case "g", "home":
		m.cursor[m.screen()] = 0
		m.moved()
	case "G", "end":
		m.cursor[m.screen()] = 1 << 30
		m.moved()
	case "enter":
		if m.screen() == screenTopics {
			if ts := m.visibleTopics(); len(ts) > 0 {
				return m.openTopic(ts[m.cursor[screenTopics]].Name)
			}
		}
	}
	return m, nil
}

func (m Model) filterKey(k tea.KeyPressMsg) Model {
	switch k.String() {
	case "enter":
		m.filtering = false
	case "esc":
		m.filtering, m.filter = false, ""
	case "backspace":
		if r := []rune(m.filter); len(r) > 0 {
			m.filter = string(r[:len(r)-1])
		}
	default:
		if k.Text != "" {
			m.filter += k.Text
		}
	}
	m.cursor[m.screen()] = 0
	return m
}

func (m Model) commandKey(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch k.String() {
	case "esc":
		m.commandOn = false
		return m, nil
	case "backspace":
		if r := []rune(m.command); len(r) > 0 {
			m.command = string(r[:len(r)-1])
		}
		return m, nil
	case "enter":
		m.commandOn = false
		return m.run(strings.Fields(m.command))
	}
	if k.Text != "" {
		m.command += k.Text
	}
	return m, nil
}

// run executes a palette command such as "topics" or "topic orders".
func (m Model) run(args []string) (tea.Model, tea.Cmd) {
	if len(args) == 0 {
		return m, nil
	}
	switch args[0] {
	case "q", "quit":
		m.unwatch()
		return m, tea.Quit
	case "topics", "t":
		m.stack = []screen{screenTopics}
		m.watch()
		return m, nil
	case "brokers", "b":
		m.stack = []screen{screenTopics, screenBrokers}
		m.watch()
		return m, nil
	case "topic":
		if len(args) < 2 {
			return m, nil
		}
		return m.openTopic(args[1])
	}
	// Bare topic names work too: ":orders".
	if m.snap != nil {
		if _, ok := m.snap.Topic(args[0]); ok {
			return m.openTopic(args[0])
		}
	}
	return m.notFound("command", args[0], append(slices.Clone(commands), m.topicNames()...))
}

func (m Model) openTopic(name string) (tea.Model, tea.Cmd) {
	if m.snap != nil {
		if _, ok := m.snap.Topic(name); !ok {
			return m.notFound("topic", name, m.topicNames())
		}
	}
	m.topic = name
	m.cursor[screenTopic] = 0
	m.filter = ""
	if m.screen() != screenTopic {
		m.stack = append(m.stack, screenTopic)
	}
	m.watch()
	return m, nil
}

func (m Model) notFound(what, name string, candidates []string) (tea.Model, tea.Cmd) {
	m.nf = notFound{what: what, name: name, suggestions: fuzzy.Suggest(name, candidates, 3), since: m.now}
	if m.screen() != screenNotFound {
		m.stack = append(m.stack, screenNotFound)
	}
	return m, m.artTick()
}

func (m Model) pop() Model {
	if len(m.stack) > 1 {
		m.stack = m.stack[:len(m.stack)-1]
		m.watch()
	}
	return m
}

// watch tells the engine which topics are on screen, so their offsets
// refresh on the fast interval while everything else ticks slowly.
func (m *Model) watch() {
	m.unwatch()
	m.unwatch = func() {}
	if m.eng == nil {
		return
	}
	switch m.screen() {
	case screenTopic:
		m.unwatch = m.eng.Watch(m.topic)
	case screenTopics:
		if ts := m.visibleTopics(); len(ts) > 0 {
			m.unwatch = m.eng.Watch(ts[m.cursor[screenTopics]].Name)
		}
	}
}

func (m Model) topicNames() []string {
	if m.snap == nil {
		return nil
	}
	out := make([]string, 0, len(m.snap.Topics))
	for _, t := range m.snap.Topics {
		out = append(out, t.Name)
	}
	return out
}

// visibleTopics applies the filter. Internal topics show only when the
// filter asks for them by name.
func (m Model) visibleTopics() []snapshot.Topic {
	if m.snap == nil {
		return nil
	}
	var out []snapshot.Topic
	for _, t := range m.snap.Topics {
		if t.Internal && !strings.HasPrefix(m.filter, "_") {
			continue
		}
		if m.filter != "" && !strings.Contains(strings.ToLower(t.Name), strings.ToLower(m.filter)) {
			continue
		}
		out = append(out, t)
	}
	return out
}

// moved clamps the cursor and, on the topics list, moves the fast-refresh
// watch to the newly selected topic.
func (m *Model) moved() {
	m.clampCursor()
	if m.screen() == screenTopics {
		m.watch()
	}
}

func (m *Model) clampCursor() {
	n := 0
	switch m.screen() {
	case screenTopics:
		n = len(m.visibleTopics())
	case screenTopic:
		if m.snap != nil {
			t, _ := m.snap.Topic(m.topic)
			n = len(t.Partitions)
		}
	case screenBrokers:
		if m.snap != nil {
			n = len(m.snap.Brokers)
		}
	}
	c := m.cursor[m.screen()]
	m.cursor[m.screen()] = max(0, min(c, n-1))
}
