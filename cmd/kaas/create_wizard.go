package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// Wizard steps, in order.
const (
	stepName = iota
	stepLayout
	stepConfigs
	stepCreate
)

var wizardSteps = []string{"Name", "Layout", "Configs", "Create"}

// preset is one value a config card cycles through; "" is the broker's.
type preset struct{ label, value string }

// configCard is one of the piece's integration cards, for a topic config
// people often set.
type configCard struct {
	key, name, desc string
	opts            []preset
	at              int
}

func (c configCard) value() string { return c.opts[c.at].value }

func configCards() []configCard {
	day := 24 * time.Hour
	ms := func(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }
	return []configCard{
		{key: "retention.ms", name: "Retention", desc: "how long records stay", opts: []preset{
			{"default", ""}, {"1h", ms(time.Hour)}, {"1d", ms(day)}, {"7d", ms(7 * day)}, {"30d", ms(30 * day)}, {"forever", "-1"}}},
		{key: "cleanup.policy", name: "Cleanup", desc: "delete or compact", opts: []preset{
			{"default", ""}, {"delete", "delete"}, {"compact", "compact"}, {"compact,delete", "compact,delete"}}},
		{key: "min.insync.replicas", name: "Min in-sync", desc: "acks=all replicas"},
		{key: "compression.type", name: "Compression", desc: "how it's stored", opts: []preset{
			{"default", ""}, {"zstd", "zstd"}, {"lz4", "lz4"}, {"snappy", "snappy"}, {"gzip", "gzip"}}},
	}
}

// wizardModel is kaas topics create's stepper.
type wizardModel struct {
	c    creation
	k    kit.Kit
	w, h int

	step, focus int
	name        string
	parts       string
	rfs         []int // the replication factors the cluster can hold
	rf          int   // index into rfs
	cards       []configCard
	extra       map[string]string // --config keys no card covers

	busy     bool
	problem  string // the controller's or the name's
	checked  bool   // the controller accepts the spec
	creating bool
	finished bool
	quit     bool // left before finishing
	led      int
	total    int
	deadline time.Time // when waiting for leaders gives up
	slow     bool      // created, but not every partition had a leader by then
}

// Messages from the controller.
type (
	defaultsMsg struct {
		made topics.Created
		err  error
	}
	checkedMsg struct{ err error }
	createdMsg struct {
		made topics.Created
		err  error
	}
	leadersMsg struct{ led, total int }
)

func newWizard(c creation, k kit.Kit) wizardModel {
	m := wizardModel{c: c, k: k, name: c.spec.Name, cards: configCards(), extra: map[string]string{}}
	for r := 1; r <= min(len(c.snap.Brokers), 5); r++ {
		m.rfs = append(m.rfs, r)
	}
	if len(m.rfs) == 0 {
		m.rfs = []int{1}
	}
	m.rf = len(m.rfs) - 1
	if c.spec.ReplicationFactor > 0 {
		m.rf = max(0, slices.Index(m.rfs, int(c.spec.ReplicationFactor)))
	}
	m.syncInsync()
	// --config flags start their card on that value; the rest go along.
	for k, v := range c.spec.Configs {
		i := slices.IndexFunc(m.cards, func(c configCard) bool { return c.key == k })
		if i < 0 {
			m.extra[k] = v
			continue
		}
		card := &m.cards[i]
		at := slices.IndexFunc(card.opts, func(p preset) bool { return p.value == v })
		if at < 0 {
			card.opts = append(card.opts, preset{v, v})
			at = len(card.opts) - 1
		}
		card.at = at
	}
	return m
}

// syncInsync offers min.insync.replicas up to the replication factor,
// keeping the choice where it still fits.
func (m *wizardModel) syncInsync() {
	i := slices.IndexFunc(m.cards, func(c configCard) bool { return c.key == "min.insync.replicas" })
	card := &m.cards[i]
	was := ""
	if card.opts != nil {
		was = card.value()
	}
	card.opts = []preset{{"default", ""}}
	for r := 1; r <= m.rfs[m.rf]; r++ {
		card.opts = append(card.opts, preset{strconv.Itoa(r), strconv.Itoa(r)})
	}
	card.at = max(0, slices.IndexFunc(card.opts, func(p preset) bool { return p.value == was }))
}

// spec is the topic as the wizard has it now.
func (m wizardModel) spec() topics.Spec {
	n, _ := strconv.Atoi(m.parts)
	s := topics.Spec{Name: m.name, Partitions: int32(n), ReplicationFactor: int16(m.rfs[m.rf]), Configs: map[string]string{}}
	for k, v := range m.extra {
		s.Configs[k] = v
	}
	for _, c := range m.cards {
		if v := c.value(); v != "" {
			s.Configs[c.key] = v
		}
	}
	return s
}

// nameProblem is why the name can't be created, checked locally.
func (m wizardModel) nameProblem() string {
	if err := topics.ValidName(m.name); err != nil {
		return err.Error()
	}
	if _, ok := m.c.snap.Topic(m.name); ok {
		return "exists already: kaas topics describe " + m.name
	}
	if other := topics.Collides(m.name, m.c.names()); other != "" {
		return collision(other)
	}
	return ""
}

func (m wizardModel) Init() tea.Cmd { return nil }

// ask runs a controller call off the UI loop.
func (m wizardModel) ask(spec topics.Spec, dry bool, wrap func(topics.Created, error) tea.Msg) tea.Cmd {
	adm := m.c.adm
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return wrap(topics.Create(ctx, adm, spec, dry))
	}
}

func (m wizardModel) pollLeaders() tea.Cmd {
	c := m.c
	return tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		led, total := c.leaders(ctx)
		return leadersMsg{led, total}
	})
}

func (m wizardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.PasteMsg:
		m.typed(strings.TrimSpace(msg.Content))
	case defaultsMsg:
		m.busy = false
		if msg.err != nil {
			m.problem = "the controller: " + msg.err.Error()
			break
		}
		// The controller fills in num.partitions and
		// default.replication.factor for a topic that leaves them out.
		if m.parts == "" {
			m.parts = strconv.Itoa(int(msg.made.Partitions))
			if m.c.spec.Partitions > 0 {
				m.parts = strconv.Itoa(int(m.c.spec.Partitions))
			}
		}
		if m.c.spec.ReplicationFactor == 0 {
			if i := slices.Index(m.rfs, int(msg.made.ReplicationFactor)); i >= 0 {
				m.rf = i
				m.syncInsync()
			}
		}
		m.step, m.focus = stepLayout, 0
	case checkedMsg:
		m.busy, m.checked = false, msg.err == nil
		if msg.err != nil {
			m.problem = msg.err.Error()
		}
	case createdMsg:
		if msg.err != nil {
			m.busy, m.creating, m.checked, m.problem = false, false, false, msg.err.Error()
			break
		}
		m.c.made, m.deadline = msg.made, time.Now().Add(30*time.Second)
		return m, m.pollLeaders()
	case leadersMsg:
		m.led, m.total = msg.led, msg.total
		done := msg.total > 0 && msg.led == msg.total
		if done || time.Now().After(m.deadline) {
			m.busy, m.finished, m.slow = false, true, !done
			return m, tea.Quit
		}
		return m, m.pollLeaders()
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

// typed adds text to the focused input: the name, or digits to the
// partition count.
func (m *wizardModel) typed(s string) {
	switch {
	case m.step == stepName:
		m.name += s
		m.problem = ""
	case m.step == stepLayout && m.focus == 0:
		for _, r := range s {
			if r >= '0' && r <= '9' && len(m.parts) < 6 {
				m.parts += string(r)
			}
		}
	}
}

func (m wizardModel) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if k.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
	if m.busy || m.finished {
		return m, nil
	}
	fields := map[int]int{stepLayout: 2, stepConfigs: len(m.cards)}[m.step]
	switch k.String() {
	case "esc":
		if m.step == stepName {
			m.quit = true
			return m, tea.Quit
		}
		m.step, m.focus, m.problem, m.checked = m.step-1, 0, "", false
	case "enter":
		return m.next()
	case "tab", "down":
		if fields > 0 {
			m.focus = (m.focus + 1) % fields
		}
	case "shift+tab", "up":
		if fields > 0 {
			m.focus = (m.focus + fields - 1) % fields
		}
	case "left", "right":
		d := 1
		if k.String() == "left" {
			d = -1
		}
		switch {
		case m.step == stepLayout && m.focus == 1:
			m.rf = min(max(0, m.rf+d), len(m.rfs)-1)
			m.syncInsync()
		case m.step == stepConfigs:
			c := &m.cards[m.focus]
			c.at = (c.at + d + len(c.opts)) % len(c.opts)
		}
	case "backspace":
		switch {
		case m.step == stepName && m.name != "":
			m.name, m.problem = m.name[:len(m.name)-1], ""
		case m.step == stepLayout && m.focus == 0 && m.parts != "":
			m.parts = m.parts[:len(m.parts)-1]
		}
	default:
		m.typed(k.Text)
	}
	return m, nil
}

// next is Continue: check the step, then move on, asking the controller
// where it has to answer first.
func (m wizardModel) next() (tea.Model, tea.Cmd) {
	switch m.step {
	case stepName:
		if p := m.nameProblem(); p != "" {
			m.problem = p
			return m, nil
		}
		m.busy = true
		// A dry create without counts: the controller checks the name and
		// fills in the cluster's defaults.
		return m, m.ask(topics.Spec{Name: m.name}, true, func(c topics.Created, err error) tea.Msg { return defaultsMsg{c, err} })
	case stepLayout:
		if n, _ := strconv.Atoi(m.parts); n < 1 {
			m.problem = "at least 1 partition"
			return m, nil
		}
		m.step, m.focus, m.problem = stepConfigs, 0, ""
	case stepConfigs:
		m.step, m.focus, m.problem, m.checked, m.busy = stepCreate, 0, "", false, true
		return m, m.ask(m.spec(), true, func(_ topics.Created, err error) tea.Msg { return checkedMsg{err} })
	case stepCreate:
		switch {
		case !m.checked:
		case m.c.dry:
			m.finished = true
			return m, tea.Quit
		default:
			// What leaders polls for is the name as typed.
			m.c.spec = m.spec()
			m.busy, m.creating = true, true
			return m, m.ask(m.spec(), false, func(c topics.Created, err error) tea.Msg { return createdMsg{c, err} })
		}
	}
	return m, nil
}

// screen is the stepper for the current step.
func (m wizardModel) screen(width int) screens.Stepper {
	k := m.k
	s := screens.Stepper{Title: "Create topic", Current: m.step, Back: m.step > stepName, Button: "Continue →"}
	if m.c.dry {
		s.Title += " · dry run"
	}
	for i, name := range wizardSteps {
		s.Steps = append(s.Steps, screens.Step{Name: name, Done: i < m.step || m.finished})
	}
	bw := s.BodyWidth(width)
	hint := "enter continue · esc back · ctrl-c quit"
	switch m.step {
	case stepName:
		s.Heading, hint = "Name your topic", "enter continue · esc or ctrl-c quit"
		problem := m.problem
		if m.busy {
			problem = ""
		}
		s.Body = append(screens.Input(k, "Topic name", m.name, min(44, bw), true, problem), "")
		s.Body = append(s.Body, m.family()...)
		if m.busy {
			s.Body = append(s.Body, "", k.S(kit.Muted, "asking the controller…"))
		}
	case stepLayout:
		s.Heading, hint = "Lay out its partitions", "tab next field · ←→ replication · enter continue · esc back"
		s.Body = screens.Input(k, "Partitions", m.parts, 20, m.focus == 0, m.problem)
		labels := make([]string, len(m.rfs))
		for i, r := range m.rfs {
			labels[i] = strconv.Itoa(r)
		}
		s.Body = append(s.Body, "")
		s.Body = append(s.Body, screens.Radios(k, "Replication factor", labels, m.rf, m.focus == 1, bw)...)
		s.Body = append(s.Body, "")
		n, _ := strconv.Atoi(m.parts)
		rf := m.rfs[m.rf]
		brokers := max(1, len(m.c.snap.Brokers))
		s.Body = append(s.Body, wrapIn(k, kit.Muted, fmt.Sprintf("%d × RF %d = %s, about %d per broker over %s",
			n, rf, plural(n*rf, "replica"), (n*rf+brokers-1)/brokers, plural(brokers, "broker")), bw)...)
		if rf == 1 {
			s.Body = append(s.Body, wrapIn(k, kit.Warn, "RF 1: a broker going down takes its partitions offline", bw)...)
		}
	case stepConfigs:
		s.Heading, hint = "Set what differs from the broker", "↑↓ choose · ←→ change · enter continue · esc back"
		for i, c := range m.cards {
			s.Body = append(s.Body, screens.Card(k, c.name, c.desc, c.opts[c.at].label, c.value() != "", i == m.focus, min(54, bw))...)
		}
		if len(m.extra) > 0 {
			// The cards fill the frame; the flags' other configs ride in the hint.
			hint = "also set: " + strings.Join(configPairs(m.extra), ", ") + " · " + hint
		}
	case stepCreate:
		s.Heading, hint = "Check, then create", "enter create · esc back · ctrl-c quit"
		s.Button = "Create →"
		if m.c.dry {
			s.Button, hint = "Done", "enter done · esc back"
		}
		if m.creating || m.finished {
			return m.done(s, bw)
		}
		s.Body = m.review(bw)
	}
	s.Hint = hint
	return s
}

// family is where the name sits among the cluster's topics: its prefix
// path, and how many topics already share it.
func (m wizardModel) family() []string {
	k := m.k
	i := strings.LastIndexByte(m.name, '.')
	if i <= 0 {
		return []string{k.S(kit.Muted, "Family"), k.S(kit.Rule, fmt.Sprintf("top level · %s in the cluster", plural(len(m.c.snap.Topics), "topic")))}
	}
	prefix := m.name[:i]
	n := 0
	for _, t := range m.c.snap.Topics {
		if strings.HasPrefix(t.Name, prefix+".") {
			n++
		}
	}
	share := fmt.Sprintf("the first topic under %s", prefix)
	if n > 0 {
		share = fmt.Sprintf("%s %s %s", plural(n, "topic"), map[bool]string{true: "shares", false: "share"}[n == 1], prefix)
	}
	path := strings.ReplaceAll(prefix, ".", " › ") + " › "
	return []string{k.S(kit.Muted, "Family"), k.S(kit.Rule, path) + k.S(kit.Accent, m.name[i+1:]), k.S(kit.Muted, share)}
}

// review is the spec, then the controller's answer on it.
func (m wizardModel) review(bw int) []string {
	k := m.k
	spec := m.spec()
	n := int(spec.Partitions)
	configs := "broker defaults"
	if len(spec.Configs) > 0 {
		configs = strings.Join(configPairs(spec.Configs), " · ")
	}
	var out []string
	for _, row := range [][2]string{
		{"Topic", spec.Name},
		{"Layout", fmt.Sprintf("%s · RF %d · %s", plural(n, "partition"), spec.ReplicationFactor, plural(n*int(spec.ReplicationFactor), "replica"))},
		{"Configs", configs},
	} {
		for i, l := range kit.Wrap(row[1], bw-10) {
			label := ""
			if i == 0 {
				label = row[0]
			}
			out = append(out, k.S(kit.Muted, kit.Fit(label, 10))+k.S(kit.FG, l))
		}
	}
	out = append(out, "")
	switch {
	case m.busy:
		out = append(out, k.S(kit.Muted, "checking with the controller…"))
	case m.problem != "":
		out = append(out, wrapIn(k, kit.Err, "✗ "+m.problem, bw)...)
		out = append(out, k.S(kit.Muted, "esc goes back to change it"))
	case m.checked && m.c.dry:
		out = append(out, k.S(kit.OK, "✓ the controller accepts this"), k.S(kit.Muted, "dry run: nothing is created"))
	case m.checked:
		out = append(out, k.S(kit.OK, "✓ the controller accepts this"))
	}
	return out
}

// done is the piece's finish: the burst, the topic, and a bar that fills
// as its partitions get leaders.
func (m wizardModel) done(s screens.Stepper, bw int) screens.Stepper {
	k := m.k
	s.Heading, s.Back, s.Hint = "Creating", false, "waiting for every partition to have a leader · ctrl-c leaves it to finish"
	spec := m.spec()
	s.Button = ""
	if m.finished {
		s.Heading, s.Button, s.Pressed, s.Hint = "Created", "Done", true, "kaas topics describe "+spec.Name+" · kaas produce "+spec.Name
		if m.slow {
			s.Hint = fmt.Sprintf("%d of %d partitions had a leader after 30s · kaas topics describe %s", m.led, m.total, spec.Name)
		}
	}
	pad := func(n int, l string) string { return strings.Repeat(" ", max(0, (bw-n)/2)) + l }
	for _, l := range screens.Burst(k) {
		s.Body = append(s.Body, pad(13, l))
	}
	ready := spec.Name + " is ready"
	if !m.finished {
		ready = "creating " + spec.Name
	}
	what := fmt.Sprintf("%s · RF %d · %s", plural(int(m.c.made.Partitions), "partition"), m.c.made.ReplicationFactor, plural(len(spec.Configs), "config"))
	frac := 0.0
	if m.total > 0 {
		frac = float64(m.led) / float64(m.total)
	}
	barW := min(40, bw-4)
	s.Body = append(s.Body, "", pad(kit.Width(ready), k.S(kit.FG, ready)), pad(kit.Width(what), k.S(kit.Muted, what)), "",
		pad(barW, screens.Progress(k, frac, barW)))
	return s
}

// wrapIn wraps plain text, then colours each line, so a wrapped line
// keeps its colour.
func wrapIn(k kit.Kit, r kit.Role, s string, w int) []string {
	lines := kit.Wrap(s, w)
	for i, l := range lines {
		lines[i] = k.S(r, l)
	}
	return lines
}

func (m wizardModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w, h = 80, 24
	}
	// Bubble Tea leaves the last view on screen: the finished stepper, or
	// nothing after a quit, so the error reads on its own.
	if m.quit {
		return tea.NewView("")
	}
	return tea.NewView(kit.Join(m.screen(w).Render(m.k, w, min(22, h-1))))
}
