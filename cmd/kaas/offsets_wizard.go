package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// Reset wizard steps, in order.
const (
	resetGroup = iota
	resetTopics
	resetTarget
	resetReview
)

var resetSteps = []string{"Group", "Topics", "Where to", "Review"}

// The --to kinds the wizard offers, with what their input means.
var resetKinds = []struct{ name, label, example, desc string }{
	{"earliest", "", "", "the start of each partition's log: the group reads everything still kept"},
	{"latest", "", "", "the end of each partition's log: the group skips to new records"},
	{"time", "Time, or how long ago", "2h", "the first record at or after it: 2h, 7d, 2026-10-31 09:00"},
	{"offset", "Offset", "0", "the same offset on every partition, kept inside each log"},
	{"shift", "Records, + forward or - back", "-1000", "from each committed offset; partitions with none are left alone"},
}

// resetTopic is one topic the group can move on.
type resetTopic struct {
	name      string
	parts     int
	committed bool
	lag       int64 // -1 unknown
	on        bool
}

// resetWizard is kaas offsets reset's stepper: the group, its topics,
// where to, and the plan's effect, before the plan itself.
type resetWizard struct {
	k    kit.Kit
	w, h int
	adm  *kadm.Client

	step   int
	groups []groups.Group
	group  int
	all    []resetTopic // every topic, for a group with no commits
	rows   []resetTopic
	row    int
	kind   int
	value  string

	problem  string
	busy     bool
	plan     *offsets.Plan
	finished bool
	quit     bool
}

type resetPlanMsg struct {
	p   offsets.Plan
	err error
}

// wizard asks for what the flags left out. It returns false when the
// user left.
func (r *resetAsk) wizard(cmd *cobra.Command, adm *kadm.Client, k kit.Kit) (bool, error) {
	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	m, err := newResetWizard(ctx, adm, k, *r)
	cancel()
	if err != nil {
		return false, err
	}
	final, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
	if err != nil {
		return false, err
	}
	w := final.(resetWizard)
	if !w.finished {
		return false, cancelled("Cancelled: no offsets were committed.")
	}
	g := w.groups[w.group]
	r.group, r.all, r.topics = g.Name, false, w.chosen()
	r.toText = w.toText()
	to := w.plan.Target
	r.to = &to
	return true, nil
}

// newResetWizard loads the cluster's groups and topics, and starts past
// whatever r already has.
func newResetWizard(ctx context.Context, adm *kadm.Client, k kit.Kit, r resetAsk) (resetWizard, error) {
	s, err := groups.Fetch(ctx, adm)
	if err != nil {
		return resetWizard{}, err
	}
	td, err := adm.ListTopics(ctx)
	if err != nil {
		return resetWizard{}, err
	}
	m := resetWizard{k: k, adm: adm, groups: s.Groups, kind: -1}
	for _, t := range td.Sorted() {
		if !t.IsInternal && !strings.HasPrefix(t.Topic, "__") {
			m.all = append(m.all, resetTopic{name: t.Topic, parts: len(t.Partitions), lag: -1})
		}
	}
	if r.group != "" {
		i := slices.IndexFunc(m.groups, func(g groups.Group) bool { return g.Name == r.group })
		if i < 0 {
			// A group with no commits yet: the reset creates it.
			m.groups = append(m.groups, groups.Group{Name: r.group, State: "Dead", Lag: -1})
			i = len(m.groups) - 1
		}
		m.group, m.step = i, resetTopics
		m.fillTopics()
	}
	if r.to != nil {
		m.kind = map[int]int{offsets.Earliest: 0, offsets.Latest: 1, offsets.Time: 2, offsets.Offset: 3, offsets.Shift: 4}[r.to.Kind]
		m.value = r.toText
	}
	if len(r.topics) > 0 || r.all {
		for i := range m.rows {
			m.rows[i].on = r.all && m.rows[i].committed || slices.Contains(r.topics, m.rows[i].name)
		}
		if m.step == resetTopics {
			m.step = resetTarget
		}
	}
	if m.kind < 0 {
		m.kind = 0
	}
	return m, nil
}

// fillTopics lists the chosen group's committed topics, all on; for a
// group with none, every topic, all off.
func (m *resetWizard) fillTopics() {
	g := m.groups[m.group]
	m.rows, m.row = nil, 0
	for _, t := range m.all {
		if slices.Contains(g.Topics, t.name) {
			t.committed, t.on, t.lag = true, true, -1
			if l, ok := g.TopicLag[t.name]; ok {
				t.lag = l
			}
			m.rows = append(m.rows, t)
		}
	}
	if len(m.rows) == 0 {
		m.rows = slices.Clone(m.all)
	}
}

func (m resetWizard) chosen() []string {
	var out []string
	for _, t := range m.rows {
		if t.on {
			out = append(out, t.name)
		}
	}
	return out
}

func (m resetWizard) toText() string {
	if m.kind < 2 {
		return resetKinds[m.kind].name
	}
	return strings.TrimSpace(m.value)
}

// target reads the chosen kind and its value.
func (m resetWizard) target() (offsets.Target, error) {
	v := m.toText()
	switch m.kind {
	case 3:
		if strings.HasPrefix(v, "+") || strings.HasPrefix(v, "-") {
			return offsets.Target{}, fmt.Errorf("an offset is a plain number; use shift to move by %s", v)
		}
	case 4:
		if v != "" && v[0] != '+' && v[0] != '-' {
			v = "+" + v
		}
	}
	t, err := offsets.ParseTo(v, time.Now())
	if err != nil {
		return t, err
	}
	want := map[int]int{0: offsets.Earliest, 1: offsets.Latest, 2: offsets.Time, 3: offsets.Offset, 4: offsets.Shift}[m.kind]
	if t.Kind != want {
		return t, fmt.Errorf("%q is not a %s", v, resetKinds[m.kind].name)
	}
	return t, nil
}

func (m resetWizard) Init() tea.Cmd { return nil }

func (m resetWizard) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.PasteMsg:
		m.typed(strings.TrimSpace(msg.Content))
	case resetPlanMsg:
		m.busy = false
		if msg.err != nil {
			m.problem = msg.err.Error()
			if msg.p.Members > 0 {
				m.problem = fmt.Sprintf("%s has %s: stop its consumers first", msg.p.Group, plural(msg.p.Members, "active member"))
			}
			break
		}
		m.plan = &msg.p
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m *resetWizard) typed(s string) {
	if m.step == resetTarget && m.kind >= 2 {
		m.value += s
		m.problem = ""
	}
}

func (m resetWizard) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if k.String() == "ctrl+c" {
		m.quit = true
		return m, tea.Quit
	}
	if m.busy {
		return m, nil
	}
	switch k.String() {
	case "esc":
		if m.step == resetGroup {
			m.quit = true
			return m, tea.Quit
		}
		m.step, m.problem, m.plan = m.step-1, "", nil
	case "enter":
		return m.next()
	case "up", "down", "shift+tab", "tab":
		d := 1
		if k.String() == "up" || k.String() == "shift+tab" {
			d = -1
		}
		switch m.step {
		case resetGroup:
			if n := len(m.groups); n > 0 {
				m.group, m.problem = (m.group+d+n)%n, ""
			}
		case resetTopics:
			if n := len(m.rows); n > 0 {
				m.row = (m.row + d + n) % n
			}
		}
	case "left", "right":
		if m.step == resetTarget {
			d := 1
			if k.String() == "left" {
				d = -1
			}
			was := m.kind
			m.kind = min(max(0, m.kind+d), len(resetKinds)-1)
			if m.kind != was {
				m.value, m.problem = resetKinds[m.kind].example, ""
			}
		}
	case "space":
		if m.step == resetTopics && len(m.rows) > 0 {
			m.rows[m.row].on = !m.rows[m.row].on
			m.problem = ""
		}
	case "backspace":
		if m.step == resetTarget && m.value != "" {
			m.value, m.problem = m.value[:len(m.value)-1], ""
		}
	default:
		if m.step == resetTopics && k.String() == "a" {
			on := slices.ContainsFunc(m.rows, func(t resetTopic) bool { return !t.on })
			for i := range m.rows {
				m.rows[i].on = on
			}
			break
		}
		m.typed(k.Text)
	}
	return m, nil
}

// next is Continue: check the step, then move on; the review asks the
// cluster for the plan.
func (m resetWizard) next() (tea.Model, tea.Cmd) {
	switch m.step {
	case resetGroup:
		if len(m.groups) == 0 {
			return m, nil
		}
		if g := m.groups[m.group]; g.Members > 0 {
			m.problem = fmt.Sprintf("%s has %s: Kafka only takes commits from a group with none, so stop them first", g.Name, plural(g.Members, "member"))
			return m, nil
		}
		m.fillTopics()
		m.step, m.problem = resetTopics, ""
	case resetTopics:
		if len(m.chosen()) == 0 {
			m.problem = "choose at least one topic: space to tick it"
			return m, nil
		}
		m.step, m.problem = resetTarget, ""
	case resetTarget:
		t, err := m.target()
		if err != nil {
			m.problem = err.Error()
			return m, nil
		}
		m.step, m.problem, m.plan, m.busy = resetReview, "", nil, true
		adm, group, topics := m.adm, m.groups[m.group].Name, m.chosen()
		return m, func() tea.Msg {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			p, err := offsets.PlanReset(ctx, adm, group, topics, t)
			return resetPlanMsg{p, err}
		}
	case resetReview:
		if m.plan == nil {
			return m, nil
		}
		if add, change, _ := m.plan.Counts(); add+change == 0 {
			return m, nil
		}
		m.finished = true
		return m, tea.Quit
	}
	return m, nil
}

// screen is the stepper for the current step.
func (m resetWizard) screen(width int) screens.Stepper {
	k := m.k
	s := screens.Stepper{Title: "Reset offsets", Current: m.step, Back: m.step > resetGroup, Button: "Continue →"}
	for i, name := range resetSteps {
		s.Steps = append(s.Steps, screens.Step{Name: name, Done: i < m.step})
	}
	bw := s.BodyWidth(width)
	hint := "enter continue · esc back · ctrl-c quit"
	switch m.step {
	case resetGroup:
		s.Heading, hint = "Which group?", "↑↓ choose · enter continue · esc or ctrl-c quit"
		s.Body = m.groupList(bw)
	case resetTopics:
		s.Heading, hint = "Move it on which topics?", "↑↓ move · space tick · a all · enter continue · esc back"
		s.Body = m.topicList(bw)
	case resetTarget:
		s.Heading, hint = "Where to?", "←→ choose · type the value · enter continue · esc back"
		kind := resetKinds[m.kind]
		names := make([]string, len(resetKinds))
		for i, rk := range resetKinds {
			names[i] = rk.name
		}
		s.Body = screens.Radios(k, "Move each partition to", names, m.kind, true, bw)
		s.Body = append(s.Body, "")
		if kind.label != "" {
			s.Body = append(s.Body, screens.Input(k, kind.label, m.value, min(32, bw), true, m.problem)...)
			s.Body = append(s.Body, "")
		}
		s.Body = append(s.Body, wrapIn(k, kit.Muted, kind.desc, bw)...)
	case resetReview:
		s.Heading, hint, s.Button = "Review", "enter shows the plan, then asks before committing · esc back", "Show plan →"
		s.Body = m.review(bw)
	}
	s.Hint = hint
	return s
}

// groupList is the cluster's groups, the chosen one in blue, with a window
// that follows the choice.
func (m resetWizard) groupList(bw int) []string {
	k := m.k
	if len(m.groups) == 0 {
		return wrapIn(k, kit.Muted, "No consumer groups yet. Name one to give it its first offsets: kaas offsets reset <group> -t <topic> --to earliest", bw)
	}
	const show = 9
	from := min(max(0, m.group-show/2), max(0, len(m.groups)-show))
	var out []string
	for i := from; i < min(len(m.groups), from+show); i++ {
		g := m.groups[i]
		mark, role := "○ ", kit.Muted
		if i == m.group {
			mark, role = "● ", kit.Accent
		}
		detail := orDash(g.State) + " · " + plural(len(g.Topics), "topic")
		if g.Lag >= 0 {
			detail += " · lag " + screens.Num(float64(g.Lag))
		}
		dr := kit.Muted
		if g.Members > 0 {
			detail, dr = orDash(g.State)+" · "+plural(g.Members, "member")+" · in use", kit.Warn
		}
		nameW := min(24, bw/2)
		out = append(out, k.S(role, mark)+k.S(kit.FG, kit.Fit(g.Name, nameW))+" "+k.S(dr, kit.Fit(detail, bw-nameW-3)))
	}
	if n := len(m.groups); n > show {
		out = append(out, k.S(kit.Rule, fmt.Sprintf("%d of %d", m.group+1, n)))
	}
	if m.problem != "" {
		out = append(out, "")
		out = append(out, wrapIn(k, kit.Err, "✗ "+m.problem, bw)...)
	}
	return out
}

// topicList is the group's topics with a tick each.
func (m resetWizard) topicList(bw int) []string {
	k := m.k
	g := m.groups[m.group]
	var out []string
	if !slices.ContainsFunc(m.rows, func(t resetTopic) bool { return t.committed }) {
		out = append(out, wrapIn(k, kit.Muted, g.Name+" has no commits yet: tick the topics to give it its first offsets", bw)...)
		out = append(out, "")
	}
	const show = 8
	from := min(max(0, m.row-show/2), max(0, len(m.rows)-show))
	for i := from; i < min(len(m.rows), from+show); i++ {
		t := m.rows[i]
		box, br := "[ ] ", kit.Muted
		if t.on {
			box, br = "[x] ", kit.OK
		}
		name := kit.Fit(t.name, min(26, bw/2))
		nr := kit.FG
		if i == m.row {
			nr = kit.Accent
		}
		detail := plural(t.parts, "partition")
		if t.lag >= 0 {
			detail += " · lag " + screens.Num(float64(t.lag))
		}
		out = append(out, k.S(br, box)+k.S(nr, name)+" "+k.S(kit.Muted, detail))
	}
	out = append(out, "", k.S(kit.Muted, plural(len(m.chosen()), "topic")+" ticked"))
	if m.problem != "" {
		out = append(out, wrapIn(k, kit.Err, "✗ "+m.problem, bw)...)
	}
	return out
}

// review is what was chosen, then the plan's effect from the cluster.
func (m resetWizard) review(bw int) []string {
	k := m.k
	g := m.groups[m.group]
	var out []string
	for _, row := range [][2]string{
		{"Group", g.Name},
		{"Topics", strings.Join(m.chosen(), ", ")},
		{"To", m.toText()},
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
		out = append(out, k.S(kit.Muted, "planning with the cluster…"))
	case m.problem != "":
		out = append(out, wrapIn(k, kit.Err, "✗ "+m.problem, bw)...)
		out = append(out, k.S(kit.Muted, "esc goes back to change it"))
	case m.plan != nil:
		add, change, same := m.plan.Counts()
		if add+change == 0 {
			out = append(out, k.S(kit.OK, "✓ "+g.Name+" is already there: nothing to commit"), k.S(kit.Muted, "esc goes back to change it"))
			break
		}
		out = append(out, k.S(kit.Notice, "~ "+plural(change, "offset")+" move")+k.S(kit.Muted, " · ")+k.S(kit.OK, "+ "+plural(add, "first commit"))+k.S(kit.Muted, fmt.Sprintf(" · %d unchanged", same)))
		reread, skipped := m.plan.Records()
		var eff []string
		if reread > 0 {
			eff = append(eff, "reads "+plural(int(reread), "record")+" again")
		}
		if skipped > 0 {
			eff = append(eff, "skips "+plural(int(skipped), "record"))
		}
		if len(eff) > 0 {
			out = append(out, wrapIn(k, kit.Muted, g.Name+" "+strings.Join(eff, " and "), bw)...)
		}
	}
	return out
}

func (m resetWizard) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w, h = 80, 24
	}
	if m.quit || m.finished {
		return tea.NewView("") // the plan follows on its own
	}
	return tea.NewView(kit.Join(m.screen(w).Render(m.k, w, min(22, h-1))))
}
