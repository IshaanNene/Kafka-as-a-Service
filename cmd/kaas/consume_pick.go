package main

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// pickModel is kaas consume --pick: the date-range-picker over the days
// the topic still keeps, counting what each choice would read.
type pickModel struct {
	k     kit.Kit
	w     int
	adm   *kadm.Client
	topic string

	now, oldest time.Time // oldest is the first record the topic keeps
	month       time.Time // the first month shown
	cursor      time.Time
	start, end  time.Time // zero until picked
	counts      map[time.Time]int64
	asked       map[time.Time]bool
	problem     string
	done, quit  bool
}

// countsMsg is the summed offsets at some midnights.
type countsMsg struct {
	at  []time.Time
	sum []int64
	err error
}

// midnight is t's day, at 00:00 in t's zone.
func midnight(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func newPickModel(k kit.Kit, adm *kadm.Client, topic string, now, oldest time.Time, prefill consume.From) pickModel {
	m := pickModel{k: k, adm: adm, topic: topic, now: now, oldest: oldest, counts: map[time.Time]int64{}, asked: map[time.Time]bool{}}
	m.cursor = midnight(now)
	if since, ok := prefill.Since(); ok {
		m.start, m.cursor = midnight(since.Local()), midnight(since.Local())
		if end := prefill.End(); !end.IsZero() {
			m.end = midnight(end.Local().Add(-time.Nanosecond))
		}
	}
	m.month = time.Date(m.cursor.Year(), m.cursor.Month()-1, 1, 0, 0, 0, 0, m.cursor.Location())
	return m
}

// pickWindow runs the picker and returns the window it read to, false when
// the user left.
func pickWindow(cmd *cobra.Command, cl *kgo.Client, topic string, k kit.Kit, prefill consume.From) (consume.From, bool, error) {
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	oldest, err := consume.Oldest(ctx, cl, adm, topic)
	cancel()
	if err != nil {
		return consume.From{}, false, err
	}
	if oldest.IsZero() {
		return consume.From{}, false, &cliError{Code: "empty", Exit: exitFailure, Message: topic + " keeps no records: nothing to pick from"}
	}
	m := newPickModel(k, adm, topic, time.Now(), oldest, prefill)
	final, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
	if err != nil {
		return consume.From{}, false, err
	}
	p := final.(pickModel)
	if !p.done {
		return consume.From{}, false, cancelled("Cancelled: nothing was read.")
	}
	from, until := p.window()
	f, err := consume.ParseFrom(from.Format(time.RFC3339Nano), p.now)
	if err != nil {
		return consume.From{}, false, err
	}
	return f.Until(until), true, nil
}

// window is the picked days as times: the start's midnight, and the
// midnight after the end, or now for a range that ends today.
func (m pickModel) window() (time.Time, time.Time) {
	a, b := m.start, m.end
	if b.Before(a) {
		a, b = b, a
	}
	until := b.AddDate(0, 0, 1)
	if until.After(m.now) {
		until = m.now
	}
	return a, until
}

// kind is how a day can be picked: out before the oldest record or after
// today, empty once counted with nothing in it.
func (m pickModel) kind(t time.Time) int {
	if t.Before(midnight(m.oldest.Local())) || t.After(midnight(m.now)) {
		return screens.DayOut
	}
	a, okA := m.counts[t]
	b, okB := m.counts[t.AddDate(0, 0, 1)]
	if okA && okB && a == b {
		return screens.DayEmpty
	}
	return screens.DayData
}

// count asks for the summed offsets at every midnight the shown months
// need that hasn't been asked for yet.
func (m *pickModel) count() tea.Cmd {
	var at []time.Time
	lo, hi := midnight(m.oldest.Local()), midnight(m.now).AddDate(0, 0, 1)
	for t := m.month; t.Before(m.month.AddDate(0, 2, 1)); t = t.AddDate(0, 0, 1) {
		if !t.Before(lo) && !t.After(hi) && !m.asked[t] {
			at = append(at, t)
			m.asked[t] = true
		}
	}
	if len(at) == 0 {
		return nil
	}
	adm, topic := m.adm, m.topic
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sum, err := consume.Counts(ctx, adm, topic, at)
		return countsMsg{at, sum, err}
	}
}

func (m pickModel) Init() tea.Cmd { return m.count() }

func (m pickModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w = msg.Width
	case countsMsg:
		if msg.err != nil {
			m.problem = "counting: " + msg.err.Error()
			break
		}
		for i, t := range msg.at {
			m.counts[t] = msg.sum[i]
		}
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m pickModel) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	move := func(days int) {
		m.cursor = m.cursor.AddDate(0, 0, days)
	}
	switch k.String() {
	case "ctrl+c", "esc", "q":
		m.quit = true
		return m, tea.Quit
	case "left", "h":
		move(-1)
	case "right", "l":
		move(1)
	case "up", "k":
		move(-7)
	case "down", "j":
		move(7)
	case "[", "pgup":
		m.cursor = m.cursor.AddDate(0, -1, 0)
	case "]", "pgdown":
		m.cursor = m.cursor.AddDate(0, 1, 0)
	case "backspace":
		switch {
		case !m.end.IsZero():
			m.end = time.Time{}
		default:
			m.start = time.Time{}
		}
	case "enter", "space":
		switch {
		case m.kind(m.cursor) == screens.DayOut:
			m.problem = "the topic keeps nothing that day"
			return m, nil
		case m.start.IsZero():
			m.start = m.cursor
		case m.end.IsZero():
			m.end = m.cursor
			if m.end.Before(m.start) {
				m.start, m.end = m.end, m.start
			}
		default:
			m.done = true
			return m, tea.Quit
		}
	}
	m.problem = ""
	// Keep the cursor in the shown months.
	switch last := m.month.AddDate(0, 2, -1); {
	case m.cursor.Before(m.month):
		m.month = time.Date(m.cursor.Year(), m.cursor.Month(), 1, 0, 0, 0, 0, m.cursor.Location())
	case m.cursor.After(last):
		m.month = time.Date(m.cursor.Year(), m.cursor.Month()-1, 1, 0, 0, 0, 0, m.cursor.Location())
	}
	return m, m.count()
}

// records is what the range from a to b, both days, holds; false while
// it is still being counted.
func (m pickModel) records(a, b time.Time) (int64, bool) {
	if b.Before(a) {
		a, b = b, a
	}
	from, until := midnight(a), b.AddDate(0, 0, 1)
	lo := midnight(m.oldest.Local())
	if from.Before(lo) {
		from = lo
	}
	if until.After(midnight(m.now).AddDate(0, 0, 1)) {
		until = midnight(m.now).AddDate(0, 0, 1)
	}
	x, okA := m.counts[from]
	y, okB := m.counts[until]
	return y - x, okA && okB
}

func (m pickModel) screen(width int) screens.RangePicker {
	p := screens.RangePicker{
		Title: "Read " + m.topic + " between", Month: m.month, Cursor: m.cursor,
		Start: m.start, End: m.end, Day: m.kind, Label: "Records", Count: "-",
		Hint: "←→↑↓ move · enter picks the first day, the last, then reads · [ ] month · backspace undo · esc quit",
	}
	day := func(t time.Time) string { return t.Format("Jan 2") }
	if !m.start.IsZero() {
		end := m.end
		if end.IsZero() {
			end = m.cursor // the range follows the cursor until the end is picked
			p.End = end
		}
		a, b := m.start, end
		if b.Before(a) {
			a, b = b, a
		}
		p.From, p.To = day(a), day(b)
		p.Count = "counting…"
		if n, ok := m.records(a, b); ok {
			p.Count = screens.Num(float64(n))
		}
		p.Ready = !m.end.IsZero()
		if p.Ready {
			p.Hint = "enter reads " + p.From + " to " + p.To + " and stops · backspace changes the end · esc quit"
		}
	}
	if m.problem != "" {
		p.Hint = m.problem + " · " + p.Hint
	}
	return p
}

func (m pickModel) View() tea.View {
	if m.quit || m.done {
		return tea.NewView("")
	}
	w := m.w
	if w == 0 {
		w = 80
	}
	return tea.NewView(kit.Join(m.screen(w).Render(m.k, w)))
}
