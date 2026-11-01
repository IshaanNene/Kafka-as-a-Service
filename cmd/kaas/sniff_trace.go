package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/core/sniff"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// traceOf is who a trace follows, in words.
func traceOf(apis []string, clientID string) string {
	who := "every client"
	if clientID != "" {
		who = clientID
	}
	if len(apis) > 0 {
		who += " · " + strings.Join(apis, ", ")
	}
	return who
}

// runTrace records requests for window from the first, on the waterfall
// on a terminal, or as lines.
func runTrace(cmd *cobra.Command, surf surface, p *sniff.Proxy, c *capture, who string, window time.Duration, maxReqs int, asJSON bool) error {
	if surf.rich && !asJSON && answerable(cmd.InOrStdin()) {
		m := newTraceModel(surf.kit, p, c, who, window, maxReqs)
		m.w, m.h = surf.width, surf.height
		_, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
		if cmd.Context().Err() != nil {
			return nil
		}
		return err
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "kaas sniff: tracing %s for %s from the first request; proxy on %s for %s\n",
		who, window, p.Addr(), routesText(p.Routes()))
	return c.trace(cmd.Context(), cmd.OutOrStdout(), asJSON, window, maxReqs)
}

type stagesJSON struct {
	N        int64   `json:"n"`
	API      string  `json:"api"`
	Client   string  `json:"client"`
	ClientID string  `json:"client_id"`
	Broker   int32   `json:"broker"`
	StartMs  float64 `json:"start_ms"` // since the trace's first request
	Connect  float64 `json:"connect_ms"`
	Send     float64 `json:"send_ms"`
	Queue    float64 `json:"queue_ms"`
	Wait     float64 `json:"wait_ms"`
	Ack      float64 `json:"ack_ms"`
	Total    float64 `json:"total_ms"`
	Answered bool    `json:"answered"`
	Err      string  `json:"error"`
	Info     string  `json:"info"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// stagesText is a request's stages in a line.
func stagesText(st sniff.Stages) string {
	return fmt.Sprintf("connect %s · send %s · queue %s · wait %s · ack %s",
		screens.Dur(st.Connect), screens.Dur(st.Send), screens.Dur(st.Queue), screens.Dur(st.Wait), screens.Dur(st.Ack))
}

// trace prints each answered request's stages until window has passed
// since the first, max are printed, or ctx ends.
func (c *capture) trace(ctx context.Context, out io.Writer, asJSON bool, window time.Duration, maxReqs int) error {
	enc := json.NewEncoder(out)
	var t0 time.Time
	var end <-chan time.Time
	n := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-end:
			return nil
		case r := <-c.ch:
			if t0.IsZero() {
				t0 = r.Start
				end = time.After(time.Until(t0.Add(window)))
			}
			if r.Start.Before(t0) || r.Start.Sub(t0) > window {
				continue
			}
			st := r.Stages
			var err error
			if asJSON {
				err = enc.Encode(stagesJSON{r.N, r.Name(), r.Client, r.ClientID, r.Broker, ms(r.Start.Sub(t0)),
					ms(st.Connect), ms(st.Send), ms(st.Queue), ms(st.Wait), ms(st.Ack), ms(st.Total()), r.Answered, r.Err, r.Info})
			} else {
				status := "ok"
				switch {
				case r.Err != "":
					status = r.Err
				case !r.Answered:
					status = "no ack"
				}
				_, err = fmt.Fprintf(out, "+%-8s %-16s broker %d %7s  %s · %s · %s\n",
					screens.Dur(r.Start.Sub(t0)), r.Name(), r.Broker, screens.Dur(st.Total()), stagesText(st), status, r.Info)
			}
			if err != nil {
				return err
			}
			if n++; maxReqs > 0 && n >= maxReqs {
				return nil
			}
		}
	}
}

// traceModel is the live network-waterfall screen.
type traceModel struct {
	k      kit.Kit
	w, h   int
	p      *sniff.Proxy
	c      *capture
	who    string
	window time.Duration
	max    int

	t0       time.Time // the first request's start; zero until it comes
	now      time.Time
	reqs     []sniff.Request
	answered []bool
	at       map[int64]int // request number → index in reqs
	over     bool          // the window has passed: nothing new joins
	sel      int           // index into the shown rows; -1 follows the newest
	tab      int
}

func newTraceModel(k kit.Kit, p *sniff.Proxy, c *capture, who string, window time.Duration, maxReqs int) traceModel {
	return traceModel{k: k, p: p, c: c, who: who, window: window, max: maxReqs, at: map[int64]int{}, sel: -1, now: time.Now()}
}

func (m traceModel) Init() tea.Cmd { return sniffEvery() }

// open says whether a request that started at t joins the trace.
func (m *traceModel) open(t time.Time) bool {
	if m.over {
		return false
	}
	if m.t0.IsZero() {
		m.t0 = t
	}
	// One that began before the first (a long poll from before the trace)
	// isn't part of it.
	return !t.Before(m.t0) && t.Sub(m.t0) <= m.window
}

// drain takes what came since the last tick: requests on their way first,
// so the trace begins at the earliest of them, then answers.
func (m *traceModel) drain() {
	// The model is the channels' only reader, so what len counts is there.
	for len(m.c.sent) > 0 {
		r := <-m.c.sent
		if _, ok := m.at[r.N]; !ok && m.open(r.Start) {
			m.at[r.N] = len(m.reqs)
			m.reqs, m.answered = append(m.reqs, r), append(m.answered, false)
		}
	}
	for len(m.c.ch) > 0 {
		r := <-m.c.ch
		if i, ok := m.at[r.N]; ok {
			m.reqs[i], m.answered[i] = r, true
		} else if m.open(r.Start) {
			m.at[r.N] = len(m.reqs)
			m.reqs, m.answered = append(m.reqs, r), append(m.answered, true)
		}
	}
}

func (m traceModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case sniffTick:
		m.now = time.Time(msg)
		m.drain()
		if !m.t0.IsZero() && m.now.Sub(m.t0) >= m.window {
			m.over = true
		}
		if n := m.done(); m.max > 0 && n >= m.max {
			m.over = true
		}
		return m, sniffEvery()
	case tea.KeyPressMsg:
		rows := len(m.shown())
		switch msg.String() {
		case "ctrl+c", "esc", "q":
			m.over = true
			return m, tea.Quit
		case "up", "k":
			if rows > 0 {
				if m.sel < 0 {
					m.sel = rows - 1
				}
				m.sel = max(0, m.sel-1)
			}
		case "down", "j":
			if m.sel >= 0 {
				if m.sel++; m.sel >= rows-1 {
					m.sel = -1
				}
			}
		case "left", "h", "shift+tab":
			m.tab, m.sel = (m.tab+len(screens.APITabs)-1)%len(screens.APITabs), -1
		case "right", "l", "tab":
			m.tab, m.sel = (m.tab+1)%len(screens.APITabs), -1
		case "enter", "end", "G":
			m.sel = -1
		}
	}
	return m, nil
}

// done is how many requests in the trace have their answers.
func (m traceModel) done() int {
	n := 0
	for i, a := range m.answered {
		if a || !m.reqs[i].Answered {
			n++
		}
	}
	return n
}

// shown are the indexes of the requests the tab shows.
func (m traceModel) shown() []int {
	var is []int
	for i, r := range m.reqs {
		if m.tab == 0 || screens.APITab(r.Key) == m.tab {
			is = append(is, i)
		}
	}
	return is
}

// end is when a request's last stage so far ends, since the trace began.
func (m traceModel) end(i int) time.Duration {
	r := m.reqs[i]
	if m.answered[i] || !r.Answered {
		return r.Start.Sub(m.t0) + r.Stages.Total()
	}
	return m.now.Sub(m.t0) // still waiting
}

func (m traceModel) screen() screens.Waterfall {
	f := screens.Waterfall{
		Title: "Trace", Tabs: screens.APITabs, Tab: m.tab, Sel: m.sel,
		Hint: "q quit · ↑↓ select · ←→ kind · enter follow",
	}
	switch {
	case m.t0.IsZero():
		f.Note = "● waiting for " + m.who + " on " + m.p.Addr()
		f.Detail = "point a client at " + m.p.Addr() + ": the trace starts with its first request and runs " + screens.Dur(m.window)
		f.Span = m.window
		return f
	case m.over:
		f.Note = "■ traced " + m.who + " for " + screens.Dur(m.window)
	default:
		f.Note = "● tracing " + m.who + " · proxy on " + m.p.Addr()
		f.Now = m.now.Sub(m.t0)
	}
	var finish time.Duration
	var bytes int64
	pending := 0
	for _, i := range m.shown() {
		r := m.reqs[i]
		row := screens.WaterRow{Name: r.Name(), Key: r.Key, Info: r.Info, Start: r.Start.Sub(m.t0), Size: -1, Status: "pending"}
		st := r.Stages
		row.Stages = [5]time.Duration{st.Connect, st.Send, st.Queue, st.Wait, st.Ack}
		switch {
		case !r.Answered:
			row.Status, row.Done = "no ack", true
		case m.answered[i]:
			row.Status, row.Done, row.Size = "ok", true, int64(r.RespSize)
			if r.Err != "" {
				row.Status = r.Err
			}
			bytes += int64(r.RespSize)
		default:
			// Queue and wait can't be told apart until the answer comes.
			pending++
			row.Stages[3] = max(0, m.end(i)-row.Start-st.Connect-st.Send)
		}
		finish = max(finish, m.end(i))
		f.Rows = append(f.Rows, row)
	}
	f.Span = max(10*time.Millisecond, time.Duration(float64(max(finish, f.Now))*1.08))

	// The moments a client is ready: its group joined, its first write acked.
	var joined, acked time.Duration
	var group string
	for i, r := range m.reqs {
		if !m.answered[i] || r.Err != "" {
			continue
		}
		switch end := m.end(i); r.Key {
		case 14: // SyncGroup
			if joined == 0 || end < joined {
				joined, group = end, strings.SplitN(r.Info, " · ", 2)[0]
			}
		case 0:
			if acked == 0 || end < acked {
				acked = end
			}
		}
	}
	if joined > 0 {
		f.Marks = append(f.Marks, screens.WaterMark{At: joined, Label: "joined " + group, Text: "joined " + group + ": " + screens.Dur(joined), Role: kit.Accent2})
	}
	if acked > 0 {
		f.Marks = append(f.Marks, screens.WaterMark{At: acked, Label: "first produce acked", Text: "first produce acked: " + screens.Dur(acked), Role: kit.Warn})
	}

	f.Summary = fmt.Sprintf("%s  |  %s answered  |  Finish: %s", plural(len(f.Rows), "request"), screens.Bytes(bytes), screens.Dur(finish))
	if pending > 0 {
		f.Summary += fmt.Sprintf("  |  %d pending", pending)
	}
	if d := m.c.dropped.Load(); d > 0 {
		f.Summary += fmt.Sprintf("  |  %d dropped", d)
	}
	sel := f.Sel
	if sel < 0 || sel >= len(f.Rows) {
		sel = len(f.Rows) - 1
	}
	if sel >= 0 {
		r := m.reqs[m.shown()[sel]]
		f.Detail = fmt.Sprintf("▶ %s · %s", r.Name(), stagesText(r.Stages))
		if !m.answered[m.shown()[sel]] && r.Answered {
			f.Detail = fmt.Sprintf("▶ %s · connect %s · send %s · waiting %s", r.Name(),
				screens.Dur(r.Stages.Connect), screens.Dur(r.Stages.Send), screens.Dur(f.Rows[sel].Stages[3]))
		}
		if r.Err != "" {
			f.Detail += " · " + r.Err
		}
	} else if len(m.reqs) > 0 {
		f.Detail = "nothing of this kind: ←→ for another"
	}
	return f
}

func (m traceModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w = 104
	}
	if h == 0 {
		h = 30
	}
	return tea.NewView(kit.Join(m.screen().Render(m.k, w, max(16, h-2))))
}
