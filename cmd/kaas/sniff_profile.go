package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/core/sniff"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// runProfile adds up requests' stages until window has passed since the
// first (0: until q or Ctrl-C) or max are counted, on the flame graph on a
// terminal, or printed once at the end.
func runProfile(cmd *cobra.Command, surf surface, p *sniff.Proxy, c *capture, who string, window time.Duration, maxReqs int, asJSON bool) error {
	if surf.rich && !asJSON && answerable(cmd.InOrStdin()) {
		m := newProfileModel(surf.kit, p, c, who, window, maxReqs)
		m.w, m.h = surf.width, surf.height
		_, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
		if cmd.Context().Err() != nil {
			return nil
		}
		return err
	}
	until := "Ctrl-C"
	if window > 0 {
		until = window.String() + " from the first request"
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "kaas sniff: profiling %s until %s; proxy on %s for %s\n", who, until, p.Addr(), routesText(p.Routes()))
	prof := c.profile(cmd.Context(), window, maxReqs)
	return writeProfile(cmd.OutOrStdout(), prof, asJSON)
}

// profile counts answered requests until window has passed since the
// first, max are counted, or ctx ends.
func (c *capture) profile(ctx context.Context, window time.Duration, maxReqs int) *sniff.Profile {
	prof := sniff.NewProfile()
	var end <-chan time.Time
	var t0 time.Time
	for {
		select {
		case <-ctx.Done():
			return prof
		case <-end:
			return prof
		case r := <-c.ch:
			if t0.IsZero() {
				t0 = r.Start
				if window > 0 {
					end = time.After(time.Until(t0.Add(window)))
				}
			}
			if r.Start.Before(t0) || (window > 0 && r.Start.Sub(t0) > window) {
				continue
			}
			prof.Add(r)
			if maxReqs > 0 && prof.Requests >= maxReqs {
				return prof
			}
		}
	}
}

type frameJSON struct {
	Name     string      `json:"name"`
	TimeMs   float64     `json:"time_ms"`
	SelfMs   float64     `json:"self_ms"`
	Requests int         `json:"requests"`
	AvgMs    float64     `json:"avg_ms"`
	P99Ms    float64     `json:"p99_ms"`
	Frames   []frameJSON `json:"frames"`
}

func frameJSONOf(f *sniff.Frame) frameJSON {
	j := frameJSON{Name: f.Name, TimeMs: ms(f.Time), SelfMs: ms(f.Self), Requests: f.N, AvgMs: ms(f.Avg()), P99Ms: ms(f.Quantile(0.99)), Frames: []frameJSON{}}
	for _, k := range f.Sorted() {
		j.Frames = append(j.Frames, frameJSONOf(k))
	}
	return j
}

func writeProfile(out io.Writer, prof *sniff.Profile, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(out).Encode(frameJSONOf(prof.Root))
	}
	_, err := io.WriteString(out, prof.Folded())
	return err
}

// profileModel is the live flame-graph screen.
type profileModel struct {
	k      kit.Kit
	w, h   int
	p      *sniff.Proxy
	c      *capture
	who    string
	window time.Duration
	max    int

	prof     *sniff.Profile
	t0       time.Time
	over     bool
	sel      []string // the selected frame, by names below all
	zoom     []string // the frame shown as the base, by names
	showIdle bool
}

func newProfileModel(k kit.Kit, p *sniff.Proxy, c *capture, who string, window time.Duration, maxReqs int) profileModel {
	return profileModel{k: k, p: p, c: c, who: who, window: window, max: maxReqs, prof: sniff.NewProfile()}
}

func (m profileModel) Init() tea.Cmd { return sniffEvery() }

func (m *profileModel) drain(now time.Time) {
	for len(m.c.ch) > 0 {
		r := <-m.c.ch
		if m.over {
			continue
		}
		if m.t0.IsZero() {
			m.t0 = r.Start
		}
		if r.Start.Before(m.t0) || (m.window > 0 && r.Start.Sub(m.t0) > m.window) {
			continue
		}
		m.prof.Add(r)
		if m.max > 0 && m.prof.Requests >= m.max {
			m.over = true
		}
	}
	if m.window > 0 && !m.t0.IsZero() && now.Sub(m.t0) >= m.window {
		m.over = true
	}
}

// view is the profile as the flame graph draws it: frames sorted, idle
// polls left out unless shown, frames with no time left out.
func (m profileModel) view(f *sniff.Frame) (screens.FlameNode, bool) {
	n := screens.FlameNode{Name: f.Name}
	if f.Self > 0 {
		if (f.Name == sniff.StageIdle || f.Name == sniff.StagePoll) && !m.showIdle {
			return n, false
		}
		n.Self = float64(f.Self)
		n.Time = n.Self
	}
	for _, k := range f.Sorted() {
		if kn, ok := m.view(k); ok {
			n.Kids = append(n.Kids, kn)
			n.Time += kn.Time
		}
	}
	// Sorted again: hidden idle polls can change which frame weighs most.
	sort.SliceStable(n.Kids, func(i, j int) bool { return n.Kids[i].Time > n.Kids[j].Time })
	return n, n.Time > 0
}

// at is the frame names lead to under root, and the indexes that reach
// it; it stops at the deepest name that is there.
func at(root screens.FlameNode, names []string) (screens.FlameNode, []int, []string) {
	n := root
	var idx []int
	var got []string
	for _, name := range names {
		found := false
		for i, k := range n.Kids {
			if k.Name == name {
				n, idx, got, found = k, append(idx, i), append(got, name), true
				break
			}
		}
		if !found {
			break
		}
	}
	return n, idx, got
}

// frame is the counted frame at names, for its numbers.
func (m profileModel) frame(names []string) *sniff.Frame {
	f := m.prof.Root
	for _, name := range names {
		var next *sniff.Frame
		for _, k := range f.Kids {
			if k.Name == name {
				next = k
			}
		}
		if next == nil {
			return f
		}
		f = next
	}
	return f
}

func (m profileModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case sniffTick:
		m.drain(time.Time(msg))
		return m, sniffEvery()
	case tea.KeyPressMsg:
		root, _ := m.view(m.prof.Root)
		_, _, sel := at(root, m.sel)
		m.sel = sel
		parent, _, _ := at(root, sel[:max(0, len(sel)-1)])
		sibling := func(step int) {
			if len(sel) == 0 {
				return
			}
			for i, k := range parent.Kids {
				if k.Name == sel[len(sel)-1] {
					j := (i + step + len(parent.Kids)) % len(parent.Kids)
					m.sel = append(append([]string(nil), sel[:len(sel)-1]...), parent.Kids[j].Name)
					return
				}
			}
		}
		switch msg.String() {
		case "ctrl+c", "q":
			m.over = true
			return m, tea.Quit
		case "left", "h":
			sibling(-1)
		case "right", "l":
			sibling(1)
		case "up", "k":
			if n, _, _ := at(root, sel); len(n.Kids) > 0 {
				m.sel = append(append([]string(nil), sel...), n.Kids[0].Name)
			}
		case "down", "j":
			if len(sel) > len(m.zoom) {
				m.sel = sel[:len(sel)-1]
			}
		case "enter":
			m.zoom = append([]string(nil), sel...)
		case "esc", "backspace":
			if len(m.zoom) > 0 {
				m.zoom = m.zoom[:len(m.zoom)-1]
			}
		case "i":
			m.showIdle = !m.showIdle
		case "r":
			m.prof, m.t0, m.over, m.sel, m.zoom = sniff.NewProfile(), time.Time{}, false, nil, nil
		}
	}
	return m, nil
}

func (m profileModel) screen() screens.Flame {
	idle := "polls hidden"
	if m.showIdle {
		idle = "polls shown"
	}
	state := "recording"
	switch {
	case m.t0.IsZero():
		state = "waiting for a request"
	case m.over:
		state = "recorded"
	case m.window > 0:
		state = "recording " + screens.Dur(m.window)
	}
	f := screens.Flame{
		Title: strings.Join([]string{"latency profile", m.who, "proxy on " + m.p.Addr(), idle, state}, " · "),
		Right: "requests " + screens.Num(float64(m.prof.Requests)),
		Hint:  "←→ sibling · ↑ up the stack · ↓ down · enter zoom · esc out · i polls · r reset · q quit",
		Empty: "nothing yet: point a client at " + m.p.Addr() + ", and its requests add up here",
	}
	full, ok := m.view(m.prof.Root)
	if !ok {
		f.Root = screens.FlameNode{Name: "all"}
		if m.prof.Requests > 0 {
			f.Empty = "every request so far was a fetch polling for records: i shows them"
		}
		return f
	}
	base, _, zoom := at(full, m.zoom)
	sel := m.sel
	if len(sel) < len(zoom) || strings.Join(sel[:len(zoom)], "\x00") != strings.Join(zoom, "\x00") {
		sel = zoom // the selection always sits on or above the base
	}
	node, idx, sel := at(full, sel)
	f.Root, f.Sel = base, idx[len(zoom):]
	name := "all"
	if len(sel) > 0 {
		name = sel[len(sel)-1]
	}
	cf := m.frame(sel)
	f.Name = name
	f.Share = fmt.Sprintf("%.1f%% of time", node.Time/full.Time*100)
	f.Stats = fmt.Sprintf("%s · avg %s · p99 %s", plural(cf.N, "request"), screens.Dur(cf.Avg()), screens.Dur(cf.Quantile(0.99)))
	path := append([]string{"all"}, sel...)
	f.Path = strings.Join(path[:len(path)-1], " → ")
	if len(zoom) > 0 {
		f.Path += "   (zoomed into " + strings.Join(zoom, " → ") + ": esc to zoom out)"
	}
	return f
}

func (m profileModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w = 100
	}
	if h == 0 {
		h = 26
	}
	return tea.NewView(kit.Join(m.screen().Render(m.k, w, max(20, h-1))))
}
