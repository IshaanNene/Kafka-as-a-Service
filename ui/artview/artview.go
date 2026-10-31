// Package artview is a full-screen player for one ascii-magic piece. It backs
// `kaas art` and is the template for the TUI's animated screens.
package artview

import (
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/asciipack"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

type tickMsg time.Time

// Model plays a pack until a key is pressed or the time limit passes.
type Model struct {
	pack    *asciipack.Pack
	th      theme.Theme
	tint    bool
	start   time.Time
	now     time.Time
	limit   time.Duration // 0 = until a key
	reduced bool          // reduced motion: hold the first frame
	w, h    int
}

type Options struct {
	Tint          bool          // recolour the piece in the theme's hue
	Limit         time.Duration // stop after this long; 0 waits for a key
	ReducedMotion bool          // KAAS_REDUCED_MOTION: show a still frame
}

func New(p *asciipack.Pack, th theme.Theme, o Options) Model {
	now := time.Now()
	return Model{pack: p, th: th, tint: o.Tint, limit: o.Limit, reduced: o.ReducedMotion, start: now, now: now}
}

func (m Model) tick() tea.Cmd {
	if m.reduced && m.limit == 0 {
		return nil
	}
	return tea.Tick(time.Second/time.Duration(m.pack.FPS), func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m Model) Init() tea.Cmd { return m.tick() }

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.KeyPressMsg:
		if msg.String() == "t" {
			m.tint = !m.tint
			return m, nil
		}
		return m, tea.Quit
	case tickMsg:
		m.now = time.Time(msg)
		if m.limit > 0 && m.now.Sub(m.start) >= m.limit {
			return m, tea.Quit
		}
		return m, m.tick()
	}
	return m, nil
}

func (m Model) View() tea.View {
	elapsed := m.now.Sub(m.start)
	if m.reduced {
		elapsed = 0
	}
	o := asciipack.RenderOptions{Width: m.w, Height: max(0, m.h-2), Default: m.th.FG}
	if m.tint {
		o.Recolor = m.th.Tint
	}
	art := m.pack.Render(m.pack.FrameAt(elapsed), o)

	muted := lipgloss.NewStyle().Foreground(theme.C(m.th.Muted))
	accent := lipgloss.NewStyle().Foreground(theme.C(m.th.Accent))
	look := "own colours"
	if m.tint {
		look = m.th.Name
	}
	caption := accent.Render(m.pack.Name) + muted.Render("  ·  ascii-magic.com (MIT)  ·  "+look+"  ·  t tint · any key quits")

	content := lipgloss.JoinVertical(lipgloss.Center, art, "", caption)
	if m.w > 0 && m.h > 0 {
		content = lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, content)
	}
	v := tea.NewView(content)
	v.AltScreen = true
	v.BackgroundColor = theme.C(m.th.BG)
	v.WindowTitle = "kaas · " + m.pack.Name
	return v
}
