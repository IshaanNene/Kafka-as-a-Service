package tui

import (
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// Frame renders one still frame of the TUI over a snapshot, without an
// engine or a terminal, after pressing keys ("enter", "esc", or text typed
// rune by rune). The docs' screenshots are made with it.
func Frame(th theme.Theme, s *snapshot.Snapshot, events []engine.Event, now time.Time, w, h int, keys ...string) string {
	m := newModel(th, Options{ReducedMotion: true})
	m.now = now
	step := func(msg tea.Msg) {
		next, _ := m.Update(msg)
		m = next.(Model)
	}
	step(tea.WindowSizeMsg{Width: w, Height: h})
	step(updateMsg{Snapshot: s, Events: events})
	for _, k := range keys {
		switch k {
		case "enter":
			step(tea.KeyPressMsg{Code: tea.KeyEnter})
		case "esc":
			step(tea.KeyPressMsg{Code: tea.KeyEscape})
		default:
			for _, r := range k {
				step(tea.KeyPressMsg{Code: r, Text: string(r)})
			}
		}
	}
	return m.View().Content
}
