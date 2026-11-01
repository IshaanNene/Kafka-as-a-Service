package screens

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func modalFixture(k kit.Kit, typed string, focus int) Modal {
	return Modal{
		Title: "Delete topic?",
		Body: []string{
			k.S(kit.Muted, "This permanently deletes ") + k.S(kit.FG, "orders"),
			k.S(kit.Muted, "with its 251,038 records (609 MiB) in 4 partitions."),
			k.S(kit.Warn, "billing and audit still read it."),
			k.S(kit.Err, "This cannot be undone."),
		},
		Confirm: "orders", Typed: typed, Focus: focus, Action: "Delete topic",
	}
}

func TestModalGoldens(t *testing.T) {
	k, _ := kits(t)
	page := Dim(k, partitionsFixture().Render(k, 90))
	golden(t, "modal", Overlay(page, modalFixture(k, "ord", FocusInput).Render(k, 60), 90, len(page)))
	golden(t, "modal_armed", modalFixture(k, "orders", FocusAction).Render(k, 60))

	open := Trash{Title: "Deleting orders in 4s", Detail: "251,038 records · nothing sent yet", Undo: 0.8, Secs: 4, Hint: "u or esc to undo"}
	golden(t, "trash", open.Render(k, 56))
	shut := Trash{Title: "Topic deleted", Detail: `"orders" and its 251,038 records are gone.`, Shut: true}
	golden(t, "trash_shut", shut.Render(k, 56))
}

func TestModalArmsOnTheExactName(t *testing.T) {
	k, _ := kits(t)
	for _, tc := range []struct {
		typed string
		armed bool
	}{{"orders", true}, {"order", false}, {"Orders", false}, {"orders\t", false}} {
		if got := modalFixture(k, tc.typed, FocusAction).Armed(); got != tc.armed {
			t.Errorf("typed %q armed %v, want %v", tc.typed, got, tc.armed)
		}
	}
	// A long name scrolls in the input rather than widening the dialog.
	m := modalFixture(k, strings.Repeat("x", 100), FocusInput)
	for _, l := range m.Render(k, 60) {
		if w := kit.Width(l); w != 60 {
			t.Fatalf("line %q is %d wide", ansi.Strip(l), w)
		}
	}
}

func TestOverlayKeepsThePageAround(t *testing.T) {
	base := []string{"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"}
	got := Overlay(base, []string{"XX"}, 10, 3)
	if got[0] != base[0] || got[1] != "bbbbXXbbbb" || got[2] != base[2] {
		t.Errorf("overlay = %q", got)
	}
}
