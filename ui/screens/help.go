// Package screens renders the full-screen pieces every kaas command shares:
// help, examples, error pages and the connect sequence. Each is a port of an
// ascii-magic-art UI piece's layout, drawn live from real data instead of
// replayed: help is the command palette, examples are a terminal session,
// failures are the 500 page, unknown names the 404 page, bad flags the form
// validation screen, and connecting is the boot sequence.
//
// Screens return blocks (see ui/kit) sized to the width they are given, so
// callers decide where they go: stdout for help, stderr for errors.
package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Item is one row of the palette: a command or a flag.
type Item struct {
	Name string // "watch", "-t, --topic strings"
	Desc string
	Hint string // right-aligned: a default value, an alias, "default"
}

// Example is one invocation shown in the terminal-session box.
type Example struct {
	Comment string // optional, rendered as a # line above the command
	Cmd     string // without the "$ "
}

// Help is a command's usage, as the palette shows it.
type Help struct {
	Path     string // "kaas watch"
	Version  string
	Short    string
	Long     string
	Usage    string // "kaas watch [flags]"
	Commands []Item
	Flags    []Item
	Global   []Item
	Examples []Example
	Prompt   string // terminal title, e.g. "you@host: ~/src"
}

// maxWidth keeps help readable on very wide terminals.
const maxWidth = 100

// Render draws the palette and, if there are examples, the session below it.
func (h Help) Render(k kit.Kit, width int) []string {
	w := min(width, maxWidth)
	inner := w - 2

	n := len(h.Commands)
	var counts []string
	if n > 0 {
		counts = append(counts, plural(n, "command"))
	}
	if f := len(h.Flags) + len(h.Global); f > 0 {
		counts = append(counts, plural(f, "flag"))
	}
	query := k.S(kit.Accent, "> ") + k.B(kit.FG, h.Path) + k.S(kit.Accent, "█")
	right := k.S(kit.Muted, strings.Join(counts, " · "))
	body := []string{" " + spread(query, right, inner-2) + " ", k.S(kit.Rule, strings.Repeat("─", inner))}

	text := h.Long
	if text == "" {
		text = h.Short
	}
	for _, l := range kit.Wrap(text, inner-4) {
		body = append(body, "  "+k.S(kit.FG, l))
	}

	selected := true
	section := func(title string, items []Item) {
		if len(items) == 0 {
			return
		}
		body = append(body, "", " "+k.S(kit.Accent, spaced(title)))
		nw := 0
		for _, it := range items {
			nw = max(nw, kit.Width(it.Name))
		}
		nw = min(nw, inner/3)
		for _, it := range items {
			body = append(body, h.row(k, it, nw, inner, selected))
			selected = false
		}
	}
	section("commands", h.Commands)
	section("flags", h.Flags)
	section("global flags", h.Global)
	body = append(body, "", k.S(kit.Rule, strings.Repeat("─", inner)))

	foot := k.S(kit.Accent, "◆ ") + k.S(kit.FG, "kaas "+h.Version) + k.S(kit.Muted, "   "+h.Usage)
	hint := k.S(kit.Accent2, "kaas help <command>") + k.S(kit.Muted, "  more")
	if n == 0 {
		hint = k.S(kit.Accent2, "-h") + k.S(kit.Muted, "  this screen")
	}
	body = append(body, " "+spread(foot, hint, inner-2)+" ")

	out := k.Draw(kit.Box{Title: k.B(kit.Accent, h.Path), Right: k.S(kit.Muted, "usage"), Role: kit.Rule, Width: w}, body)
	if len(h.Examples) > 0 {
		out = append(out, "")
		out = append(out, Session(k, h.Prompt, h.Examples, w)...)
	}
	return kit.Clip(width, out)
}

// row draws one palette entry. The first entry is drawn selected, the way the
// palette highlights its top result: ▌ bar, ░ fill, hint at the far right.
func (h Help) row(k kit.Kit, it Item, nw, inner int, selected bool) string {
	hw := kit.Width(it.Hint)
	dw := max(0, inner-nw-hw-8)
	desc := kit.Fit(it.Desc, dw)
	if selected {
		fill := func(s string) string { return strings.ReplaceAll(s, " ", "░") }
		line := "░" + kit.Fit(strings.TrimLeft(it.Name, " "), nw) + "░░" + desc + "░░" + it.Hint + "░"
		line = kit.Fit(fill(line), inner-2)
		return " " + k.S(kit.Accent, "▌") + k.S(kit.Sel, line)
	}
	left := "   " + k.S(kit.Accent2, kit.Fit(it.Name, nw)) + "  " + k.S(kit.FG, desc)
	return spread(left, k.S(kit.Muted, it.Hint)+"  ", inner)
}

// Session draws examples as a terminal session: a titled window with
// "$ command" prompts and # comments.
func Session(k kit.Kit, prompt string, ex []Example, w int) []string {
	title := k.S(kit.Err, "●") + k.S(kit.Rule, "─") + k.S(kit.Warn, "●") + k.S(kit.Rule, "─") + k.S(kit.OK, "●")
	var body []string
	for i, e := range ex {
		if i > 0 && e.Comment != "" {
			body = append(body, "")
		}
		if e.Comment != "" {
			body = append(body, k.S(kit.Muted, "# "+e.Comment))
		}
		body = append(body, k.S(kit.Accent, "$ ")+k.S(kit.FG, e.Cmd))
	}
	return k.Draw(kit.Box{Lead: title, Role: kit.Rule, Width: w, Pad: 1, Right: k.S(kit.Muted, prompt)}, body)
}

// spread puts left and right at either end of w cells.
func spread(left, right string, w int) string {
	gap := w - kit.Width(left) - kit.Width(right)
	if gap < 1 {
		return kit.Fit(left, w)
	}
	return left + strings.Repeat(" ", gap) + right
}

// spaced letter-spaces a heading: "flags" → "F L A G S".
func spaced(s string) string {
	return strings.Join(strings.Split(strings.ToUpper(s), ""), " ")
}
