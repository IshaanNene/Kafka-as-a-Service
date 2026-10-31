// Package kit draws the pieces every kaas screen is built from: boxes,
// dotted rules, block digits, buttons and status glyphs, in the style of the
// ascii-magic-art UI pieces (MIT, see ui/asciipack/packs/LICENSE.ascii-magic-art).
//
// Everything works on blocks: a block is a slice of lines that may carry ANSI
// styling. Widths are measured in terminal cells, so styled and wide runes
// line up. With Color off a Kit emits no escape codes at all, which is what
// the golden tests and NO_COLOR terminals see.
package kit

import (
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// Role names a colour in the theme, so screens never hard-code one.
type Role int

const (
	FG Role = iota
	Muted
	Accent
	Accent2
	OK
	Warn
	Err
	Info
	Rule
	Sel
)

// Kit styles text for one theme.
type Kit struct {
	Theme theme.Theme
	Color bool
}

// New returns a kit for th; color false strips all styling.
func New(th theme.Theme, color bool) Kit { return Kit{Theme: th, Color: color} }

func (k Kit) style(r Role) lipgloss.Style {
	t := k.Theme
	hex := map[Role]string{
		FG: t.FG, Muted: t.Muted, Accent: t.Accent, Accent2: t.Accent2, OK: t.OK,
		Warn: t.Warn, Err: t.Err, Info: t.Info, Rule: t.Rule, Sel: t.Accent2,
	}[r]
	s := lipgloss.NewStyle().Foreground(theme.C(hex))
	if r == Sel {
		s = s.Background(theme.C(t.Selection))
	}
	return s
}

// S paints s in role r.
func (k Kit) S(r Role, s string) string {
	if !k.Color || s == "" {
		return s
	}
	return k.style(r).Render(s)
}

// B paints s in role r, bold.
func (k Kit) B(r Role, s string) string {
	if !k.Color || s == "" {
		return s
	}
	return k.style(r).Bold(true).Render(s)
}

// Width is the cell width of s, ignoring escape codes.
func Width(s string) int { return ansi.StringWidth(s) }

// Fit pads or truncates s to exactly w cells.
func Fit(s string, w int) string {
	if w <= 0 {
		return ""
	}
	if n := Width(s); n > w {
		return ansi.Truncate(s, w, "…")
	} else if n < w {
		return s + strings.Repeat(" ", w-n)
	}
	return s
}

// BlockWidth is the widest line in b.
func BlockWidth(b []string) int {
	w := 0
	for _, l := range b {
		w = max(w, Width(l))
	}
	return w
}

// Beside lays blocks out left to right with gap cells between them, padding
// shorter blocks with blank lines.
func Beside(gap int, blocks ...[]string) []string {
	h := 0
	for _, b := range blocks {
		h = max(h, len(b))
	}
	out := make([]string, h)
	for i, b := range blocks {
		w := BlockWidth(b)
		for y := range h {
			line := ""
			if y < len(b) {
				line = b[y]
			}
			if i < len(blocks)-1 {
				line = Fit(line, w) + strings.Repeat(" ", gap)
			}
			out[y] += line
		}
	}
	return out
}

// Indent shifts every line of b right by n cells.
func Indent(n int, b []string) []string {
	pad := strings.Repeat(" ", n)
	out := make([]string, len(b))
	for i, l := range b {
		if l != "" {
			l = pad + l
		}
		out[i] = l
	}
	return out
}

// Clip truncates every line of b to w cells.
func Clip(w int, b []string) []string {
	out := make([]string, len(b))
	for i, l := range b {
		if Width(l) > w {
			l = ansi.Truncate(l, w, "…")
		}
		out[i] = l
	}
	return out
}

// Join renders b as one string with a trailing newline.
func Join(b []string) string {
	var sb strings.Builder
	for _, l := range b {
		sb.WriteString(strings.TrimRight(l, " "))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// Border picks a box's line set.
type Border int

const (
	Rounded Border = iota // ╭─╮ inputs, buttons, panels
	Heavy                 // ┏━┓ focused input, hardware panels
)

// Box frames body in a border of outer width w. A title sits in the top edge
// as "╭─ Title ───╮". Body lines are clipped to fit.
type Box struct {
	Title  string // already styled is fine
	Lead   string // drawn flush after the corner instead of a title, e.g. ●─●─●
	Right  string // optional text at the right of the top edge
	Border Border
	Role   Role // border colour
	Width  int  // outer width; 0 fits the body
	Pad    int  // blank cells inside each side
}

// Draw frames body.
func (k Kit) Draw(b Box, body []string) []string {
	tl, tr, bl, br, h, v := "╭", "╮", "╰", "╯", "─", "│"
	if b.Border == Heavy {
		tl, tr, bl, br, h, v = "┏", "┓", "┗", "┛", "━", "┃"
	}
	inner := b.Width - 2
	if b.Width == 0 {
		inner = BlockWidth(body) + 2*b.Pad
		if t := Width(b.Title) + Width(b.Lead) + Width(b.Right) + 6; t > inner {
			inner = t
		}
	}
	inner = max(inner, 2)
	top := tl + h
	used := 1
	switch {
	case b.Lead != "":
		top = k.S(b.Role, tl+h) + b.Lead
		used = Width(b.Lead) + 1
	case b.Title != "":
		top = k.S(b.Role, tl+h+" ") + b.Title + k.S(b.Role, " ")
		used = Width(b.Title) + 3
	default:
		top = k.S(b.Role, top)
	}
	right := ""
	if b.Right != "" {
		right = k.S(b.Role, " ") + b.Right + k.S(b.Role, " "+h)
	}
	fill := inner - used - Width(right)
	if fill < 0 {
		right, fill = "", max(0, inner-used)
	}
	top += k.S(b.Role, strings.Repeat(h, fill)) + right + k.S(b.Role, tr)
	out := []string{ansi.Truncate(top, inner+2, "")}
	pad := strings.Repeat(" ", b.Pad)
	for _, l := range body {
		out = append(out, k.S(b.Role, v)+pad+Fit(l, inner-2*b.Pad)+pad+k.S(b.Role, v))
	}
	return append(out, k.S(b.Role, bl+strings.Repeat(h, inner)+br))
}

// Divider is a full-width rule inside a box of outer width w: "│───│".
func (k Kit) Divider(w int, r Role) string {
	return k.S(r, "│"+strings.Repeat("─", max(0, w-2))+"│")
}

// Rule is the dotted separator kaas uses between sections.
func (k Kit) Rule(w int) string {
	return k.S(Rule, strings.TrimRight(strings.Repeat("· ", (w+1)/2), " "))
}

// Button is a rounded box around a label, padded to w cells wide.
func (k Kit) Button(label string, w int, r Role) []string {
	return k.Draw(Box{Role: r, Width: max(w, Width(label)+4)}, []string{" " + label})
}

// Status is the state behind a dot.
type Status int

const (
	Pending Status = iota
	Good
	Degraded
	Down
	Skipped
)

// Dot is a status dot: ● in the state's colour, ○ for pending or skipped.
// Without colour, degraded and down get their own shapes.
func (k Kit) Dot(s Status) string {
	if !k.Color {
		// Without colour the glyph itself must carry the state.
		switch s {
		case Degraded:
			return "◐"
		case Down:
			return "✕"
		}
	}
	switch s {
	case Good:
		return k.S(OK, "●")
	case Degraded:
		return k.S(Warn, "●")
	case Down:
		return k.S(Err, "●")
	default:
		return k.S(Muted, "○")
	}
}

// Mark is the one-cell verdict glyph used beside fields and steps.
func (k Kit) Mark(s Status) string {
	switch s {
	case Good:
		return k.S(OK, "✓")
	case Degraded:
		return k.S(Warn, "!")
	case Down:
		return k.S(Err, "✗")
	default:
		return k.S(Muted, "·")
	}
}

// KV aligns key/value rows: keys muted, padded to the widest key.
func (k Kit) KV(rows [][2]string) []string {
	kw := 0
	for _, r := range rows {
		kw = max(kw, Width(r[0]))
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = k.S(Muted, Fit(r[0], kw)) + "  " + r[1]
	}
	return out
}

// Wrap breaks plain text into lines of at most w cells on spaces.
func Wrap(s string, w int) []string {
	var out []string
	for _, para := range strings.Split(s, "\n") {
		line := ""
		for _, word := range strings.Fields(para) {
			switch {
			case line == "":
				line = word
			case Width(line)+1+Width(word) <= w:
				line += " " + word
			default:
				out = append(out, line)
				line = word
			}
		}
		out = append(out, line)
	}
	return out
}
