package screens

import (
	"fmt"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// EditorTab is one buffer tab: the piece's open files.
type EditorTab struct {
	Name  string
	Dirty bool // edited since the last send: the piece's ●
}

// Editor languages, which pick the highlighting.
const (
	LangJSON    = "JSON"
	LangText    = "Text"
	LangHeaders = "Headers"
)

// Editor is the code-editor piece for composing a record: a window with
// buffer tabs, a breadcrumb, numbered lines in syntax colours with a
// minimap, and a status bar with the topic, problems and the cursor.
type Editor struct {
	Title       string // centred in the frame: "orders · kaas produce"
	Tabs        []EditorTab
	Active      int
	Path        string // breadcrumb: "orders / value"
	Lines       []string
	Line, Col   int // cursor, from 0; Col counts runes
	Cursor      bool
	Lang        string
	Branch      string // the topic, as the piece's ◆ main
	Fine        string // "valid JSON", when there is no problem
	Problem     string // the first problem, red in the status bar
	ProblemLine int    // its line from 1, marked in the gutter; 0 for none
	Notice      string // the last send, green
	Hint        string // under the frame
}

// strColour is the piece's string colour, #a5d6ff; no theme role has it.
const strColour = "#a5d6ff"

// Render draws the editor at width × height.
func (e Editor) Render(k kit.Kit, width, height int) []string {
	w := max(30, width)
	inner := w - 2
	mapW := 0
	if inner >= 70 {
		mapW = 16 // the minimap and its rule
	}
	codeW := inner - 6 - mapW // gutter: space, three digits, two spaces
	vis := max(3, height-8)
	if e.Hint == "" {
		vis++
	}

	// Tabs, with the active one white and underlined in blue.
	var tabs strings.Builder
	under := k.S(kit.Faint, "─")
	used := 1
	for i, t := range e.Tabs {
		label := "• " + t.Name
		role := kit.Muted
		if i == e.Active {
			role = kit.FG
		}
		cell := k.S(role, label)
		if t.Dirty {
			cell += " " + k.S(role, "●")
			label += " ●"
		}
		if used+kit.Width(label)+3 > inner {
			break
		}
		tabs.WriteString(" " + cell + "  ")
		bar := kit.Faint
		if i == e.Active {
			bar = kit.Accent
		}
		under += k.S(bar, strings.Repeat(map[bool]string{true: "━", false: "─"}[i == e.Active], kit.Width(label)+1)) + k.S(kit.Faint, "──")
		used += kit.Width(label) + 3
	}
	under += k.S(kit.Faint, strings.Repeat("─", max(0, inner-used)))
	body := []string{tabs.String(), under, " " + k.S(kit.Rule, kit.Fit(e.Path, inner-2))}

	// Lines, scrolled so the cursor stays in view, and shifted left when
	// the cursor is past the right edge.
	top := max(0, e.Line-vis+2)
	shift := max(0, e.Col-codeW+4)
	for r := range vis {
		li := top + r
		if li >= len(e.Lines) {
			body = append(body, "")
			continue
		}
		num := k.S(kit.Rule, fmt.Sprintf("%3d", li+1))
		switch {
		case li+1 == e.ProblemLine:
			num = k.S(kit.Err, fmt.Sprintf("%3d", li+1))
		case li == e.Line:
			num = k.S(kit.FG, fmt.Sprintf("%3d", li+1))
		}
		code := e.paint(k, e.Lines[li], li, shift, codeW)
		row := " " + num + "  " + code
		if mapW > 0 {
			row = kit.Fit(row, inner-mapW) + k.S(kit.Faint, "│") + " " + e.minimap(k, e.Lines[li], li == e.Line, mapW-2)
		}
		body = append(body, row)
	}

	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	body = append(body, " "+e.status(k, inner-2))
	out := k.Draw(kit.Box{Title: e.title(k, inner), Role: kit.Rule, Width: w}, body)
	if e.Hint != "" {
		for _, l := range wrapParts(e.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}

// title is the piece's three window dots, then the name, muted.
func (e Editor) title(k kit.Kit, inner int) string {
	dots := k.S(kit.Err, "●") + " " + k.S(kit.Warn, "●") + " " + k.S(kit.OK, "●")
	if e.Title == "" || kit.Width(e.Title)+12 > inner {
		return dots
	}
	return dots + "  " + k.S(kit.Muted, " "+e.Title+" ")
}

// paint highlights one line from rune shift for w cells, with the cursor.
func (e Editor) paint(k kit.Kit, line string, li, shift, w int) string {
	rs := []rune(line)
	colours := highlight(rs, e.Lang)
	var b strings.Builder
	cells := 0
	for i := shift; i < len(rs) && cells < w; i++ {
		ch := string(rs[i])
		if rs[i] == '\t' {
			ch = " "
		}
		switch {
		case e.Cursor && li == e.Line && i == e.Col:
			b.WriteString(k.S(kit.Sel, ch))
		case colours[i].hex != "":
			b.WriteString(k.Hex(colours[i].hex, ch))
		default:
			b.WriteString(k.S(colours[i].role, ch))
		}
		cells++
	}
	if e.Cursor && li == e.Line && e.Col >= len(rs) && e.Col-shift < w {
		b.WriteString(strings.Repeat(" ", max(0, e.Col-max(shift, len(rs)))) + k.S(kit.Accent, "▌"))
	}
	return b.String()
}

// minimap is the piece's outline of the line: a ▪ per four characters,
// indented by half the line's indent.
func (e Editor) minimap(k kit.Kit, line string, current bool, w int) string {
	trimmed := strings.TrimLeft(line, " \t")
	indent := (len(line) - len(trimmed)) / 2
	n := min(12, (len([]rune(strings.TrimSpace(line)))+3)/4)
	if indent+n > w {
		n = max(0, w-indent)
	}
	role := kit.Faint
	if current {
		role = kit.Muted
	}
	return strings.Repeat(" ", min(indent, w)) + k.S(role, strings.Repeat("▪", n))
}

// status is the piece's bar: ◆ branch, then problems, then the cursor and
// the file's settings, dropping from the right when narrow.
func (e Editor) status(k kit.Kit, w int) string {
	left := k.S(kit.Accent2, "◆ "+e.Branch)
	switch {
	case e.Problem != "":
		left += "  " + k.S(kit.Err, "✗ "+e.Problem)
	case e.Fine != "":
		left += "  " + k.S(kit.OK, "✓ "+e.Fine)
	}
	if e.Notice != "" {
		left += "  " + k.S(kit.OK, e.Notice)
	}
	for _, right := range []string{
		fmt.Sprintf("Ln %d, Col %d   Spaces: 2   UTF-8   %s", e.Line+1, e.Col+1, e.Lang),
		fmt.Sprintf("Ln %d, Col %d   %s", e.Line+1, e.Col+1, e.Lang),
		fmt.Sprintf("%d:%d", e.Line+1, e.Col+1),
	} {
		if kit.Width(left)+kit.Width(right)+2 <= w {
			return spread(left, k.S(kit.Muted, right), w)
		}
	}
	if kit.Width(left) > w {
		return kit.Fit(left, w)
	}
	return left
}

// paintColour is one rune's colour: a role, or the string hex.
type paintColour struct {
	role kit.Role
	hex  string
}

// highlight colours a line the way the piece colours TypeScript, mapped to
// JSON: object keys purple as the piece's calls, strings light blue,
// numbers and true, false and null blue, punctuation grey.
func highlight(rs []rune, lang string) []paintColour {
	out := make([]paintColour, len(rs))
	switch lang {
	case LangHeaders:
		eq := -1
		for i, r := range rs {
			if r == '=' {
				eq = i
				break
			}
		}
		for i := range rs {
			switch {
			case eq < 0 || i < eq:
				out[i].role = kit.Accent2
			case i == eq:
				out[i].role = kit.Err
			default:
				out[i].hex = strColour
			}
		}
		return out
	case LangJSON:
	default:
		return out // FG
	}
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
		case r == '"':
			j := i + 1
			for j < len(rs) && rs[j] != '"' {
				if rs[j] == '\\' {
					j++
				}
				j++
			}
			end := min(j, len(rs)-1)
			// A string followed by a colon is a key.
			n := end + 1
			for n < len(rs) && rs[n] == ' ' {
				n++
			}
			c := paintColour{hex: strColour}
			if n < len(rs) && rs[n] == ':' {
				c = paintColour{role: kit.Accent2}
			}
			for x := i; x <= end; x++ {
				out[x] = c
			}
			i = end + 1
		case r == '-' || (r >= '0' && r <= '9'):
			j := i
			for j < len(rs) && strings.ContainsRune("-+.eE0123456789", rs[j]) {
				out[j].role = kit.Accent
				j++
			}
			i = j
		case r >= 'a' && r <= 'z':
			j := i
			for j < len(rs) && rs[j] >= 'a' && rs[j] <= 'z' {
				j++
			}
			role := kit.Err // an unquoted word is not JSON
			switch string(rs[i:j]) {
			case "true", "false", "null":
				role = kit.Accent
			}
			for x := i; x < j; x++ {
				out[x].role = role
			}
			i = j
		case strings.ContainsRune("{}[]:,", r):
			out[i].role = kit.Muted
			i++
		default:
			i++
		}
	}
	return out
}
