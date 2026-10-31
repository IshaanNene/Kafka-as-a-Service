package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Node is one row in a failure page's rack: a seed broker and the dots for
// each stage it reached.
type Node struct {
	Name string
	Dots []kit.Status
}

// Pill is one status entry under the facts: "● dns  ok".
type Pill struct {
	Label  string
	Status kit.Status
	State  string
}

// Failure is the 500 page: a rack of nodes on the left, a big status code,
// what went wrong, the facts behind it and what to do next.
type Failure struct {
	Code      string // "503"
	Title     string
	Subtitle  string
	Rack      string   // rack title, e.g. "seeds"
	Legend    []string // one letter per dot column: d t a m
	Nodes     []Node
	RackFoot  string
	Facts     [][2]string
	Pills     []Pill
	Actions   []string // rendered as buttons; the first is the primary one
	NoActions bool
}

// Render draws the page; below ~76 columns the rack moves under the text.
func (f Failure) Render(k kit.Kit, width int) []string {
	rack := f.rack(k)
	rw := kit.BlockWidth(rack)
	side := width-rw-8 >= 48
	textW := min(width-4, 56)
	if side {
		textW = min(width-rw-8, 56)
	}
	right := []string{}
	right = append(right, k.Big(f.Code, kit.Accent)...)
	right = append(right, "", k.B(kit.FG, f.Title))
	for _, l := range kit.Wrap(f.Subtitle, textW) {
		right = append(right, k.S(kit.Muted, l))
	}
	if len(f.Facts) > 0 {
		right = append(right, "")
		kw := 0
		for _, r := range f.Facts {
			kw = max(kw, kit.Width(r[0]))
		}
		// Long values (error strings) wrap under themselves, not under the key.
		for _, r := range f.Facts {
			for i, l := range kit.Wrap(r[1], max(textW-kw-2, 16)) {
				key := ""
				if i == 0 {
					key = r[0]
				}
				right = append(right, k.S(kit.Muted, kit.Fit(key, kw))+"  "+k.S(kit.FG, l))
			}
		}
	}
	if len(f.Pills) > 0 {
		right = append(right, "")
		right = append(right, pills(k, f.Pills, 2)...)
	}
	if len(f.Actions) > 0 {
		right = append(right, "")
		right = append(right, buttons(k, f.Actions, textW)...)
	}

	var out []string
	if side {
		out = kit.Beside(4, rack, right)
	} else {
		out = append(append(out, right...), "")
		out = append(out, rack...)
	}
	return kit.Clip(width, append([]string{""}, kit.Indent(2, out)...))
}

// rack draws the heavy-framed server rack: each node is a drive bay whose
// lower edge carries its name and one dot per stage.
func (f Failure) rack(k kit.Kit) []string {
	if len(f.Nodes) == 0 {
		return nil
	}
	nw := 8
	for _, n := range f.Nodes {
		nw = max(nw, kit.Width(n.Name))
	}
	nw = min(nw, 24)
	inner := nw + 2*len(f.Legend) + 5
	legend := ""
	for _, l := range f.Legend {
		legend += k.S(kit.Muted, l) + k.S(kit.Rule, "─")
	}
	var body []string
	for _, n := range f.Nodes {
		top := k.S(kit.Rule, "┌─") + k.S(kit.Rule, strings.Repeat("▒", nw)) + k.S(kit.Rule, "──") + legend + k.S(kit.Rule, "┐")
		name := kit.Fit(n.Name, nw)
		name = strings.TrimRight(name, " ")
		row := k.S(kit.Rule, "└─") + k.S(kit.Accent2, name) + k.S(kit.Rule, strings.Repeat("─", nw-kit.Width(name)+2))
		for _, d := range n.Dots {
			row += k.Dot(d) + k.S(kit.Rule, "─")
		}
		for range len(f.Legend) - len(n.Dots) {
			row += k.Dot(kit.Pending) + k.S(kit.Rule, "─")
		}
		row += k.S(kit.Rule, "┘")
		body = append(body, top, row)
	}
	body = append(body, "", " "+k.S(kit.Muted, f.RackFoot))
	return k.Draw(kit.Box{Border: kit.Heavy, Role: kit.Muted, Width: inner + 2}, body)
}

func pills(k kit.Kit, ps []Pill, perRow int) []string {
	lw, sw := 0, 0
	for _, p := range ps {
		lw, sw = max(lw, kit.Width(p.Label)), max(sw, kit.Width(p.State))
	}
	var out []string
	for i := 0; i < len(ps); i += perRow {
		var line []string
		for _, p := range ps[i:min(i+perRow, len(ps))] {
			line = append(line, k.Dot(p.Status)+" "+k.S(kit.FG, kit.Fit(p.Label, lw))+"  "+k.S(kit.Muted, kit.Fit(p.State, sw)))
		}
		out = append(out, strings.Join(line, "   "))
	}
	return out
}

// buttons lays commands out as rounded buttons, wrapping onto a new row when
// they would pass w.
func buttons(k kit.Kit, actions []string, w int) []string {
	var rows [][]string
	var cur [][]string
	used := 0
	for i, a := range actions {
		role := kit.Muted
		label := k.S(kit.Accent2, a)
		if i == 0 {
			role, label = kit.Accent, k.B(kit.Accent, a)
		}
		b := k.Button(label, 0, role)
		bw := kit.BlockWidth(b)
		if used > 0 && used+2+bw > w {
			rows = append(rows, kit.Beside(2, cur...))
			cur, used = nil, 0
		}
		cur = append(cur, b)
		used += bw + 2
	}
	rows = append(rows, kit.Beside(2, cur...))
	var out []string
	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}

// NotFound is the 404 page for an unknown command, topic or other name.
type NotFound struct {
	Kind        string // "command", "topic"
	Name        string
	Where       string // "kaas", "cluster v0kwmoCz…"
	Suggestions []Item
	Actions     []string
}

// Render draws the big 404 in a field of stars, the name that missed and the
// closest real names.
func (n NotFound) Render(k kit.Kit, width int) []string {
	stars := func(pattern string) string {
		var sb strings.Builder
		for _, r := range pattern {
			switch r {
			case '*':
				sb.WriteString(k.S(kit.Accent2, "✦"))
			case '.':
				sb.WriteString(k.S(kit.Rule, "·"))
			default:
				sb.WriteRune(r)
			}
		}
		return sb.String()
	}
	out := []string{"", stars("   .         *              .        .")}
	out = append(out, kit.Indent(2, k.Big("404", kit.Accent))...)
	out = append(out, stars("      .              .          *"), "")
	out = append(out, "  "+k.S(kit.FG, "unknown "+n.Kind+" ")+k.B(kit.Err, `"`+n.Name+`"`))
	out = append(out, "  "+k.S(kit.Muted, n.Where+" has no "+n.Kind+" by that name."))
	if len(n.Suggestions) > 0 {
		out = append(out, "", "  "+k.S(kit.Accent, spaced("did you mean")))
		nw := 0
		for _, s := range n.Suggestions {
			nw = max(nw, kit.Width(s.Name))
		}
		for i, s := range n.Suggestions {
			mark := "  "
			if i == 0 {
				mark = k.S(kit.Accent, "▸ ")
			}
			out = append(out, "  "+mark+k.S(kit.Accent2, kit.Fit(s.Name, nw))+"  "+k.S(kit.Muted, s.Desc))
		}
	}
	if len(n.Actions) > 0 {
		out = append(out, "")
		out = append(out, kit.Indent(2, buttons(k, n.Actions, width-4))...)
	}
	return kit.Clip(width, out)
}

// Field is one input on the validation screen: a flag or an argument.
type Field struct {
	Label    string // "--output"
	Required bool
	Value    string
	State    kit.Status // Good, Degraded (the bad one), Pending (unset)
	Msg      string     // shown under a bad field
	Focus    bool       // drawn heavy, as the template does for the active input
}

// Invalid is the form-validation screen for bad flags and arguments.
type Invalid struct {
	Title  string // "kaas snapshot"
	Fields []Field
	Action string // the button: usually "kaas snapshot --help"
}

// Render draws the form: each field boxed with its verdict, messages under
// the bad ones, and a count of what needs attention.
func (v Invalid) Render(k kit.Kit, width int) []string {
	w := min(width, 72)
	fw := w - 18
	bad := 0
	var body []string
	for _, f := range v.Fields {
		label := k.S(kit.FG, f.Label)
		if f.Required {
			label += k.S(kit.Accent, " *")
		}
		body = append(body, "", " "+label)
		role, border := kit.Muted, kit.Rounded
		if f.Focus {
			role, border = kit.Accent, kit.Heavy
		}
		val := k.S(kit.Accent2, f.Value)
		if f.Value == "" {
			val = k.S(kit.Muted, "unset")
		}
		in := spread(val, k.Mark(f.State), fw-4)
		if f.State == kit.Degraded {
			bad++
			in = spread(k.S(kit.Err, f.Value), k.Mark(kit.Degraded), fw-4)
		}
		body = append(body, kit.Indent(1, k.Draw(kit.Box{Role: role, Border: border, Width: fw, Pad: 1}, []string{in}))...)
		if f.Msg != "" {
			for _, l := range kit.Wrap(f.Msg, fw-4) {
				body = append(body, "   "+k.S(kit.Err, "✗ ")+k.S(kit.Err, l))
			}
		}
	}
	body = append(body, "")
	btn := k.Button(k.B(kit.Accent, v.Action), 0, kit.Accent)
	need := k.S(kit.Warn, "▲ "+plural(bad, "field")+" need"+map[bool]string{true: "s", false: ""}[bad == 1]+" attention")
	body = append(body, kit.Indent(1, kit.Beside(3, btn, []string{"", need}))...)
	for _, f := range v.Fields {
		if f.Required {
			body = append(body, " "+k.S(kit.Muted, "* required"))
			break
		}
	}
	return kit.Clip(width, k.Draw(kit.Box{Title: k.B(kit.Accent, v.Title), Right: k.S(kit.Err, "invalid input"), Role: kit.Rule, Width: w, Pad: 1}, body))
}
