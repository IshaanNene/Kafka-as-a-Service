package screens

import (
	"strconv"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Step is one stop on the stepper's rail.
type Step struct {
	Name string
	Done bool
}

// Stepper is the onboarding-stepper piece: a framed wizard with the steps
// on a rail at the left, ticked as they complete, and the current step's
// form at the right above Back and Continue.
type Stepper struct {
	Title   string // on the frame: "Create topic"
	Steps   []Step
	Current int
	Heading string // the step's question: "Name your topic"
	Body    []string
	Back    bool   // the step has a step before it
	Button  string // "Continue →"; "" for none
	// Pressed draws the button green, for the finished step.
	Pressed bool
	Hint    string // under the frame
}

// Rail widths: the piece's 22 columns less the frame, or just the marks
// when the form would be narrower than its fields.
const (
	railW    = 21
	railMark = 6
	formMin  = 46
)

// frame is the stepper's outer width and its rail's at width.
func frame(width int) (w, rail int) {
	w = min(max(width, 36), 90)
	rail = railW
	if w-2-railW-3 < formMin {
		rail = railMark
	}
	return w, rail
}

// BodyWidth is how wide the form is at width, for sizing Input and Card.
func (s Stepper) BodyWidth(width int) int {
	w, rail := frame(width)
	return w - 2 - rail - 3
}

// Render draws the stepper at width × height.
func (s Stepper) Render(k kit.Kit, width, height int) []string {
	w, railW := frame(width)
	inner := w - 2
	hint := wrapParts(s.Hint, w-2)
	if s.Hint == "" {
		hint = nil
	}
	rows := max(12, height-2-len(hint))
	rail := s.rail(k, rows, railW)
	rw := inner - railW - 3

	right := []string{"", k.S(kit.FG, kit.Fit(s.Heading, rw)), k.S(kit.Faint, strings.Repeat("─", rw)), ""}
	room := rows - len(right) - 3
	body := s.Body
	if len(body) > room {
		body = body[:room]
	}
	right = append(right, body...)
	for len(right) < rows-3 {
		right = append(right, "")
	}
	right = append(right, s.buttons(k, rw)...)

	lines := make([]string, rows)
	for i := range rows {
		lines[i] = kit.Fit(rail[i], railW) + k.S(kit.Faint, "│") + " " + kit.Fit(right[i], rw) + " "
	}
	step := ""
	if len(s.Steps) > 0 {
		step = k.S(kit.Muted, "Step "+strconv.Itoa(min(s.Current, len(s.Steps)-1)+1)+" of "+strconv.Itoa(len(s.Steps)))
	}
	out := k.Draw(kit.Box{Title: k.S(kit.FG, s.Title), Right: step, Role: kit.Rule, Width: w}, lines)
	for _, l := range hint {
		out = append(out, " "+k.S(kit.Rule, l))
	}
	return kit.Clip(width, out)
}

// rail is the steps, four rows apart as in the piece when there is room:
// (✓) and Completed in green, the current one in blue, the rest faint,
// joined by a line that turns green behind each finished step.
func (s Stepper) rail(k kit.Kit, rows, railW int) []string {
	out := make([]string, rows)
	gap := 4
	if n := len(s.Steps); n > 1 && 2+(n-1)*gap+2 > rows {
		gap = max(2, (rows-4)/(n-1))
	}
	for i, st := range s.Steps {
		y := 2 + i*gap
		if y+1 >= rows {
			break
		}
		mark, name, sub, role, subRole := " "+strconv.Itoa(i+1)+" ", kit.Rule, "Pending", kit.Rule, kit.Faint
		switch {
		case st.Done:
			mark, name, sub, role, subRole = "(✓)", kit.Muted, "Completed", kit.OK, kit.OK
		case i == s.Current:
			mark, name, sub, role, subRole = "("+strconv.Itoa(i+1)+")", kit.FG, "In progress", kit.Accent, kit.Accent
		}
		out[y] = "  " + k.S(role, mark)
		if railW > railMark {
			out[y] += " " + k.S(name, kit.Fit(st.Name, railW-7))
			out[y+1] = "      " + k.S(subRole, sub)
		}
		if i == len(s.Steps)-1 {
			continue
		}
		line := kit.Faint
		if st.Done {
			line = kit.OK
		}
		for d := 1; d < gap; d++ {
			if y+d < rows {
				rest := ""
				if d == 1 && railW > railMark {
					rest = "  " + k.S(subRole, sub)
				}
				out[y+d] = "   " + k.S(line, "│") + rest
			}
		}
	}
	return out
}

// buttons is "← Back" at the left and the boxed button at the right, on
// the frame's last three rows.
func (s Stepper) buttons(k kit.Kit, w int) []string {
	out := []string{"", "", ""}
	back := ""
	if s.Back {
		back = k.S(kit.Muted, "← Back")
	}
	if s.Button == "" {
		out[1] = back
		return out
	}
	role := kit.Accent
	if s.Pressed {
		role = kit.OK
	}
	btn := k.Draw(kit.Box{Role: role, Width: kit.Width(s.Button) + 6}, []string{"  " + k.S(kit.FG, s.Button)})
	bw := kit.BlockWidth(btn)
	for i, l := range btn {
		left := ""
		if i == 1 {
			left = back
		}
		out[i] = kit.Fit(left, w-bw) + l
	}
	return out
}

// Input is the piece's labelled text box: blue while it has focus, with
// the cursor at the end, and a problem in red under it.
func Input(k kit.Kit, label, value string, w int, focus bool, problem string) []string {
	role, cursor := kit.Faint, ""
	if focus {
		role, cursor = kit.Accent, k.S(kit.FG, "█")
	}
	box := k.Draw(kit.Box{Role: role, Width: w}, []string{" " + k.S(kit.FG, value) + cursor})
	out := append([]string{k.S(kit.Muted, label)}, box...)
	if problem != "" {
		out = append(out, k.S(kit.Err, "✗ "+problem))
	}
	return out
}

// Radios is the piece's option row: ● on the chosen one in blue, ○ on the
// rest, with the label brighter while the row has focus.
func Radios(k kit.Kit, label string, opts []string, chosen int, focus bool) []string {
	lr := kit.Muted
	if focus {
		lr = kit.FG
	}
	var row []string
	for i, o := range opts {
		if i == chosen {
			row = append(row, k.S(kit.Accent, "● "+o))
		} else {
			row = append(row, k.S(kit.Muted, "○ "+o))
		}
	}
	return []string{k.S(lr, label), strings.Join(row, "    ")}
}

// Card is the piece's integration card: a name, what it does, and its
// state at the right, green when set. With focus, ‹ › show the value can
// be changed.
func Card(k kit.Kit, name, desc, value string, set, focus bool, w int) []string {
	border, vr := kit.Faint, kit.Accent
	if set {
		border, vr = kit.OK, kit.OK
	}
	if focus {
		border = kit.Accent
		value = "‹ " + value + " ›"
	}
	inner := w - 4
	nameW := min(18, inner/3)
	left := k.S(kit.FG, kit.Fit(name, nameW)) + k.S(kit.Muted, kit.Fit(desc, max(0, inner-nameW-kit.Width(value)-1)))
	return k.Draw(kit.Box{Role: border, Width: w, Pad: 1}, []string{spread(left, k.S(vr, value), inner)})
}

// Burst is the piece's finish: a burst of sparks around ✓✓✓.
func Burst(k kit.Kit) []string {
	rows := []string{"  .   *   .  ", "*   \\ | /   *", "  -- ✓✓✓ --  ", "*   / | \\   *", "  .   *   .  "}
	roles := []kit.Role{kit.Notice, kit.Accent2, kit.Warn}
	out := make([]string, len(rows))
	for i, r := range rows {
		role := roles[i%len(roles)]
		if i == 2 {
			role = kit.OK
		}
		out[i] = k.S(role, r)
	}
	return out
}

// Progress is the piece's finishing bar: ━ in green for done, ─ for the rest.
func Progress(k kit.Kit, frac float64, w int) string {
	n := int(min(1, max(0, frac)) * float64(w))
	return k.S(kit.OK, strings.Repeat("━", n)) + k.S(kit.Faint, strings.Repeat("─", w-n))
}
