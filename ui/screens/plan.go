package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Plan change kinds.
const (
	PlanAdd = iota
	PlanChange
)

// PlanAttr is one line of a block: key = to, or key = from -> to.
type PlanAttr struct {
	Key, From, To string // From is "" for an add
	Note          string // muted at the right: "re-reads 224"
}

// PlanBlock is one resource in the plan.
type PlanBlock struct {
	Change   int
	Comment  string // "billing · orders"
	Resource string // `offsets "billing" "orders"`
	Attrs    []PlanAttr
	Hidden   string // "(3 unchanged partitions hidden)"
}

// ApplyLine is one line of the apply: "orders: Commit complete after 4ms".
type ApplyLine struct {
	Name, Status string
	Role         kit.Role
	Detail       string // muted after the status
}

// Plan is the infra-plan-diff piece: the command, the plan in coloured
// diffs, Plan: N to add, N to change, the prompt with what was typed, the
// apply lines, and the footer of chips under a rule.
type Plan struct {
	Command string // after "$ "
	Lead    string // "kaas will perform the following actions:"
	Blocks  []PlanBlock
	Add     int
	Change  int
	Note    string // muted under the summary
	Ask     string // "Do you want to perform these actions?"; "" for no prompt
	Answer  string
	Apply   []ApplyLine
	Done    string // "Reset complete! …", green
	Failed  bool   // Done in red
	Footer  string // "group: billing  ·  state: Empty"
	Phase   string // "awaiting approval"
	PhaseOf kit.Role
	Lock    string // at the far right: "members: 0"
}

// The piece's marks: + green, ~ yellow.
var planMarks = map[int]struct {
	mark string
	role kit.Role
	verb string
}{
	PlanAdd:    {"+", kit.OK, " will be created"},
	PlanChange: {"~", kit.Notice, " will be updated in-place"},
}

// Lines is the plan without the footer, for printing before the prompt.
func (p Plan) Lines(k kit.Kit, width int) []string {
	out := []string{k.S(kit.OK, "$ ") + k.S(kit.FG, p.Command)}
	if p.Lead != "" {
		out = append(out, k.S(kit.FG, p.Lead), "")
	}
	for _, b := range p.Blocks {
		m := planMarks[b.Change]
		out = append(out,
			"  "+k.S(kit.FG, "# "+b.Comment)+k.S(m.role, m.verb),
			"  "+k.S(m.role, m.mark+" ")+k.S(kit.FG, b.Resource+" {"))
		keyW, valW := 0, 0
		for _, a := range b.Attrs {
			keyW = max(keyW, kit.Width(a.Key))
			valW = max(valW, kit.Width(a.value()))
		}
		for _, a := range b.Attrs {
			v := k.S(kit.OK, a.To)
			if a.From != "" {
				v = k.S(kit.Err, a.From) + k.S(kit.Muted, " -> ") + k.S(kit.OK, a.To)
			}
			line := "      " + k.S(m.role, m.mark+" ") + k.S(kit.Muted, kit.Fit(a.Key, keyW)+" = ") + v
			if a.Note != "" {
				line += strings.Repeat(" ", valW-kit.Width(a.value())+3) + k.S(kit.Muted, "# "+a.Note)
			}
			out = append(out, line)
		}
		if b.Hidden != "" {
			out = append(out, "        "+k.S(kit.Muted, "# "+b.Hidden))
		}
		out = append(out, "    "+k.S(kit.FG, "}"), "")
	}
	sum := k.S(kit.FG, "Plan: ") + k.S(kit.OK, Num(float64(p.Add))+" to add") + k.S(kit.FG, ", ") + k.S(kit.Notice, Num(float64(p.Change))+" to change") + k.S(kit.FG, ".")
	out = append(out, sum)
	if p.Note != "" {
		for _, l := range wrapParts(p.Note, max(20, width-2)) {
			out = append(out, k.S(kit.Muted, l))
		}
	}
	if p.Ask != "" && p.Answer != "" {
		out = append(out, "", k.S(kit.FG, p.Ask+" ")+k.S(kit.Muted, "Enter a value: ")+k.S(kit.Notice, p.Answer), "")
	}
	for _, a := range p.Apply {
		line := k.S(kit.FG, a.Name+": ") + k.S(a.Role, a.Status)
		if a.Detail != "" {
			line += k.S(kit.Muted, " "+a.Detail)
		}
		out = append(out, line)
	}
	if p.Done != "" {
		role := kit.OK
		if p.Failed {
			role = kit.Err
		}
		out = append(out, "", k.S(role, p.Done))
	}
	return out
}

// value is the attribute's text after "= ", for aligning notes.
func (a PlanAttr) value() string {
	if a.From == "" {
		return a.To
	}
	return a.From + " -> " + a.To
}

// Render draws the plan and its footer at width, a space in from the left
// as the piece has it.
func (p Plan) Render(k kit.Kit, width int) []string {
	w := min(max(width, 40), 90)
	var out []string
	for _, l := range p.Lines(k, width) {
		out = append(out, " "+l)
	}
	return kit.Clip(width, append(out, p.Foot(k, w)...))
}

// Foot is the rule and the chips under it.
func (p Plan) Foot(k kit.Kit, w int) []string {
	right := k.S(kit.OK, "+"+Num(float64(p.Add))) + " " + k.S(kit.Notice, "~"+Num(float64(p.Change))) + "   " + k.S(kit.Accent2, p.Lock)
	room := w - 2
	if wide := kit.Width(p.Footer) + 5 + kit.Width(p.Phase) + 3 + kit.Width(right); wide <= room {
		room -= kit.Width(right) + 3
	} else {
		right = "" // too narrow for the counts: the phase matters more
	}
	left := k.S(kit.Muted, cut(p.Footer, max(4, room-5-kit.Width(p.Phase)))+"  ·  ") + k.S(p.PhaseOf, p.Phase)
	foot := spread(left, right, w-2)
	return []string{k.S(kit.Faint, strings.Repeat("─", w)), " " + foot}
}
