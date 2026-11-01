package screens

import (
	"strings"

	"github.com/charmbracelet/x/ansi"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Modal focus stops, in tab order as in the piece.
const (
	FocusInput = iota
	FocusCancel
	FocusAction
)

// Modal is the modal-dialog piece: a confirm dialog that takes a typed
// name, with Cancel and a red action that only arms once the name
// matches.
type Modal struct {
	Title   string   // "Delete topic?"
	Body    []string // styled lines between the title and the prompt
	Confirm string   // what to type, in orange
	Typed   string
	Focus   int
	Action  string // "Delete topic"
	Busy    string // replaces the action with a spinner and this, once pressed
	Frame   int    // spinner frame
}

var spin = []string{"┤", "┘", "┴", "└", "├", "┌", "┬", "┐"}

// Spin is the piece's spinner at frame.
func Spin(frame int) string { return spin[frame%len(spin)] }

// Armed is whether the typed text matches.
func (m Modal) Armed() bool { return m.Typed == m.Confirm }

// Render draws the dialog alone, w wide; Overlay puts it over a page.
func (m Modal) Render(k kit.Kit, w int) []string {
	w = max(w, 40)
	inner := w - 2
	in := inner - 4 // text inside the margins
	out := []string{"", spread("  "+k.S(kit.FG, m.Title), k.S(kit.Muted, "×")+"  ", inner), k.S(kit.Faint, strings.Repeat("─", inner))}
	for _, l := range m.Body {
		out = append(out, "  "+l)
	}
	out = append(out, "", "  "+k.S(kit.Muted, "Type ")+k.S(kit.Warn, m.Confirm)+k.S(kit.Muted, " to confirm"))

	role, border, cursor := kit.Faint, kit.Rounded, ""
	if m.Focus == FocusInput {
		role, border, cursor = kit.Accent, kit.Heavy, k.S(kit.Accent, "█")
	}
	typed := m.Typed
	if kit.Width(typed) > in-4 {
		typed = ansi.TruncateLeft(typed, kit.Width(typed)-(in-4), "…")
	}
	for _, l := range k.Draw(kit.Box{Role: role, Border: border, Width: in}, []string{" " + k.S(kit.FG, typed) + cursor}) {
		out = append(out, "  "+l)
	}
	out = append(out, "")

	// Buttons at the right, the focus hint at the left, as in the piece.
	cancelRole, cancelBorder := kit.Muted, kit.Rounded
	if m.Focus == FocusCancel {
		cancelRole, cancelBorder = kit.Accent, kit.Heavy
	}
	cancel := k.Draw(kit.Box{Role: cancelRole, Border: cancelBorder, Width: 12}, []string{"  " + k.S(kit.FG, "Cancel")})
	actRole, actBorder, label := kit.Faint, kit.Rounded, k.S(kit.Muted, m.Action)
	if m.Armed() {
		actRole, label = kit.Err, k.S(kit.Err, m.Action)
	}
	if m.Focus == FocusAction {
		actBorder = kit.Heavy
	}
	if m.Busy != "" {
		actRole, actBorder, label = kit.Err, kit.Heavy, k.S(kit.Err, Spin(m.Frame)+" "+m.Busy)
	}
	act := k.Draw(kit.Box{Role: actRole, Border: actBorder, Width: max(kit.Width(m.Action), kit.Width(m.Busy)+2) + 4}, []string{" " + label})
	hint := map[int]string{FocusInput: "tab to move", FocusCancel: "enter or esc to close", FocusAction: "enter to confirm"}[m.Focus]
	if m.Focus == FocusAction && !m.Armed() {
		hint = "type the name first"
	}
	buttons := kit.Beside(1, cancel, act)
	bw := kit.BlockWidth(buttons)
	for i, l := range buttons {
		left := ""
		if i == 1 {
			left = k.S(kit.Rule, hint)
		}
		out = append(out, "  "+kit.Fit(left, max(0, in-bw))+l)
	}
	return k.Draw(kit.Box{Role: kit.Rule, Width: w}, append(out, ""))
}

// Dim is a page under the scrim: its text only, in the faintest colour,
// as the piece greys the settings page behind the dialog.
func Dim(k kit.Kit, page []string) []string {
	out := make([]string, len(page))
	for i, l := range page {
		out[i] = k.S(kit.Faint, ansi.Strip(l))
	}
	return out
}

// Overlay centres top over base, which it pads to width × height.
func Overlay(base, top []string, width, height int) []string {
	out := make([]string, max(height, len(top)))
	copy(out, base)
	tw := kit.BlockWidth(top)
	x, y := max(0, (width-tw)/2), max(0, (len(out)-len(top))/2)
	for i, l := range top {
		row := out[y+i]
		left := ansi.Cut(row, 0, x)
		right := ansi.Cut(row, x+tw, width)
		out[y+i] = kit.Fit(left, x) + l + right
	}
	return out
}
