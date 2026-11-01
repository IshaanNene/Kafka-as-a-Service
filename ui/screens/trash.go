package screens

import (
	"fmt"
	"math"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// The piece's steel greys for the bin, light to dark; no theme role has
// them.
const (
	steelLight = "#c9d1d9"
	steelMid   = "#8b949e"
	steelDark  = "#6e7681"
	steelRib   = "#3d444d"
)

// Trash is the empty-trash-lid piece: a bin with its lid open while undo
// is still possible, shut once the delete is done, with an Undo button
// and a bar that drains.
type Trash struct {
	Title  string  // "Deleting orders in 4s", "Topic deleted"
	Detail string  // muted, under the title
	Undo   float64 // the share of the undo window left; 0 when it is over
	Secs   int     // seconds left, for the button
	Shut   bool    // the lid is closed: the delete was sent
	Hint   string  // under the card
}

// Render draws the card, at most the piece's 56 columns.
func (t Trash) Render(k kit.Kit, width int) []string {
	w := min(max(width, 30), 56)
	inner := w - 2
	const rows = 18
	grid := make([][]string, rows) // one styled cell per column
	for y := range grid {
		grid[y] = make([]string, inner)
		for x := range grid[y] {
			grid[y][x] = " "
		}
	}
	set := func(x, y int, s string) {
		if x >= 0 && x < inner && y >= 0 && y < rows {
			grid[y][x] = s
		}
	}
	cx, top, bottom := inner/2, 5, 11

	// The body, tapering, with dark ribs, lit from the left.
	for y := top; y <= bottom; y++ {
		half := 6 - (y-top)/4
		for x := cx - half; x <= cx+half; x++ {
			hex, ch := steelMid, "█"
			switch {
			case x < cx-2:
				hex = steelLight
			case x > cx+2:
				hex = steelDark
			}
			switch {
			case (x-cx)%3 == 0 && y > top && y < bottom:
				hex, ch = steelRib, "▐"
			case y == bottom:
				ch = "▀"
			}
			set(x, y, k.Hex(hex, ch))
		}
	}
	// The lid, hinged at the right: swung up while undo is possible.
	angle := 0.0
	if !t.Shut {
		angle = 0.7
	}
	hx, hy := float64(cx+7), float64(top-1)
	for i := 0; i <= 15; i++ {
		x := hx - math.Cos(angle)*float64(i)
		y := hy - math.Sin(angle)*float64(i)*0.4
		set(int(math.Round(x)), int(math.Round(y)), k.Hex(steelMid, "█"))
	}
	mx, my := hx-math.Cos(angle)*7.5, hy-math.Sin(angle)*7.5*0.4
	for d := -1; d <= 1; d++ {
		set(int(math.Round(mx+float64(d)-math.Sin(angle)*1.2)), int(math.Round(my-math.Cos(angle))), k.Hex(steelLight, "▄"))
	}
	// The crumpled page waits above the open bin.
	if !t.Shut {
		px := cx - 4
		set(px, 2, k.S(kit.FG, "▟"))
		set(px+1, 2, k.S(kit.FG, "▙"))
		set(px, 3, k.S(kit.FG, "▜"))
		set(px+1, 3, k.S(kit.FG, "▛"))
	}

	body := make([]string, 0, rows+6)
	for _, row := range grid {
		body = append(body, strings.Join(row, ""))
	}
	center := func(s string, r kit.Role) string {
		if kit.Width(s) > inner-2 {
			s = kit.Fit(s, inner-2) // Fit pads too, so only to cut
		}
		return strings.Repeat(" ", max(0, (inner-kit.Width(s))/2)) + k.S(r, s)
	}
	body = append(body[:13], center(t.Title, kit.FG), center(t.Detail, kit.Muted), "")

	// Undo, live in blue with its seconds, and the bar draining beside it.
	live := t.Undo > 0
	label, role := "Undo      ", kit.Faint
	if live {
		label, role = fmt.Sprintf("Undo (%ds)", t.Secs), kit.Accent
	}
	btn := k.Draw(kit.Box{Role: role, Width: 14}, []string{" " + k.S(role, label)})
	barW := min(18, inner-14-6)
	n := int(math.Round(t.Undo * float64(barW)))
	bar := k.S(kit.Accent, strings.Repeat("━", n)) + k.S(kit.Faint, strings.Repeat("━", barW-n))
	left := max(1, (inner-14-2-barW)/2)
	for i, l := range btn {
		line := strings.Repeat(" ", left) + l
		if i == 1 {
			line += "  " + bar
		}
		body = append(body, line)
	}
	out := k.Draw(kit.Box{Role: kit.Faint, Width: w}, body)
	if t.Hint != "" {
		for _, l := range wrapParts(t.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}
