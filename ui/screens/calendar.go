package screens

import (
	"fmt"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// How a day can be picked.
const (
	DayData  = iota // records, or not counted yet
	DayEmpty        // counted: no records that day
	DayOut          // before what the topic keeps, or after today
)

// RangePicker is the date-range-picker piece: two months side by side, a
// start and end in brackets with the days between shaded, a cursor, and a
// footer with the range, what it holds, and Apply.
type RangePicker struct {
	Title      string    // on the frame: "Read orders between"
	Month      time.Time // the first month shown; any day in it
	Start, End time.Time // picked days; zero until picked
	Cursor     time.Time
	Day        func(time.Time) int // DayData, DayEmpty or DayOut
	From, To   string              // the footer's values; "" shows "Add date"
	Label      string              // the third footer column: "Records"
	Count      string              // "-" until there is a start
	Ready      bool                // Apply lit
	Hint       string
}

// The piece's range shading; no theme role has it.
const rangeBlue = "#2f5a8f"

// pickerW is the piece's width; its height grows to fit six-week months.
const pickerW = 66

// Render draws the picker; below its 66 columns, one month.
func (p RangePicker) Render(k kit.Kit, width int) []string {
	months := 2
	w := pickerW
	if width < pickerW {
		months, w = 1, 34
	}
	inner := w - 2
	grid := make([][]string, 5+6*2) // title, weekdays, then six weeks
	for y := range grid {
		grid[y] = make([]string, inner)
		for x := range grid[y] {
			grid[y][x] = " "
		}
	}
	put := func(x, y int, s string, style func(string) string) {
		for _, ch := range s {
			if y >= 0 && y < len(grid) && x >= 0 && x < inner {
				grid[y][x] = style(string(ch))
			}
			x++
		}
	}
	role := func(r kit.Role) func(string) string { return func(s string) string { return k.S(r, s) } }
	day := func(t time.Time) time.Time { return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()) }
	first := time.Date(p.Month.Year(), p.Month.Month(), 1, 0, 0, 0, 0, p.Month.Location())
	start, end := day(p.Start), day(p.End)
	if !p.End.IsZero() && end.Before(start) {
		start, end = end, start
	}
	cursor := day(p.Cursor)
	cells := map[time.Time][2]int{}
	for m := range months {
		mon := first.AddDate(0, m, 0)
		x0 := 2 + m*32
		name := mon.Format("January 2006")
		put(x0+(27-len(name))/2, 1, name, role(kit.FG))
		if m == 0 {
			put(x0, 1, "◄", role(kit.Muted))
		}
		if m == months-1 {
			put(x0+26, 1, "►", role(kit.Muted))
		}
		put(x0, 3, "Mo Tu We Th Fr Sa Su", role(kit.Rule))
		off := (int(mon.Weekday()) + 6) % 7
		days := mon.AddDate(0, 1, -1).Day()
		for d := 1; d <= days; d++ {
			t := time.Date(mon.Year(), mon.Month(), d, 0, 0, 0, 0, mon.Location())
			o := off + d - 1
			x, y := x0+(o%7)*4, 5+(o/7)*2
			cells[t] = [2]int{x, y}
			inRange := !p.Start.IsZero() && !p.End.IsZero() && t.After(start) && t.Before(end)
			if inRange {
				put(x-1, y, "░░░░", func(s string) string { return k.Hex(rangeBlue, s) })
			}
			ds := fmt.Sprintf("%2d", d)
			if inRange && d < 10 {
				x, ds = x+1, ds[1:] // keep the shading under the blank
			}
			kind := DayData
			if p.Day != nil {
				kind = p.Day(t)
			}
			isEdge := (!p.Start.IsZero() && t.Equal(start)) || (!p.End.IsZero() && t.Equal(end))
			switch {
			case isEdge:
				put(x-1, y, "[", role(kit.Accent))
				put(x, y, ds, role(kit.Notice))
				put(x+2, y, "]", role(kit.Accent))
			case inRange:
				put(x, y, ds, role(kit.FG))
			case kind == DayOut:
				put(x, y, ds, role(kit.Faint))
			case kind == DayEmpty:
				put(x, y, ds, role(kit.Muted))
			default:
				put(x, y, ds, role(kit.FG))
			}
		}
	}
	if c, ok := cells[cursor]; ok && !p.Ready {
		put(c[0]+1, c[1]+1, "↖", role(kit.Warn))
	}

	rows := 0
	for _, c := range cells {
		rows = max(rows, c[1]+2)
	}
	body := make([]string, 0, rows+5)
	for _, row := range grid[:rows] {
		body = append(body, strings.Join(row, ""))
	}
	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	val := func(s string) string {
		if s == "" {
			return k.S(kit.Faint, "Add date")
		}
		return k.S(kit.FG, s)
	}
	count := k.S(kit.Faint, "-")
	if p.Count != "" && p.Count != "-" {
		count = k.S(kit.Warn, p.Count)
	}
	btnRole, btnText := kit.Faint, kit.Faint
	if p.Ready {
		btnRole, btnText = kit.Accent, kit.FG
	}
	btn := k.Draw(kit.Box{Role: btnRole, Width: 13}, []string{"  " + k.S(btnText, "Apply")})
	cols := []int{2, 17, 33}
	if months == 1 {
		cols = []int{1, 11, 21}
	}
	labels := []string{"From", "To", orDash(p.Label)}
	vals := []string{val(p.From), val(p.To), count}
	row := func(parts []string, cw []int) string {
		var sb strings.Builder
		at := 0
		for i, s := range parts {
			sb.WriteString(strings.Repeat(" ", max(0, cw[i]-at)))
			sb.WriteString(s)
			at = cw[i] + kit.Width(s)
		}
		return sb.String()
	}
	foot := []string{
		row([]string{k.S(kit.Muted, labels[0]), k.S(kit.Muted, labels[1]), k.S(kit.Muted, labels[2])}, cols),
		row(vals, cols),
		"",
	}
	if months == 2 {
		for i := range foot {
			foot[i] = kit.Fit(foot[i], inner-15) + btn[i]
		}
	} else {
		for _, l := range btn {
			foot = append(foot, strings.Repeat(" ", inner-14)+l)
		}
	}
	body = append(body, foot...)
	out := k.Draw(kit.Box{Title: k.S(kit.FG, p.Title), Role: kit.Rule, Width: w}, body)
	if p.Hint != "" {
		for _, l := range wrapParts(p.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}
