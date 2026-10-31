package screens

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// BenchState is where a case is in the run.
type BenchState int

const (
	BenchPending BenchState = iota
	BenchRunning
	BenchDone
	BenchFailed
)

// BenchRow is one case in the results table.
type BenchRow struct {
	Name  string
	State BenchState

	// Done
	RecsPerSec, MBPerSec float64
	P50, P99, Max        time.Duration
	Errors               int64
	Err                  string

	// Running
	Acked, Total int64
}

// Bench is the benchmark-table screen: a results table filling case by case
// with a live counter on the running one, then the running case's latency
// histogram and throughput beside it.
type Bench struct {
	Title     string // "bench produce · orders"
	Right     string // "localhost:19092 · kafka v4.2"
	Rows      []BenchRow
	Spin      int // spinner frame
	Hist      []int64
	Buckets   []time.Duration // Hist's upper bounds; Hist has one more (overflow)
	HistTitle string
	Rate      []float64
	RateTitle string
	Foot      string
}

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Render draws the table, then the two charts side by side or stacked.
func (b Bench) Render(k kit.Kit, width int) []string {
	w := min(width, 100)
	inner := w - 2
	const (
		numW = 10
		durW = 8
	)
	nw := 4
	for _, r := range b.Rows {
		nw = max(nw, kit.Width(r.Name))
	}
	nw = min(nw, max(12, inner/3))
	// Narrow terminals drop max, then MB/s, before squeezing anything.
	showMax, showMB := true, true
	fixed := func() int {
		f := 1 + nw + 2 + numW + 2*(1+durW) + 3
		if showMB {
			f += 8
		}
		if showMax {
			f += 1 + durW
		}
		return f
	}
	if fixed() > inner {
		showMax = false
	}
	if fixed() > inner {
		showMB = false
	}
	barW := max(0, inner-fixed()-1)

	head := " " + kit.Fit("Case", nw) + "  " + lpad("rec/s", numW)
	if showMB {
		head += " " + lpad("MB/s", 7)
	}
	head += " " + lpad("p50", durW) + " " + lpad("p99", durW)
	if showMax {
		head += " " + lpad("max", durW)
	}
	head += "   "
	if barW >= 8 {
		head += "relative"
	}
	body := []string{k.S(kit.FG, head), k.S(kit.Faint, strings.Repeat("─", inner))}

	best := 0.0
	for _, r := range b.Rows {
		best = math.Max(best, r.RecsPerSec)
	}
	// Too narrow for a table even without max and MB/s: one card per case.
	cards := fixed() > inner || inner < 60
	if cards {
		body = b.cards(k, inner, best)
	}
	for _, r := range b.Rows {
		if cards {
			break
		}
		lead := " "
		name := kit.Fit(r.Name, nw)
		switch r.State {
		case BenchDone, BenchFailed:
			line := lead + k.S(kit.FG, name) + "  " + k.B(kit.FG, lpad(Num(r.RecsPerSec), numW))
			if showMB {
				line += " " + k.S(kit.FG, lpad(fmt.Sprintf("%.2f", r.MBPerSec), 7))
			}
			line += " " + k.S(kit.FG, lpad(Dur(r.P50), durW)) + " " + k.S(kit.FG, lpad(Dur(r.P99), durW))
			if showMax {
				line += " " + k.S(kit.FG, lpad(Dur(r.Max), durW))
			}
			line += "   "
			switch {
			case r.State == BenchFailed || r.Errors > 0:
				msg := fmt.Sprintf("✗ %d errors", r.Errors)
				if r.Err != "" {
					msg += ": " + r.Err
				}
				line += k.S(kit.Err, msg)
			case barW >= 8 && best > 0:
				line += k.S(barRole(r, best), strings.Repeat("■", max(1, int(float64(barW)*r.RecsPerSec/best))))
			}
			body = append(body, line)
		case BenchRunning:
			lead = k.S(kit.Accent, spinner[b.Spin%len(spinner)])
			pct := 0.0
			if r.Total > 0 {
				pct = float64(r.Acked) / float64(r.Total)
			}
			count := fmt.Sprintf("acked %s/%s", Num(float64(r.Acked)), Num(float64(r.Total)))
			pw := max(4, min(24, inner-nw-kit.Width(count)-12))
			fill := int(pct * float64(pw))
			body = append(body, lead+k.S(kit.FG, name)+"  "+k.S(kit.Accent, count)+"  "+
				k.S(kit.Accent, strings.Repeat("█", fill))+k.S(kit.Accent, strings.Repeat("░", pw-fill))+
				k.S(kit.Muted, fmt.Sprintf(" %3.0f%%", pct*100)))
		default:
			body = append(body, lead+k.S(kit.Rule, name)+"  "+k.S(kit.Faint, "···········"))
		}
	}
	body = append(body, "", k.S(kit.Faint, strings.Repeat("─", inner)))
	for _, l := range kit.Wrap(b.Foot, inner-2) {
		body = append(body, " "+k.S(kit.Muted, l))
	}
	out := k.Draw(kit.Box{Title: k.S(kit.Muted, b.Title), Right: k.S(kit.Muted, b.Right), Role: kit.Rule, Width: w}, body)

	// Charts sit side by side when the throughput chart still gets 40
	// columns; otherwise they stack at full width.
	const histW = 40
	if w-histW-2 >= 40 {
		hist, rate := b.histogram(k, histW), b.rateChart(k, w-histW-2)
		if len(hist) == 0 {
			hist, rate = nil, b.rateChart(k, w)
		}
		out = append(out, kit.Beside(2, hist, rate)...)
	} else {
		out = append(out, b.histogram(k, w)...)
		out = append(out, b.rateChart(k, w)...)
	}
	return kit.Clip(width, out)
}

// histogram draws latency buckets as horizontal bars, trimmed to the range
// that has samples.
func (b Bench) histogram(k kit.Kit, w int) []string {
	var total int64
	lo, hi := -1, -1
	for i, n := range b.Hist {
		total += n
		if n > 0 {
			if lo < 0 {
				lo = i
			}
			hi = i
		}
	}
	if total == 0 {
		return nil
	}
	lo, hi = max(0, min(lo, hi-3)), min(len(b.Hist)-1, max(hi, lo+3))
	var peak int64
	for _, n := range b.Hist[lo : hi+1] {
		peak = max(peak, n)
	}
	const labelW = 7
	barW := w - 2 - labelW - 8
	var body []string
	for i := lo; i <= hi; i++ {
		label := "> " + Dur(b.Buckets[len(b.Buckets)-1])
		if i < len(b.Buckets) {
			label = "≤ " + Dur(b.Buckets[i])
		}
		n := b.Hist[i]
		bar := int(float64(barW) * float64(n) / float64(peak))
		if n > 0 {
			bar = max(bar, 1)
		}
		// Fast buckets start the chart ramp and slow ones end it, as the
		// histogram piece shades its bars by distance along the axis.
		body = append(body, " "+k.S(kit.Muted, lpad(label, labelW))+" "+
			k.Hex(theme.Ramp(k.Theme.Chart, i-lo, hi-lo+1), strings.Repeat("■", bar))+strings.Repeat(" ", barW-bar)+
			k.S(kit.FG, lpad(fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total)), 6)))
	}
	return k.Draw(kit.Box{Title: k.S(kit.FG, b.HistTitle), Role: kit.Rule, Width: w}, body)
}

// rateChart draws throughput as an 8-level block column chart, newest on
// the right.
func (b Bench) rateChart(k kit.Kit, w int) []string {
	if len(b.Rate) == 0 {
		return nil
	}
	const rows = 6
	axisW := 8
	cols := w - 2 - axisW - 1
	data := b.Rate
	if len(data) > cols {
		data = data[len(data)-cols:]
	}
	peak, low := 0.0, math.Inf(1)
	for _, v := range data[min(1, len(data)-1):] { // the first window is a partial ramp-up
		peak, low = math.Max(peak, v), math.Min(low, v)
	}
	// A steady rate on a zero baseline is a solid block that hides the
	// wobble, so the floor rises when everything sits in the top half.
	floor := 0.0
	if low > peak/2 {
		floor = math.Max(0, low-(peak-low)*0.5-1)
	}
	if peak <= floor {
		peak = floor + 1
	}
	levels := []rune(" ▁▂▃▄▅▆▇█")
	var body []string
	for row := rows - 1; row >= 0; row-- {
		axis := strings.Repeat(" ", axisW)
		switch row {
		case rows - 1:
			axis = lpad(Num(peak), axisW-1) + " "
		case 0:
			axis = lpad(Num(floor), axisW-1) + " "
		}
		// Solid cells in the chart's deep blue, the partial top of each
		// column in the accent, as network-traffic draws its fill.
		line := k.S(kit.Muted, axis) + k.S(kit.Faint, "┤")
		var run strings.Builder
		top := false
		flush := func() {
			if run.Len() == 0 {
				return
			}
			if top {
				line += k.S(kit.Accent, run.String())
			} else {
				line += k.Hex(theme.Ramp(k.Theme.Chart, 0, 2), run.String())
			}
			run.Reset()
		}
		for _, v := range data {
			eighths := int((v - floor) / (peak - floor) * rows * 8)
			cell := eighths - row*8
			r, isTop := ' ', true
			switch {
			case cell >= 8:
				r, isTop = '█', eighths-(row+1)*8 <= 0
			case cell > 0:
				r = levels[cell]
			}
			if isTop != top {
				flush()
				top = isTop
			}
			run.WriteRune(r)
		}
		flush()
		body = append(body, line)
	}
	now := data[len(data)-1]
	body = append(body, k.S(kit.Muted, fmt.Sprintf(" now %s/s · peak %s/s · %d windows", Num(now), Num(peak), len(b.Rate))))
	return k.Draw(kit.Box{Title: k.S(kit.FG, b.RateTitle), Role: kit.Rule, Width: w}, body)
}

// barRole paints the fastest case green, as benchmark-table marks the case
// that beat its baseline; the rest are blue.
func barRole(r BenchRow, best float64) kit.Role {
	if r.RecsPerSec >= best {
		return kit.OK
	}
	return kit.Accent
}

// Dur prints a latency compactly: 840µs, 5.2ms, 1.3s.
func Dur(d time.Duration) string {
	switch {
	case d <= 0:
		return "-"
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// Num groups thousands: 12,345; fractions only below 100.
func Num(v float64) string {
	if v < 100 && v != math.Trunc(v) {
		return fmt.Sprintf("%.1f", v)
	}
	s := fmt.Sprintf("%d", int64(math.Round(v)))
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func lpad(s string, w int) string {
	if n := kit.Width(s); n < w {
		return strings.Repeat(" ", w-n) + s
	}
	return s
}

// cards draws each case as two lines for narrow terminals: the name with
// its relative bar, then the numbers, wrapping on " · ".
func (b Bench) cards(k kit.Kit, inner int, best float64) []string {
	var body []string
	for i, r := range b.Rows {
		if i > 0 {
			body = append(body, "")
		}
		switch r.State {
		case BenchDone, BenchFailed:
			bar := ""
			if best > 0 && r.Errors == 0 && r.State == BenchDone {
				bar = k.S(barRole(r, best), strings.Repeat("■", max(1, int(10*r.RecsPerSec/best))))
			}
			body = append(body, spread(" "+k.B(kit.FG, r.Name), bar+" ", inner))
			body = append(body, joinWrap(k, []string{
				k.B(kit.FG, Num(r.RecsPerSec)+" rec/s"), k.S(kit.FG, fmt.Sprintf("%.2f MB/s", r.MBPerSec)),
				k.S(kit.Muted, "p50 ") + k.S(kit.FG, Dur(r.P50)), k.S(kit.Muted, "p99 ") + k.S(kit.FG, Dur(r.P99)),
				k.S(kit.Muted, "max ") + k.S(kit.FG, Dur(r.Max)),
			}, inner-3, "   ")...)
			if r.State == BenchFailed || r.Errors > 0 {
				msg := fmt.Sprintf("✗ %d errors", r.Errors)
				if r.Err != "" {
					msg += ": " + r.Err
				}
				for _, l := range kit.Wrap(msg, inner-4) {
					body = append(body, "   "+k.S(kit.Err, l))
				}
			}
		case BenchRunning:
			pct := 0.0
			if r.Total > 0 {
				pct = float64(r.Acked) / float64(r.Total)
			}
			body = append(body, k.S(kit.Accent, spinner[b.Spin%len(spinner)])+k.B(kit.FG, r.Name))
			count := fmt.Sprintf(" %3.0f%% · %s/%s", pct*100, Num(float64(r.Acked)), Num(float64(r.Total)))
			pw := max(4, inner-4-kit.Width(count))
			fill := int(pct * float64(pw))
			body = append(body, "   "+k.S(kit.Accent, strings.Repeat("█", fill))+k.S(kit.Accent, strings.Repeat("░", pw-fill))+k.S(kit.Muted, count))
		default:
			body = append(body, " "+k.S(kit.Rule, r.Name), "   "+k.S(kit.Faint, "···········"))
		}
	}
	return body
}

// joinWrap joins styled parts with " · ", starting a new indented line
// whenever the next part would pass w.
func joinWrap(k kit.Kit, parts []string, w int, indent string) []string {
	var out []string
	line := ""
	for _, p := range parts {
		switch {
		case line == "":
			line = p
		case kit.Width(line)+3+kit.Width(p) <= w:
			line += k.S(kit.Muted, " · ") + p
		default:
			out = append(out, indent+line)
			line = p
		}
	}
	return append(out, indent+line)
}
