package screens

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Series is one watched topic's row: a records/sec sparkline and totals.
type Series struct {
	Name     string
	Rates    []float64 // records/sec per offset refresh, oldest first
	Retained int64
	Known    bool // offsets fetched at least once
}

// Toast is one cluster event card.
type Toast struct {
	Role    kit.Role
	Glyph   string // ✓ ✗ ▲ •
	Title   string // "ISR shrunk"
	Subject string // "orders/1"
	Detail  string
	Age     time.Duration
	Life    float64 // 1 when new, 0 when about to expire
}

// Watch is kaas watch's screen: the sparkline-row panel for watched topics,
// with the toast stack of recent cluster events below it.
type Watch struct {
	Title  string // "watch · cluster 5L6g3nShT…"
	Right  string // "gen 326 · 1 broker · 2 topics"
	Series []Series
	Empty  string // shown when there are no series
	Foot   string
	Toasts []Toast // newest first
	Quiet  string  // shown when there are no toasts
}

var seriesRoles = []kit.Role{kit.Accent, kit.Accent2, kit.OK, kit.Warn, kit.Info}

// Render draws the panel and as many toasts as fit in height.
func (v Watch) Render(k kit.Kit, width, height int) []string {
	w := min(width, 100)
	inner := w - 2
	body := []string{""}
	const nameW, tailW = 14, 30
	spark := max(8, inner-nameW-tailW-2)
	for i, s := range v.Series {
		// Each series gets its own colour and its newest cell is white, as
		// in sparkline-row.
		role := seriesRoles[i%len(seriesRoles)]
		spk := func(w int) string {
			line := sparkline(s.Rates, w)
			r := []rune(line)
			return k.S(role, string(r[:len(r)-1])) + k.S(kit.FG, string(r[len(r)-1:]))
		}
		line := " " + k.S(kit.Muted, kit.Fit(s.Name, nameW)) + " "
		if !s.Known {
			body = append(body, line+k.S(kit.Muted, "waiting for offsets"), "")
			continue
		}
		now := 0.0
		if len(s.Rates) > 0 {
			now = s.Rates[len(s.Rates)-1]
		}
		trend := k.S(kit.Muted, "·")
		if n := len(s.Rates); n >= 2 {
			switch {
			case s.Rates[n-1] > s.Rates[n-2]:
				trend = k.S(kit.OK, "↑")
			case s.Rates[n-1] < s.Rates[n-2]:
				trend = k.S(kit.Warn, "↓")
			}
		}
		if inner < nameW+tailW+18 {
			// Narrow: numbers beside the name, the sparkline on its own line.
			nums := k.S(kit.FG, Num(now)+"/s") + " " + trend + k.S(kit.Muted, " · "+Num(float64(s.Retained))+" kept")
			body = append(body, spread(" "+k.S(kit.Muted, s.Name), nums+" ", inner),
				" "+spk(inner-2), "")
			continue
		}
		tail := k.S(kit.FG, lpad(Num(now)+"/s", 10)) + " " + trend + " " +
			k.S(kit.Muted, lpad(Num(float64(s.Retained)), 11)+" kept")
		body = append(body, line+spk(spark)+" "+tail, "")
	}
	if len(v.Series) == 0 {
		for _, l := range kit.Wrap(v.Empty, inner-2) {
			body = append(body, " "+k.S(kit.Muted, l))
		}
		body = append(body, "")
	}
	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	for _, l := range kit.Wrap(v.Foot, inner-2) {
		body = append(body, " "+k.S(kit.Muted, l))
	}
	out := k.Draw(kit.Box{Title: k.S(kit.FG, v.Title), Right: k.S(kit.Muted, v.Right), Role: kit.Faint, Width: w}, body)

	room := height - len(out) - 1
	if len(v.Toasts) == 0 {
		if room > 0 {
			out = append(out, "")
			for _, l := range kit.Wrap(v.Quiet, w-4) {
				out = append(out, "  "+k.S(kit.Muted, l))
			}
		}
		return kit.Clip(width, out)
	}
	tw := min(w, 56)
	shown := 0
	for _, t := range v.Toasts {
		card := toast(k, t, tw)
		if room < len(card) {
			break
		}
		out = append(out, kit.Indent(w-tw, card)...)
		room -= len(card)
		shown++
	}
	if more := len(v.Toasts) - shown; more > 0 && room > 0 {
		out = append(out, kit.Indent(w-tw, []string{k.S(kit.Muted, fmt.Sprintf("  + %d more", more))})...)
	}
	return kit.Clip(width, out)
}

// toast draws one card. Its bottom edge is a ━ fuse that burns down as the
// toast ages, the template's countdown.
func toast(k kit.Kit, t Toast, w int) []string {
	inner := w - 2
	age := k.S(kit.Muted, ago(t.Age))
	head := " " + k.S(t.Role, t.Glyph) + " " + k.B(kit.FG, t.Title) + "  " + k.S(kit.Muted, t.Subject)
	head = spread(head, age+" ", inner)
	sub := "   " + k.S(kit.Muted, t.Detail)
	fuse := int(math.Round(t.Life * float64(inner)))
	return []string{
		k.S(kit.Rule, "╭"+strings.Repeat("─", inner)+"╮"),
		k.S(kit.Rule, "│") + kit.Fit(head, inner) + k.S(kit.Rule, "│"),
		k.S(kit.Rule, "│") + kit.Fit(sub, inner) + k.S(kit.Rule, "│"),
		k.S(kit.Rule, "╰") + k.S(t.Role, strings.Repeat("━", fuse)) + k.S(kit.Rule, strings.Repeat("─", inner-fuse)+"╯"),
	}
}

// sparkline draws the newest w values on an 8-level scale from zero.
func sparkline(vals []float64, w int) string {
	if len(vals) > w {
		vals = vals[len(vals)-w:]
	}
	peak := 0.0
	for _, v := range vals {
		peak = math.Max(peak, v)
	}
	levels := []rune("▁▂▃▄▅▆▇█")
	var sb strings.Builder
	sb.WriteString(strings.Repeat(" ", w-len(vals)))
	for _, v := range vals {
		i := 0
		if peak > 0 {
			i = int(v / peak * float64(len(levels)-1))
		}
		sb.WriteRune(levels[i])
	}
	return sb.String()
}

func ago(d time.Duration) string {
	switch {
	case d < time.Second:
		return "now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh", int(d.Hours()))
}
