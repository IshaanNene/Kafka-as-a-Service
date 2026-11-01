package screens

import (
	"fmt"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// LineKind is what a channel line says.
type LineKind int

const (
	LineMsg  LineKind = iota // <nick> text
	LineJoin                 // --> nick text, green
	LinePart                 // <-- nick text, red
	LineAct                  //  * text, purple
)

// ChanLine is one line of a Channel.
type ChanLine struct {
	Kind LineKind
	Time time.Time
	Meta string // muted, before the nick: "p2:1043"
	Nick string
	Hue  int // which nick colour; kaas consume uses the partition
	Text string
}

// Nick is one entry of the nick list.
type Nick struct {
	Name  string
	Hue   int
	Count string // muted, right-aligned
}

// Channel is the relay-chat-channel piece: a title bar, timestamped lines
// that wrap under their text, a nick list on the right, a status bar and an
// input line. kaas consume draws a topic as a channel, each record a line.
type Channel struct {
	Name   string // "#orders"
	Info   string // muted after the name
	Server string // at the right of the title bar
	Lines  []ChanLine
	Nicks  []Nick
	Status []string // bracketed segments on the status bar
	Input  string   // muted hint on the input line
}

// maxRowsPerLine keeps one huge record from filling the screen.
const maxRowsPerLine = 6

// nickHues are the piece's nick colours, in its order.
var nickHues = []struct {
	role kit.Role
	hex  string
}{{kit.Accent2, ""}, {kit.Accent, ""}, {kit.Warn, ""}, {0, "#ff9bce"}, {kit.Info, ""}, {kit.Notice, ""}, {kit.OK, ""}}

// Hue paints s in nick colour i.
func Hue(k kit.Kit, i int, s string) string {
	h := nickHues[((i%len(nickHues))+len(nickHues))%len(nickHues)]
	if h.hex != "" {
		return k.Hex(h.hex, s)
	}
	return k.S(h.role, s)
}

// Render draws the channel at width × height, newest lines at the bottom.
func (c Channel) Render(k kit.Kit, width, height int) []string {
	w := width
	nickW := 0
	if w >= 70 && len(c.Nicks) > 0 {
		nickW = 16
		for _, n := range c.Nicks {
			nickW = max(nickW, kit.Width(n.Name)+kit.Width(n.Count)+4)
		}
		nickW = min(nickW, 24)
	}
	cw := w - nickW
	if nickW > 0 {
		cw--
	}
	showMeta := cw >= 72

	title := k.S(kit.Info, " "+c.Name+" ")
	if c.Info != "" && kit.Width(c.Name)+kit.Width(c.Info)+4 <= w {
		title += k.S(kit.Muted, c.Info)
	}
	if c.Server != "" && kit.Width(title)+kit.Width(c.Server)+3 <= w {
		title = spread(title, k.S(kit.Rule, c.Server)+" ", w)
	}
	out := []string{title, k.S(kit.Faint, strings.Repeat("─", w))}

	var rows []string
	for _, l := range c.Lines {
		rows = append(rows, c.line(k, l, cw, showMeta)...)
	}
	vis := max(1, height-5)
	if len(rows) > vis {
		rows = rows[len(rows)-vis:]
	}
	for len(rows) < vis {
		rows = append(rows, "")
	}
	for i, r := range rows {
		line := kit.Fit(r, cw)
		if nickW > 0 {
			line += k.S(kit.Faint, "│") + c.nick(k, i, nickW)
		}
		out = append(out, line)
	}

	out = append(out, k.S(kit.Faint, strings.Repeat("─", w)))
	var status []string
	used := 0
	for _, s := range c.Status {
		seg := "[" + s + "]"
		if used > 0 && used+1+kit.Width(seg) > w {
			break
		}
		status = append(status, seg)
		used += kit.Width(seg) + 1
	}
	out = append(out, k.S(kit.Accent, strings.Join(status, " ")))
	prompt := k.S(kit.Info, "["+c.Name+"] ")
	if c.Input != "" && kit.Width(c.Name)+kit.Width(c.Input)+4 <= w {
		prompt += k.S(kit.Rule, c.Input)
	}
	out = append(out, prompt)
	return kit.Clip(width, out)
}

func (c Channel) nick(k kit.Kit, i, w int) string {
	if i >= len(c.Nicks) {
		return ""
	}
	n := c.Nicks[i]
	return " " + spread(Hue(k, n.Hue, n.Name), k.S(kit.Muted, n.Count), w-2)
}

// line lays out one channel line, wrapping the text under itself with the
// piece's six-cell hang and cutting very long ones after maxRowsPerLine.
func (c Channel) line(k kit.Kit, l ChanLine, w int, showMeta bool) []string {
	stamp := k.S(kit.Rule, l.Time.Format("15:04:05")+" ")
	if showMeta && l.Meta != "" {
		stamp += k.S(kit.Faint, l.Meta+" ")
	}
	var lead string
	textRole := kit.FG
	text := strings.NewReplacer("\r\n", " ↵ ", "\n", " ↵ ").Replace(l.Text)
	switch l.Kind {
	case LineJoin:
		lead = k.S(kit.OK, "-->") + " " + Hue(k, l.Hue, l.Nick) + " "
		textRole = kit.OK
	case LinePart:
		lead = k.S(kit.Err, "<--") + " " + Hue(k, l.Hue, l.Nick) + " "
		textRole = kit.Err
	case LineAct:
		lead = k.S(kit.Accent2, " * ")
		textRole = kit.Accent2
	default:
		lead = k.S(kit.Rule, "<") + Hue(k, l.Hue, l.Nick) + k.S(kit.Rule, "> ")
	}
	head := stamp + lead
	const hang = 6
	first := max(8, w-kit.Width(head))
	rest := max(8, w-hang)
	var rows []string
	// Wrap the first row at its own width, the rest at the hang width.
	text = strings.Join(strings.Fields(text), " ")
	parts := hardWrap(text, first)
	if len(parts) > 1 {
		tail := strings.TrimSpace(strings.TrimPrefix(text, parts[0]))
		parts = append(parts[:1], hardWrap(tail, rest)...)
	}
	for i, p := range parts {
		if i == maxRowsPerLine-1 && len(parts) > maxRowsPerLine {
			more := len(parts) - i
			rows = append(rows, strings.Repeat(" ", hang)+k.S(kit.Rule, fmt.Sprintf("⋯ %d more rows, -o json for all of it", more)))
			break
		}
		if i == 0 {
			rows = append(rows, head+k.S(textRole, p))
			continue
		}
		rows = append(rows, strings.Repeat(" ", hang)+k.S(textRole, p))
	}
	if len(parts) == 0 {
		rows = append(rows, head)
	}
	return rows
}

// hardWrap wraps s at spaces like kit.Wrap, and also breaks a word wider
// than w, since a record value is often one long run of JSON.
func hardWrap(s string, w int) []string {
	var words []string
	for _, word := range strings.Fields(s) {
		for kit.Width(word) > w {
			// Cut at the width, or just after the last comma within the
			// final third of it, so JSON breaks between fields.
			cut, cells, comma := 0, 0, 0
			for i, r := range word {
				rw := kit.Width(string(r))
				if cells+rw > w {
					cut = i
					break
				}
				cells += rw
				if r == ',' && cells >= w*2/3 {
					comma = i + 1
				}
			}
			if comma > 0 {
				cut = comma
			}
			words = append(words, word[:cut])
			word = word[cut:]
		}
		words = append(words, word)
	}
	return kit.Wrap(strings.Join(words, " "), w)
}
