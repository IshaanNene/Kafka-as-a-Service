package screens

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Modal editor modes.
const (
	ModeNormal  = "NORMAL"
	ModeInsert  = "INSERT"
	ModeCommand = "COMMAND"
)

// tildeHex is the piece's colour for the ~ past the end of the file.
const tildeHex = "#1f6feb"

// ModalDiff is one line of the change panel: + set, ~ changed, - unset.
type ModalDiff struct {
	Sign     string
	Key      string
	From, To string
}

// ModalEditor is the modal-editor piece: a config file in a vi-style
// buffer with relative line numbers, a status line with the mode, file
// and cursor, and a command line under it. Diff, when set, is a panel
// over the bottom of the buffer showing what a write would change.
type ModalEditor struct {
	File      string
	Lines     []string
	Row, Col  int // the cursor, in runes
	Mode      string
	Command   string // the command line being typed, from its :
	Msg       string
	MsgRole   kit.Role
	Dirty     bool
	Cursor    bool   // the cursor drawn this frame: it blinks
	Highlight string // text to mark across the buffer, as after a substitution
	Last      string // the last keys typed in normal mode
	Diff      []ModalDiff
	DiffTitle string
	DiffAsk   string
}

// paintConfig colours one line of a config file as the piece does: #
// comments muted, [sections] purple, keys blue, = red, numbers orange,
// other values green, and an inline note muted.
func paintConfig(c *canvas, x, y int, line string, maxW int) {
	t := strings.TrimSpace(line)
	switch {
	case strings.HasPrefix(t, "#"):
		c.put(x, y, clipTo(line, maxW), kit.Muted)
		return
	case strings.HasPrefix(t, "["):
		c.put(x, y, clipTo(line, maxW), kit.Accent2)
		return
	}
	body, note := line, ""
	if i := strings.Index(line, "  #"); i >= 0 {
		body, note = line[:i], line[i:]
	}
	eq := strings.Index(body, "=")
	at := x
	end := x + maxW
	write := func(s string, r kit.Role) {
		if at >= end {
			return
		}
		at = c.put(at, y, clipTo(s, end-at), r)
	}
	if eq < 0 {
		write(body, kit.FG)
	} else {
		write(body[:eq], kit.Accent)
		write("=", kit.Err)
		val := body[eq+1:]
		role := kit.OK
		if _, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil {
			role = kit.Warn
		}
		write(val, role)
	}
	write(note, kit.Muted)
}

// Render draws the editor at width × height.
func (m ModalEditor) Render(k kit.Kit, width, height int) []string {
	w, h := max(width, 40), max(height, 8)
	c := newCanvas(w, h)
	vis := h - 2
	top := 0
	if m.Row >= vis {
		top = m.Row - vis + 1
	}
	if len(m.Lines) > vis {
		top = min(max(0, m.Row-vis/2), len(m.Lines)-vis)
	}
	const tx = 4
	for i := range vis {
		li := top + i
		if li >= len(m.Lines) {
			c.putHex(0, i, "~", tildeHex)
			continue
		}
		cur := li == m.Row
		num, role := strconv.Itoa(absInt(li-m.Row)), kit.Rule
		if cur {
			num, role = strconv.Itoa(li+1), kit.Notice
		}
		c.put(0, i, lpad(num, 3), role)
		line := m.Lines[li]
		paintConfig(c, tx, i, line, w-tx)
		if m.Highlight != "" {
			for from := 0; ; {
				j := strings.Index(line[from:], m.Highlight)
				if j < 0 {
					break
				}
				c.put(tx+kit.Width(line[:from+j]), i, m.Highlight, kit.Notice)
				from += j + len(m.Highlight)
			}
		}
		if cur && m.Mode != ModeCommand && m.Cursor {
			if m.Mode == ModeInsert {
				c.put(tx+m.Col, i, "▏", kit.FG)
			} else {
				c.put(tx+m.Col, i, "█", kit.FG)
			}
		}
	}

	if len(m.Diff) > 0 {
		// The change panel, over the bottom of the buffer.
		n, last := len(m.Diff)+1, vis // the title and the changes, then a blank and the question
		if m.DiffAsk != "" {
			n, last = n+2, vis-2
		}
		py := max(1, vis-n)
		for y := py; y < vis; y++ {
			c.put(0, y, strings.Repeat(" ", w), kit.FG)
		}
		c.hline(0, w-1, py, kit.Faint)
		c.put(2, py, " "+m.DiffTitle+" ", kit.FG)
		kw := 0
		for _, d := range m.Diff {
			kw = max(kw, kit.Width(d.Key))
		}
		for i, d := range m.Diff {
			y := py + 1 + i
			if y >= last-1 && i < len(m.Diff)-1 {
				c.put(4, y, fmt.Sprintf("… and %d more", len(m.Diff)-i), kit.Muted)
				break
			}
			role := map[string]kit.Role{"+": kit.OK, "~": kit.Notice, "-": kit.Err}[d.Sign]
			c.put(2, y, d.Sign, role)
			x := c.put(4, y, kit.Fit(d.Key, kw), kit.Accent) + 2
			switch d.Sign {
			case "+":
				c.put(x, y, clipTo(d.To, w-x-1), kit.OK)
			case "-":
				x = c.put(x, y, clipTo(d.From, w-x-1), kit.Muted)
				c.put(x, y, clipTo(" → "+d.To+" (unset)", w-x-1), kit.Err)
			default:
				x = c.put(x, y, clipTo(d.From, w-x-1), kit.Muted)
				c.put(x, y, clipTo(" → "+d.To, w-x-1), kit.Notice)
			}
		}
		if m.DiffAsk != "" {
			c.put(2, vis-1, clipTo(m.DiffAsk, w-4), kit.Warn)
		}
	}

	// The status line.
	sy := h - 2
	c.put(0, sy, strings.Repeat("▄", w), kit.Faint)
	modeRole := kit.Accent
	switch m.Mode {
	case ModeInsert:
		modeRole = kit.OK
	case ModeCommand:
		modeRole = kit.Warn
	}
	c.put(0, sy, " "+m.Mode+" ", modeRole)
	file := " " + m.File
	if m.Dirty {
		file += " [+]"
	}
	c.put(10, sy, clipTo(file+" ", w-42), kit.FG)
	col := min(m.Col, max(0, runeLen(lineAt(m.Lines, m.Row))))
	pos := fmt.Sprintf(" conf  utf-8  %2d:%-3d ", m.Row+1, col+1)
	c.put(w-6-kit.Width(pos), sy, pos, kit.Muted)
	pct := 100
	if len(m.Lines) > 0 {
		pct = (m.Row + 1) * 100 / len(m.Lines)
	}
	c.put(w-6, sy, lpad(" "+strconv.Itoa(pct)+"% ", 6), modeRole)

	// The command line.
	cy := h - 1
	switch {
	case m.Mode == ModeCommand:
		x := c.put(0, cy, clipTo(m.Command, w-2), kit.FG)
		if m.Cursor {
			c.put(x, cy, "█", kit.FG)
		}
	case m.Mode == ModeInsert && m.Msg == "":
		c.put(0, cy, "-- INSERT --", kit.OK)
	case m.Msg != "":
		c.put(0, cy, clipTo(m.Msg, w-14), m.MsgRole)
	}
	if m.Last != "" && m.Mode == ModeNormal {
		c.put(w-12, cy, lpad(m.Last, 4), kit.Rule)
	}
	return kit.Clip(width, c.lines(k))
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func lineAt(ls []string, i int) string {
	if i < 0 || i >= len(ls) {
		return ""
	}
	return ls[i]
}

func runeLen(s string) int { return len([]rune(s)) }
