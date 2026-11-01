package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// What a settings row draws on its right.
const (
	SettingText   = iota // the value alone
	SettingToggle        // a switch, on or off
	SettingSlider        // a slider at Pos, and the value
)

// SettingRow is one line of a settings panel: a section's heading, or a
// setting.
type SettingRow struct {
	Heading string // a heading row when set
	Key     string
	Detail  string
	Control int
	On      bool    // a toggle's state
	Pos     float64 // a slider's knob, 0 to 1
	Value   string
	Set     bool   // not the default: drawn bright and tagged
	Tag     string // where a set value comes from: "topic", "broker"
	TagRole kit.Role
}

// Settings is the settings-toggles piece: a framed list of settings under
// headings, switches and sliders on the right, a focus marker on the
// selected one, and its documentation at the foot.
type Settings struct {
	Title string // on the frame, top left
	Badge string // on the frame, top right: "✓ 3 set on the topic"
	Rows  []SettingRow
	Sel   int      // the selected row; a heading is never selected
	Foot  []string // about the selected row, under the list
	Keys  string   // on the frame, bottom left
	Empty string   // when there are no rows
}

// toggle is a switch: a knob at the right end when on.
func toggle(c *canvas, x, y int, on bool) {
	edge, track, at := kit.Rule, kit.Rule, 0
	if on {
		edge, track, at = kit.OK, kit.OK, 4
	}
	c.put(x, y, "(", edge)
	for i := range 5 {
		switch {
		case i == at:
			c.put(x+1+i, y, "●", kit.FG)
		case i < at:
			c.put(x+1+i, y, "━", track)
		default:
			c.put(x+1+i, y, "─", track)
		}
	}
	c.put(x+6, y, ")", edge)
}

// slider is a track w wide with its knob at pos.
func slider(c *canvas, x, y, w int, pos float64) {
	at := int(min(1, max(0, pos))*float64(w-1) + 0.5)
	for i := range w {
		switch {
		case i == at:
			c.put(x+i, y, "●", kit.FG)
		case i < at:
			c.put(x+i, y, "━", kit.Accent)
		default:
			c.put(x+i, y, "─", kit.Rule)
		}
	}
}

// Height is how tall the panel is with every row in view.
func (s Settings) Height() int {
	n := len(s.Rows)
	for i, r := range s.Rows {
		if r.Heading != "" && i > 0 {
			n++ // the blank above it
		}
	}
	if len(s.Foot) > 0 {
		n += len(s.Foot) + 1
	}
	return max(12, n+2)
}

// Render draws the panel at width × height, scrolled to keep the selected
// row in view.
func (s Settings) Render(k kit.Kit, width, height int) []string {
	w, h := max(width, 60), max(height, 12)
	c := newCanvas(w, h)
	c.box(kit.Rule)
	c.put(2, 0, " "+clipTo(s.Title, w-24)+" ", kit.FG)
	if s.Badge != "" {
		b := " " + s.Badge + " "
		c.put(w-3-kit.Width(b), 0, b, kit.OK)
	}
	if s.Keys != "" {
		c.put(2, h-1, " "+clipTo(s.Keys, w-6)+" ", kit.Muted)
	}
	foot := len(s.Foot)
	if foot > 0 {
		foot++ // the rule above it
	}
	view := h - 2 - foot
	if len(s.Rows) == 0 {
		c.put(3, 2, clipTo(s.Empty, w-6), kit.Muted)
		return kit.Clip(width, c.lines(k))
	}

	var lines []int // the rows in the list, -1 for the blank before a heading
	sel := 0
	keyW := 0
	for i, r := range s.Rows {
		if r.Heading != "" {
			if i > 0 {
				lines = append(lines, -1)
			}
		} else {
			keyW = max(keyW, kit.Width(r.Key))
		}
		if i == s.Sel {
			sel = len(lines)
		}
		lines = append(lines, i)
	}
	top := 0
	if len(lines) > view {
		top = min(max(0, sel-view/2), len(lines)-view)
	}

	// Columns from the right: the tag, the value, the control.
	tagX := w - 10
	valX := tagX - 11
	ctlX := valX - 14
	keyW = min(keyW, max(16, ctlX-3-20))
	detX := 3 + keyW + 2
	for y, row := range lines[top:min(len(lines), top+view)] {
		y++
		if row < 0 {
			continue
		}
		r := s.Rows[row]
		if r.Heading != "" {
			name := strings.ToUpper(r.Heading)
			c.put(2, y, name, kit.Accent2)
			c.hline(3+kit.Width(name), w-3, y, kit.Faint)
			continue
		}
		focus := row == s.Sel
		keyRole, valRole := kit.Muted, kit.Muted
		if r.Set {
			keyRole, valRole = kit.FG, kit.FG
		}
		if focus {
			c.put(1, y, "▌", kit.Accent)
			keyRole = kit.Accent
		}
		c.put(3, y, clipTo(r.Key, keyW), keyRole)
		if ctlX-detX-2 >= 8 {
			c.put(detX, y, clipTo(r.Detail, ctlX-detX-2), kit.Muted)
		}
		switch r.Control {
		case SettingToggle:
			toggle(c, valX-8, y, r.On)
			v, role := "Off", kit.Muted
			if r.On {
				v, role = "On", kit.OK
			}
			c.put(valX, y, v, role)
		case SettingSlider:
			slider(c, ctlX, y, 12, r.Pos)
			c.put(valX, y, lpad(clipTo(r.Value, 10), 10), valRole)
		default:
			v := clipTo(r.Value, tagX-ctlX-1)
			c.put(tagX-1-kit.Width(v), y, v, valRole)
		}
		if r.Set && r.Tag != "" {
			c.put(tagX+1, y, clipTo(r.Tag, 8), r.TagRole)
		}
	}
	if len(lines) > view {
		// Where the view is in the list, on the frame's right edge.
		at := 1 + (view-1)*top/max(1, len(lines)-view)
		c.put(w-1, at, "┃", kit.Muted)
	}
	if foot > 0 {
		fy := h - 1 - foot
		c.hline(1, w-2, fy, kit.Faint)
		for i, l := range s.Foot {
			role := kit.Muted
			if i == 0 {
				role = kit.FG
			}
			c.put(3, fy+1+i, clipTo(l, w-6), role)
		}
	}
	return kit.Clip(width, c.lines(k))
}
