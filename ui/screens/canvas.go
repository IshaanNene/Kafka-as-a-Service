package screens

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// canvas is a grid of styled cells, for the pieces that place text by
// column and row rather than flowing it.
type canvas struct {
	w, h  int
	cells [][]canvasCell
}

type canvasCell struct {
	s   string
	r   kit.Role
	hex string // a colour no role has, such as a flame's; overrides r
}

func newCanvas(w, h int) *canvas {
	c := &canvas{w: w, h: h, cells: make([][]canvasCell, h)}
	for y := range c.cells {
		c.cells[y] = make([]canvasCell, w)
		for x := range c.cells[y] {
			c.cells[y][x] = canvasCell{s: " ", r: kit.FG}
		}
	}
	return c
}

// put writes s from (x, y), dropping what falls outside, and returns the
// column after it.
func (c *canvas) put(x, y int, s string, r kit.Role) int {
	for _, ch := range s {
		if y >= 0 && y < c.h && x >= 0 && x < c.w {
			c.cells[y][x] = canvasCell{s: string(ch), r: r}
		}
		x++
	}
	return x
}

// putHex is put in a colour given as hex.
func (c *canvas) putHex(x, y int, s, hex string) int {
	for _, ch := range s {
		if y >= 0 && y < c.h && x >= 0 && x < c.w {
			c.cells[y][x] = canvasCell{s: string(ch), hex: hex}
		}
		x++
	}
	return x
}

// blank is whether (x, y) holds a space.
func (c *canvas) blank(x, y int) bool {
	return y >= 0 && y < c.h && x >= 0 && x < c.w && c.cells[y][x].s == " "
}

func (c *canvas) hline(x0, x1, y int, r kit.Role) {
	c.put(x0, y, strings.Repeat("─", max(0, x1-x0+1)), r)
}

// box draws a rounded frame around the whole canvas.
func (c *canvas) box(r kit.Role) {
	c.put(0, 0, "╭"+strings.Repeat("─", c.w-2)+"╮", r)
	c.put(0, c.h-1, "╰"+strings.Repeat("─", c.w-2)+"╯", r)
	for y := 1; y < c.h-1; y++ {
		c.put(0, y, "│", r)
		c.put(c.w-1, y, "│", r)
	}
}

// lines styles each row, a run of one role at a time.
func (c *canvas) lines(k kit.Kit) []string {
	out := make([]string, c.h)
	for y, row := range c.cells {
		var sb strings.Builder
		for i := 0; i < len(row); {
			j := i
			var run strings.Builder
			for j < len(row) && row[j].r == row[i].r && row[j].hex == row[i].hex {
				run.WriteString(row[j].s)
				j++
			}
			if row[i].hex != "" {
				sb.WriteString(k.Hex(row[i].hex, run.String()))
			} else {
				sb.WriteString(k.S(row[i].r, run.String()))
			}
			i = j
		}
		out[y] = sb.String()
	}
	return out
}

// clipTo cuts s to n cells, ending in … when it had more.
func clipTo(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if kit.Width(s) > n {
		rs := []rune(s)
		return string(rs[:max(0, n-1)]) + "…"
	}
	return s
}
