package kit

import (
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// font is a 4×5 pixel face for the big status codes on error pages. Each
// pixel is two cells wide so the glyphs come out roughly square.
var font = map[rune][5]string{
	'0': {"####", "#..#", "#..#", "#..#", "####"},
	'1': {"..#.", ".##.", "..#.", "..#.", ".###"},
	'2': {"####", "...#", "####", "#...", "####"},
	'3': {"####", "...#", ".###", "...#", "####"},
	'4': {"#..#", "#..#", "####", "...#", "...#"},
	'5': {"####", "#...", "####", "...#", "####"},
	'6': {"####", "#...", "####", "#..#", "####"},
	'7': {"####", "...#", "..#.", ".#..", ".#.."},
	'8': {"####", "#..#", "####", "#..#", "####"},
	'9': {"####", "#..#", "####", "...#", "####"},
}

// BigHeight is how many lines Big returns.
const BigHeight = 5

// Big renders digits in the block face, lit pixels in role r and unlit ones
// as a faint ░ grid, like the template's dot-matrix codes. Runes the face
// lacks are skipped.
func (k Kit) Big(s string, r Role) []string {
	return k.big(s, func(int, int) string { return k.S(r, "██") })
}

// BigRamp is Big painted along a gradient: down the rows, as page-500 burns
// from dark red to yellow, or across the columns, as page-404 fades from
// purple to cyan.
func (k Kit) BigRamp(s string, ramp []string, across bool) []string {
	cols := 0
	for _, ch := range s {
		if _, ok := font[ch]; ok {
			cols += 5 // four pixels and the gap
		}
	}
	return k.big(s, func(x, y int) string {
		if across {
			return k.Hex(theme.Ramp(ramp, x, cols), "██")
		}
		return k.Hex(theme.Ramp(ramp, y, BigHeight), "██")
	})
}

func (k Kit) big(s string, lit func(x, y int) string) []string {
	out := make([]string, BigHeight)
	col := 0
	for _, ch := range s {
		g, ok := font[ch]
		if !ok {
			continue
		}
		for y := range BigHeight {
			if col > 0 {
				out[y] += "  "
			}
			var dim strings.Builder
			for x, px := range g[y] {
				if px != '#' {
					dim.WriteString("░░")
					continue
				}
				if dim.Len() > 0 {
					out[y] += k.S(Faint, dim.String())
					dim.Reset()
				}
				out[y] += lit(col+x, y)
			}
			out[y] += k.S(Faint, dim.String())
		}
		col += 5
	}
	return out
}
