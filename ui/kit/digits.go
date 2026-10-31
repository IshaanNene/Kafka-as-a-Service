package kit

import "strings"

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
// as a faint ░ grid in Rule, like the template's dot-matrix codes. Runes the
// face lacks are skipped.
func (k Kit) Big(s string, r Role) []string {
	out := make([]string, BigHeight)
	first := true
	for _, ch := range s {
		g, ok := font[ch]
		if !ok {
			continue
		}
		for y := range BigHeight {
			if !first {
				out[y] += "  "
			}
			var lit, dim strings.Builder
			flush := func() {
				if lit.Len() > 0 {
					out[y] += k.S(r, lit.String())
					lit.Reset()
				}
				if dim.Len() > 0 {
					out[y] += k.S(Rule, dim.String())
					dim.Reset()
				}
			}
			for _, px := range g[y] {
				if px == '#' {
					if dim.Len() > 0 {
						flush()
					}
					lit.WriteString("██")
				} else {
					if lit.Len() > 0 {
						flush()
					}
					dim.WriteString("░░")
				}
			}
			flush()
		}
		first = false
	}
	return out
}
