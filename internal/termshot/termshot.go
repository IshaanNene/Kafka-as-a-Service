// Package termshot turns styled terminal output into an SVG "screenshot":
// a window with the theme's background, traffic lights and a title, and
// every cell placed on a fixed grid so box drawing lines up in any browser
// and on GitHub.
//
// It understands the SGR codes kaas emits: reset, bold, and 24-bit
// foreground and background colours. Everything else is dropped.
package termshot

import (
	"fmt"
	"html"
	"strconv"
	"strings"

	"github.com/charmbracelet/x/ansi"
)

// Options describes the window.
type Options struct {
	Title  string
	BG     string // window background, #rrggbb
	FG     string // default text colour
	Chrome string // title bar and border colour
	Cols   int    // grid width; 0 fits the widest line
}

const (
	cellW    = 8.4 // px per column at 14px monospace
	cellH    = 18.0
	padX     = 16.0
	padTop   = 44.0 // room for the title bar
	padBot   = 16.0
	fontSize = 14
)

type run struct {
	col    int
	text   string
	fg, bg string
	bold   bool
}

// SVG renders text, which may contain ANSI escapes, as a terminal window.
func SVG(text string, o Options) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	cols := o.Cols
	var rows [][]run
	for _, l := range lines {
		r := parse(l)
		rows = append(rows, r)
		if o.Cols == 0 {
			cols = max(cols, ansi.StringWidth(l))
		}
	}
	w := padX*2 + float64(cols)*cellW
	h := padTop + float64(len(rows))*cellH + padBot

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" role="img" aria-label="%s">`+"\n",
		w, h, w, h, html.EscapeString(o.Title))
	fmt.Fprintf(&b, `<rect width="100%%" height="100%%" rx="10" fill="%s" stroke="%s"/>`+"\n", o.BG, o.Chrome)
	for i, c := range []string{"#ff5f57", "#febc2e", "#28c840"} {
		fmt.Fprintf(&b, `<circle cx="%d" cy="18" r="6" fill="%s"/>`+"\n", 22+i*20, c)
	}
	fmt.Fprintf(&b, `<text x="%.0f" y="22" fill="%s" font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" font-size="12" text-anchor="middle">%s</text>`+"\n",
		w/2, o.Chrome, html.EscapeString(o.Title))
	fmt.Fprintf(&b, `<g font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,'DejaVu Sans Mono',monospace" font-size="%d" xml:space="preserve">`+"\n", fontSize)
	for y, row := range rows {
		top := padTop + float64(y)*cellH
		for _, r := range row {
			x := padX + float64(r.col)*cellW
			n := ansi.StringWidth(r.text)
			if r.bg != "" {
				fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.0f" fill="%s"/>`, x, top, float64(n)*cellW, cellH, r.bg)
			}
			fg := r.fg
			if fg == "" {
				fg = o.FG
			}
			weight := ""
			if r.bold {
				weight = ` font-weight="bold"`
			}
			// Each word is placed at its own column and pinned to its cells
			// with textLength: spaces never reach the SVG, so renderers can't
			// collapse them, and font advance widths can't drift the grid.
			col := r.col
			for _, word := range strings.SplitAfter(r.text, " ") {
				ww := ansi.StringWidth(word)
				trim := strings.TrimRight(word, " ")
				if trim != "" {
					tw := ansi.StringWidth(trim)
					fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" fill="%s"%s textLength="%.1f" lengthAdjust="spacingAndGlyphs">%s</text>`+"\n",
						padX+float64(col)*cellW, top+cellH-5, fg, weight, float64(tw)*cellW, html.EscapeString(trim))
				}
				col += ww
			}
		}
	}
	b.WriteString("</g>\n</svg>\n")
	return b.String()
}

// parse splits one line into runs of identical style, tracking columns.
func parse(line string) []run {
	var (
		out  []run
		cur  run
		col  int
		text strings.Builder
	)
	flush := func() {
		if text.Len() > 0 {
			cur.text = text.String()
			out = append(out, cur)
			col += ansi.StringWidth(cur.text)
			text.Reset()
		}
		cur.col = col
	}
	for i := 0; i < len(line); {
		if line[i] == 0x1b && i+1 < len(line) && line[i+1] == '[' {
			j := i + 2
			for j < len(line) && (line[j] < 0x40 || line[j] > 0x7e) {
				j++
			}
			if j < len(line) && line[j] == 'm' {
				flush()
				cur = sgr(cur, line[i+2:j])
				cur.col = col
			}
			i = j + 1
			continue
		}
		text.WriteByte(line[i])
		i++
	}
	flush()
	return out
}

func sgr(r run, params string) run {
	ps := strings.Split(params, ";")
	if params == "" {
		ps = []string{"0"}
	}
	for i := 0; i < len(ps); i++ {
		switch ps[i] {
		case "0", "":
			r = run{}
		case "1":
			r.bold = true
		case "22":
			r.bold = false
		case "39":
			r.fg = ""
		case "49":
			r.bg = ""
		case "38", "48":
			if i+4 < len(ps) && ps[i+1] == "2" {
				hex := rgb(ps[i+2], ps[i+3], ps[i+4])
				if ps[i] == "38" {
					r.fg = hex
				} else {
					r.bg = hex
				}
				i += 4
			}
		}
	}
	return r
}

func rgb(r, g, b string) string {
	c := func(s string) int { n, _ := strconv.Atoi(s); return n }
	return fmt.Sprintf("#%02x%02x%02x", c(r), c(g), c(b))
}
