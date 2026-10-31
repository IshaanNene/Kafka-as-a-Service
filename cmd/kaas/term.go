package main

import (
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/term"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// surface is where a screen is going and how much it may draw there.
//
// The contract: rich layouts only on an interactive terminal. Pipes,
// redirects and TERM=dumb get stable plain text that scripts can parse;
// NO_COLOR keeps the layout and drops the colour (no-color.org).
type surface struct {
	w      io.Writer
	raw    io.Writer // w without colour downsampling, for cursor control
	rich   bool
	width  int
	height int
	kit    kit.Kit
}

// detect inspects w. Anything that is not an *os.File on a terminal, such as
// the buffers tests use, is plain.
func detect(w io.Writer, themeName string) surface {
	s := surface{w: w, raw: w, width: 80, height: 24}
	f, ok := w.(*os.File)
	if !ok || !term.IsTerminal(f.Fd()) || os.Getenv("TERM") == "dumb" {
		return s
	}
	th, err := theme.Get(themeName)
	if err != nil {
		th, _ = theme.Get(theme.Default)
	}
	if cols, rows, err := term.GetSize(f.Fd()); err == nil && cols > 0 {
		s.width, s.height = cols, rows
	}
	// colorprofile downsamples truecolour to what the terminal supports and
	// strips it entirely under NO_COLOR.
	s.w = colorprofile.NewWriter(f, os.Environ())
	s.rich = true
	s.kit = kit.New(th, os.Getenv("NO_COLOR") == "")
	return s
}

// erase clears the last n lines printed, leaving the cursor where they began.
func (s surface) erase(n int) {
	_, _ = io.WriteString(s.raw, strings.Repeat("\x1b[1A\x1b[2K", n))
}

func (s surface) print(b []string) {
	_, _ = io.WriteString(s.w, kit.Join(b))
}

// live redraws a block in place: each draw replaces the previous one, so a
// screen can update without taking over the terminal.
type live struct {
	s     surface
	lines int
}

func (l *live) draw(b []string) {
	b = kit.Clip(l.s.width, b)
	if h := l.s.height - 1; h > 0 && len(b) > h {
		b = b[:h] // a block taller than the terminal can't be redrawn in place
	}
	var sb strings.Builder
	if l.lines > 0 {
		sb.WriteString("\x1b[" + strconv.Itoa(l.lines) + "A\r")
	} else {
		sb.WriteString("\x1b[?25l") // hide the cursor while drawing
	}
	_, _ = io.WriteString(l.s.raw, sb.String())
	for _, line := range b {
		_, _ = io.WriteString(l.s.raw, "\x1b[2K")
		_, _ = io.WriteString(l.s.w, strings.TrimRight(line, " ")+"\n")
	}
	for range l.lines - len(b) {
		_, _ = io.WriteString(l.s.raw, "\x1b[2K\n")
	}
	if extra := l.lines - len(b); extra > 0 {
		_, _ = io.WriteString(l.s.raw, "\x1b["+strconv.Itoa(extra)+"A")
	}
	l.lines = len(b)
}

// done leaves the last frame on screen and restores the cursor.
func (l *live) done() { _, _ = io.WriteString(l.s.raw, "\x1b[?25h") }
