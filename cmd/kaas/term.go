package main

import (
	"io"
	"os"
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
	w     io.Writer
	raw   io.Writer // w without colour downsampling, for cursor control
	rich  bool
	width int
	kit   kit.Kit
}

// detect inspects w. Anything that is not an *os.File on a terminal, such as
// the buffers tests use, is plain.
func detect(w io.Writer, themeName string) surface {
	s := surface{w: w, raw: w, width: 80}
	f, ok := w.(*os.File)
	if !ok || !term.IsTerminal(f.Fd()) || os.Getenv("TERM") == "dumb" {
		return s
	}
	th, err := theme.Get(themeName)
	if err != nil {
		th, _ = theme.Get(theme.Default)
	}
	if cols, _, err := term.GetSize(f.Fd()); err == nil && cols > 0 {
		s.width = cols
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
