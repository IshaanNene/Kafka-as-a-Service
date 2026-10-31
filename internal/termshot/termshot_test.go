package termshot

import (
	"strings"
	"testing"
)

func TestSVG(t *testing.T) {
	in := "\x1b[1;38;2;255;158;44mK A A S\x1b[0m  plain\n\x1b[38;2;255;198;109;48;2;58;42;18m▌sel\x1b[m"
	got := SVG(in, Options{Title: "kaas", BG: "#0c0905", FG: "#ffc66d", Chrome: "#4d3a1f"})
	for _, want := range []string{
		`fill="#ff9e2c" font-weight="bold"`, ">K<", ">S<", ">plain<",
		`fill="#3a2a12"`, ">▌sel<", `aria-label="kaas"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	// "plain" starts at column 9: x = 16 + 9*8.4. Spaces never reach the
	// SVG, so a renderer can't collapse them and shift it.
	if !strings.Contains(got, `x="91.6" y="57.0" fill="#ffc66d" textLength="42.0"`) {
		t.Errorf("plain not pinned to columns 9-13:\n%s", got)
	}
	if strings.Contains(got, "> ") {
		t.Errorf("a text element carries spaces:\n%s", got)
	}
}
