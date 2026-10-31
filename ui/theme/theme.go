// Package theme holds KaaS's semantic colour tokens. Screens never name a
// colour directly; they ask for a role (accent, ok, err, rule), so every
// theme, colour profile and the web dashboard read the same palette.
package theme

import (
	"fmt"
	"image/color"
	"sort"
	"strconv"

	"charm.land/lipgloss/v2"
)

// Theme is a set of semantic tokens as #rrggbb strings. Heat is a dim-to-
// bright ramp used to recolour art and charts in the theme's own hue.
type Theme struct {
	Name      string
	BG        string // canvas
	FG        string // body text
	Muted     string // secondary text, key hints
	Accent    string // work in progress, focus, warmth
	Accent2   string // highlights within accent
	OK        string // done, healthy
	Warn      string // degraded
	Err       string // failure only
	Info      string // data, links
	Selection string // selected row background
	Rule      string // dotted rules and frames
	Heat      []string
}

var builtins = map[string]Theme{
	"amber": {
		Name:      "Amber Phosphor",
		BG:        "#0c0905",
		FG:        "#ffc66d",
		Muted:     "#8a6a3a",
		Accent:    "#ff9e2c",
		Accent2:   "#ffd8a8",
		OK:        "#7ee787",
		Warn:      "#f2cc60",
		Err:       "#ff6b5b",
		Info:      "#79c0ff",
		Selection: "#3a2a12",
		Rule:      "#4d3a1f",
		Heat:      []string{"#2a1a08", "#5c3610", "#9a5a14", "#d9822b", "#ffa657", "#ffd8a8"},
	},
	"ice": {
		Name:      "Ice",
		BG:        "#0a0c14",
		FG:        "#e6edf3",
		Muted:     "#8b949e",
		Accent:    "#a371f7",
		Accent2:   "#56d4dd",
		OK:        "#7ee787",
		Warn:      "#f2cc60",
		Err:       "#ff7b72",
		Info:      "#79c0ff",
		Selection: "#1f2937",
		Rule:      "#30363d",
		Heat:      []string{"#161b2e", "#2d2a5c", "#6e40c9", "#a371f7", "#79c0ff", "#56d4dd"},
	},
}

// Default is the theme used when none is chosen.
const Default = "amber"

// Get returns a built-in theme by key.
func Get(key string) (Theme, error) {
	t, ok := builtins[key]
	if !ok {
		return Theme{}, fmt.Errorf("unknown theme %q (have %v)", key, Names())
	}
	return t, nil
}

// Names lists the built-in theme keys, sorted.
func Names() []string {
	out := make([]string, 0, len(builtins))
	for k := range builtins {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// C turns a token into a colour for lipgloss.
func C(hex string) color.Color { return lipgloss.Color(hex) }

// Tint maps a colour onto the theme's heat ramp by brightness, so art drawn
// in any palette reads as this theme (amber pieces stay amber, blue ones turn
// amber too) while keeping its light and shade.
func (t Theme) Tint(hex string) string {
	r, g, b, ok := parseHex(hex)
	if !ok || len(t.Heat) == 0 {
		return t.FG
	}
	// Rec. 709 luma, 0..1.
	l := (0.2126*float64(r) + 0.7152*float64(g) + 0.0722*float64(b)) / 255
	i := int(l*float64(len(t.Heat)-1) + 0.5)
	return t.Heat[min(i, len(t.Heat)-1)]
}

func parseHex(s string) (r, g, b uint8, ok bool) {
	if len(s) != 7 || s[0] != '#' {
		return 0, 0, 0, false
	}
	v, err := strconv.ParseUint(s[1:], 16, 32)
	if err != nil {
		return 0, 0, 0, false
	}
	return uint8(v >> 16), uint8(v >> 8), uint8(v), true
}
