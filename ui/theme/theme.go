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
// bright ramp used to recolour art in the theme's own hue; Chart, Hot and
// Cool are the gradients screens paint data and big status codes with.
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
	Notice    string // the selection marker: >> and the chosen button
	Selection string // selected row background
	Rule      string // frames
	Faint     string // dividers, empty cells, pending dots
	Heat      []string
	Chart     []string // bars and histograms, low to high
	Hot       []string // failure codes (500), top to bottom
	Cool      []string // not-found codes (404), left to right
	Native    bool     // art keeps its own palette: Tint is the identity
}

var builtins = map[string]Theme{
	// magic is the palette the ascii-magic-art UI pieces are drawn in, so
	// each kaas screen looks like the piece it was ported from.
	"magic": {
		Name:      "ascii-magic",
		BG:        "#0d1117",
		FG:        "#e6edf3",
		Muted:     "#8b949e",
		Accent:    "#79c0ff",
		Accent2:   "#d2a8ff",
		OK:        "#7ee787",
		Warn:      "#ffa657",
		Err:       "#ff7b72",
		Info:      "#56d4dd",
		Notice:    "#f2cc60",
		Selection: "#21262d",
		Rule:      "#484f58",
		Faint:     "#30363d",
		Heat:      []string{"#0d2d5e", "#1f6feb", "#79c0ff", "#a5d6ff", "#e6edf3"},
		Chart:     []string{"#1f6feb", "#3d8af2", "#5ba5f8", "#79c0ff", "#97b8ff", "#b4b0ff", "#d2a8ff", "#de98e8", "#eb88d1", "#f778ba"},
		Hot:       []string{"#8b1d1d", "#bd4541", "#ee6e66", "#ff876a", "#ff9a5f", "#fdab58", "#f8bc5c", "#f2cc60"},
		Cool:      []string{"#6e40c9", "#8050d8", "#9161e8", "#a371f7", "#958bfa", "#87a6fc", "#79c0ff", "#6dc7f4", "#62cde8", "#56d4dd"},
		Native:    true,
	},
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
		Notice:    "#ffe7c2",
		Selection: "#3a2a12",
		Rule:      "#4d3a1f",
		Faint:     "#33261a",
		Heat:      []string{"#2a1a08", "#5c3610", "#9a5a14", "#d9822b", "#ffa657", "#ffd8a8"},
		Chart:     []string{"#9a5a14", "#d9822b", "#ff9e2c", "#ffa657", "#ffc66d", "#ffd8a8"},
		Hot:       []string{"#8b1d1d", "#c2410c", "#ff6b5b", "#ff9e2c", "#ffc66d"},
		Cool:      []string{"#9a5a14", "#d9822b", "#ff9e2c", "#ffc66d", "#ffd8a8"},
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
		Notice:    "#f2cc60",
		Selection: "#1f2937",
		Rule:      "#30363d",
		Faint:     "#21262d",
		Heat:      []string{"#161b2e", "#2d2a5c", "#6e40c9", "#a371f7", "#79c0ff", "#56d4dd"},
		Chart:     []string{"#6e40c9", "#a371f7", "#79c0ff", "#56d4dd"},
		Hot:       []string{"#8b1d1d", "#bd4541", "#ff7b72", "#ffa657", "#f2cc60"},
		Cool:      []string{"#6e40c9", "#a371f7", "#79c0ff", "#56d4dd"},
	},
}

// Default is the theme used when none is chosen.
const Default = "magic"

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

// Ramp picks the colour for position i of n along ramp, so a gradient
// stretches over any number of rows, bars or columns.
func Ramp(ramp []string, i, n int) string {
	if len(ramp) == 0 {
		return ""
	}
	if n <= 1 {
		return ramp[len(ramp)-1]
	}
	return ramp[min(i*(len(ramp)-1)/(n-1), len(ramp)-1)]
}

// C turns a token into a colour for lipgloss.
func C(hex string) color.Color { return lipgloss.Color(hex) }

// Tint maps a colour onto the theme's heat ramp by brightness, so art drawn
// in any palette reads as this theme (amber pieces stay amber, blue ones turn
// amber too) while keeping its light and shade.
func (t Theme) Tint(hex string) string {
	if t.Native {
		if _, _, _, ok := parseHex(hex); ok {
			return hex
		}
		return t.FG
	}
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
