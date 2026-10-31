// Package asciipack plays ascii-magic-art pieces baked into frame packs by
// tools/asciipack. A pack is the piece's frames sampled once at build time,
// so playback is a lookup by elapsed time and the binary ships no
// JavaScript. The pieces are MIT licensed by ASCII Magic; see
// packs/LICENSE.ascii-magic-art and packs/NOTICE.ascii-magic-art.
package asciipack

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"charm.land/lipgloss/v2"
)

//go:embed packs/*.pack.gz
var packsFS embed.FS

// Pack is one baked piece.
type Pack struct {
	Slug     string            `json:"slug"`
	Name     string            `json:"name"`
	Note     string            `json:"note"`
	Category string            `json:"category"`
	Cols     int               `json:"cols"`
	Rows     int               `json:"rows"`
	FPS      int               `json:"fps"`
	Palette  map[string]string `json:"palette"` // colour key -> #rrggbb
	Frames   []Frame           `json:"frames"`
}

// Frame is rows of characters and a same-shaped grid of colour keys; a
// space key means default ink. C is empty for uncoloured pieces.
type Frame struct {
	T string `json:"t"`
	C string `json:"c"`
}

var (
	mu    sync.Mutex
	cache = map[string]*Pack{}
)

// List returns the slugs of every embedded pack, sorted.
func List() []string {
	ents, _ := packsFS.ReadDir("packs")
	var out []string
	for _, e := range ents {
		if s, ok := strings.CutSuffix(e.Name(), ".pack.gz"); ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// Load decodes a pack by slug. Packs are decoded once and cached.
func Load(slug string) (*Pack, error) {
	mu.Lock()
	defer mu.Unlock()
	if p, ok := cache[slug]; ok {
		return p, nil
	}
	raw, err := packsFS.ReadFile(path.Join("packs", slug+".pack.gz"))
	if err != nil {
		return nil, fmt.Errorf("asciipack: no piece %q (have %v)", slug, List())
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("asciipack: %s: %w", slug, err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("asciipack: %s: %w", slug, err)
	}
	var p Pack
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("asciipack: %s: %w", slug, err)
	}
	if len(p.Frames) == 0 || p.FPS <= 0 {
		return nil, fmt.Errorf("asciipack: %s: empty pack", slug)
	}
	cache[slug] = &p
	return &p, nil
}

// Duration is one loop of the pack.
func (p *Pack) Duration() time.Duration {
	return time.Duration(len(p.Frames)) * time.Second / time.Duration(p.FPS)
}

// FrameAt returns the frame shown d into playback, looping.
func (p *Pack) FrameAt(d time.Duration) Frame {
	i := int(d * time.Duration(p.FPS) / time.Second)
	return p.Frames[((i%len(p.Frames))+len(p.Frames))%len(p.Frames)]
}

// RenderOptions controls how a frame becomes terminal text.
type RenderOptions struct {
	Width, Height int                     // crop to this, centred; 0 means the piece's size
	Recolor       func(hex string) string // e.g. theme.Tint; nil keeps the piece's palette
	Plain         bool                    // no colour at all (NO_COLOR, --plain)
	Default       string                  // ink for cells with no colour key; "" leaves them unstyled
}

// Render turns a frame into styled text. Runs of cells sharing a colour are
// styled together, which keeps escape sequences to a minimum. Colours are
// emitted as truecolor; Bubble Tea or a colorprofile writer downsamples them
// to what the terminal supports.
func (p *Pack) Render(f Frame, o RenderOptions) string {
	text := strings.Split(f.T, "\n")
	keys := strings.Split(f.C, "\n")
	x0, y0, w, h := crop(p.Cols, p.Rows, o.Width, o.Height)

	styles := map[byte]lipgloss.Style{}
	style := func(k byte) (lipgloss.Style, bool) {
		if s, ok := styles[k]; ok {
			return s, true
		}
		hex := o.Default
		if k != ' ' {
			if c, ok := p.Palette[string(k)]; ok {
				hex = c
			}
		}
		if hex == "" {
			return lipgloss.Style{}, false
		}
		if o.Recolor != nil {
			hex = o.Recolor(hex)
		}
		s := lipgloss.NewStyle().Foreground(lipgloss.Color(hex))
		styles[k] = s
		return s, true
	}

	var b strings.Builder
	for y := y0; y < y0+h; y++ {
		if y > y0 {
			b.WriteByte('\n')
		}
		var row []rune
		if y < len(text) {
			row = []rune(text[y])
		}
		var krow string
		if y < len(keys) {
			krow = keys[y]
		}
		cell := func(x int) (rune, byte) {
			r, k := ' ', byte(' ')
			if x < len(row) {
				r = row[x]
			}
			if x < len(krow) {
				k = krow[x]
			}
			return r, k
		}
		for x := x0; x < x0+w; {
			r, k := cell(x)
			run := []rune{r}
			x++
			for x < x0+w {
				r2, k2 := cell(x)
				if k2 != k {
					break
				}
				run = append(run, r2)
				x++
			}
			s, ok := style(k)
			if o.Plain || !ok {
				b.WriteString(string(run))
			} else {
				b.WriteString(s.Render(string(run)))
			}
		}
	}
	return b.String()
}

// crop centres a w×h window on a cols×rows piece. A zero or oversized
// window keeps the full dimension.
func crop(cols, rows, w, h int) (x0, y0, cw, ch int) {
	cw, ch = cols, rows
	if w > 0 && w < cols {
		cw, x0 = w, (cols-w)/2
	}
	if h > 0 && h < rows {
		ch, y0 = h, (rows-h)/2
	}
	return x0, y0, cw, ch
}
