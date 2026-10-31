package asciipack

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
)

func TestEveryPackLoads(t *testing.T) {
	slugs := List()
	if len(slugs) == 0 {
		t.Fatal("no packs embedded")
	}
	for _, s := range slugs {
		p, err := Load(s)
		if err != nil {
			t.Fatal(err)
		}
		for i, f := range p.Frames {
			lines := strings.Split(f.T, "\n")
			if len(lines) != p.Rows {
				t.Fatalf("%s frame %d: %d rows, want %d", s, i, len(lines), p.Rows)
			}
			if f.C != "" && len(strings.Split(f.C, "\n")) != p.Rows {
				t.Fatalf("%s frame %d: colour rows mismatch", s, i)
			}
		}
	}
}

func TestFrameAtLoops(t *testing.T) {
	p, err := Load("page-404")
	if err != nil {
		t.Fatal(err)
	}
	if p.FrameAt(0) != p.Frames[0] {
		t.Error("t=0 should be the first frame")
	}
	if p.FrameAt(p.Duration()) != p.Frames[0] {
		t.Error("one full loop should wrap to the first frame")
	}
	last := p.Duration() - time.Second/time.Duration(p.FPS)
	if p.FrameAt(last) != p.Frames[len(p.Frames)-1] {
		t.Error("just before the loop ends should be the last frame")
	}
}

func TestRenderCropsAndPlain(t *testing.T) {
	p, err := Load("page-404")
	if err != nil {
		t.Fatal(err)
	}
	f := p.FrameAt(0)

	full := p.Render(f, RenderOptions{Plain: true})
	if got := strings.Count(full, "\n") + 1; got != p.Rows {
		t.Errorf("rows = %d, want %d", got, p.Rows)
	}
	if strings.Contains(full, "\x1b[") {
		t.Error("plain render contains escape codes")
	}

	cropped := p.Render(f, RenderOptions{Width: 40, Height: 10})
	lines := strings.Split(cropped, "\n")
	if len(lines) != 10 {
		t.Fatalf("cropped rows = %d, want 10", len(lines))
	}
	for i, l := range lines {
		if w := ansi.StringWidth(l); w != 40 {
			t.Fatalf("cropped row %d width = %d, want 40", i, w)
		}
	}
	if !strings.Contains(cropped, "\x1b[") {
		t.Error("coloured render has no colour")
	}
}

func TestRenderRecolor(t *testing.T) {
	p, err := Load("page-404")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	p.Render(p.FrameAt(0), RenderOptions{Recolor: func(h string) string { seen[h] = true; return "#ff9e2c" }})
	if len(seen) == 0 {
		t.Error("recolor never called")
	}
}

func TestUnknownPiece(t *testing.T) {
	if _, err := Load("nope"); err == nil || !strings.Contains(err.Error(), "page-404") {
		t.Errorf("err = %v, want it to list available pieces", err)
	}
}
