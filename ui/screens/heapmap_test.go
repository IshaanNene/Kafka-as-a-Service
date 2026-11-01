package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func heapFixture() HeapMap {
	var cells []HeapCell
	for i := range 960 {
		c := HeapCell{Density: 1}
		switch {
		case i < 300: // compacted
			c.Kind, c.Density = 1, 0.2+float64(i%7)/10
		case i >= 410 && i < 440:
			c.Hole = true
		case i%37 == 0:
			c.Kind = 5
		case i%37 < 6:
			c.Kind = 2 + (i/37)%3
		}
		cells = append(cells, c)
	}
	return HeapMap{
		Title: "log · orders p0 · leader 1",
		Range: "offsets 0 – 48,000 · 50 a cell",
		Cells: cells,
		Stats: []HeapStat{
			{"offsets", "48,000", kit.FG},
			{"records", "39,120", kit.Accent},
			{"on disk", "4.1 MiB", kit.FG},
			{"holes", "1 (1,500)", kit.Warn},
			{"compacted", "18%", kit.Notice},
		},
		Bar:    0.18,
		Reads:  []HeapLine{{"fetch @0      1.0 MiB  212", kit.OK}, {"fetch @9,880  1.0 MiB  208", kit.OK}},
		Legend: "· hole   █ records   ▒ compacted   ▌ run end",
		Foot:   "cells are offset ranges; Kafka doesn't expose segment files",
	}
}

func TestHeapMapGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "heapmap", heapFixture().Render(k, 84, 28))
	golden(t, "heapmap_narrow", heapFixture().Render(k, 70, 24))
	out := kit.Join(heapFixture().Render(k, 84, 28))
	for _, want := range []string{"LOG STATS", "READS", "·", "▌", "committed", "segment files"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q:\n%s", want, out)
		}
	}
	if glyph(HeapCell{Density: 1}) != "█" || glyph(HeapCell{Density: 0.1}) != "░" {
		t.Error("glyphs")
	}
}
