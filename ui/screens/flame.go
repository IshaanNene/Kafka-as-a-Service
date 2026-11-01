package screens

import (
	"math"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// The piece's flame colours, coolest first; no theme role has them.
var flameHeat = []string{"#b62324", "#d1462b", "#e2662f", "#ec8a3a", "#f2a649", "#f5c35c", "#f8dc76"}

// FlameNode is one frame and the frames above it.
type FlameNode struct {
	Name string
	Time float64 // this frame and everything above it
	Self float64
	Kids []FlameNode // drawn left to right in this order
}

// Flame is the flame-graph piece: frames stacked from the bottom, each as
// wide as its share of its parent, coloured by how hot it runs, with one
// selected and described in the footer.
type Flame struct {
	Title string // "latency profile · proxy on localhost:19092 · every client"
	Right string // "requests 48,213"
	Root  FlameNode
	Sel   []int // kid indexes from Root to the selected frame; empty for Root

	Name  string // the selected frame's: "wait"
	Share string // "41.2% of time"
	Stats string // "1,204 requests · avg 3.1ms · p99 12ms"
	Path  string // "Produce → broker 1 → orders"
	Empty string // shown instead of frames while nothing is counted
	Hint  string
}

// heatHex is a frame's colour: hotter the more of its parent's time it
// takes, and hottest where the time is its own.
func heatHex(share float64, self bool) string {
	h := share * 0.75
	if self {
		h += 0.25
	}
	return flameHeat[min(len(flameHeat)-1, max(0, int(h*float64(len(flameHeat)))))]
}

// Render draws the graph at width × height.
func (f Flame) Render(k kit.Kit, width, height int) []string {
	w, h := max(width, 60), max(height, 20)
	c := newCanvas(w, h)
	c.put(2, 0, clipTo(f.Title, w-4-len(f.Right)-2), kit.FG)
	c.put(w-2-kit.Width(f.Right), 0, f.Right, kit.Accent)
	c.hline(0, w-1, 1, kit.Faint)

	gw, base := w-4, h-7
	type placed struct{ x0, x1, y int }
	var sel placed
	found := false
	var place func(n FlameNode, depth int, x, wd, share float64, path []int)
	place = func(n FlameNode, depth int, x, wd, share float64, path []int) {
		y := base - depth*2
		x0, x1 := 2+int(math.Round(x)), 2+int(math.Round(x+wd))-1
		if x1 < x0 || y < 3 {
			return
		}
		isSel := len(path) == len(f.Sel)
		for i := range path {
			isSel = isSel && path[i] == f.Sel[i]
		}
		hex := heatHex(share, n.Self > 0)
		paint := func(xx, yy int, s string) {
			if isSel {
				c.put(xx, yy, s, kit.FG)
			} else {
				c.putHex(xx, yy, s, hex)
			}
		}
		for xx := x0; xx <= x1; xx++ {
			if xx == x1 && x1 > x0 {
				paint(xx, y, "▌")
			} else {
				paint(xx, y, "█")
				paint(xx, y-1, "▄")
			}
		}
		if x1-x0 >= 4 {
			role := kit.FG
			if isSel {
				role = kit.Accent
			}
			c.put(x0+1, y, clipTo(n.Name, x1-x0-1), role)
		}
		if isSel {
			sel, found = placed{x0, x1, y}, true
		}
		cx := x
		for i, kid := range n.Kids {
			kw := 0.0
			if n.Time > 0 {
				kw = kid.Time / n.Time * wd
			}
			ks := 0.0
			if n.Time > 0 {
				ks = kid.Time / n.Time
			}
			place(kid, depth+1, cx, kw, ks, append(append([]int(nil), path...), i))
			cx += kw
		}
	}
	if f.Root.Time > 0 {
		place(f.Root, 0, 0, float64(gw), 1, nil)
	} else if f.Empty != "" {
		c.put(2, base, clipTo(f.Empty, w-4), kit.Muted)
	}
	if found && sel.y-2 >= 2 {
		c.put(sel.x0, sel.y-2, "▼", kit.FG)
	}

	c.hline(0, w-1, h-6, kit.Faint)
	if f.Name != "" {
		x := c.put(2, h-5, clipTo("▶ "+f.Name, 23), kit.FG)
		x = max(x+1, 26)
		x = c.putHex(x, h-5, f.Share, flameHeat[6])
		c.putHex(max(x+2, 46), h-5, clipTo(f.Stats, w-max(x+2, 46)-2), flameHeat[4])
		c.put(2, h-4, clipTo(f.Path, w-4), kit.Muted)
	}
	c.put(2, h-2, clipTo(f.Hint, w-4), kit.Rule)
	return kit.Clip(width, c.lines(k))
}
