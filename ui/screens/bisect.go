package screens

import (
	"fmt"
	"math"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// BisectMark is one offset read so far.
type BisectMark struct {
	Offset int64
	Bad    bool
}

// Bisect is the bisect-hunt piece: a rail of the partition's offsets with
// the ones read marked good or bad, a bracket under what is left, the log
// of each read below, and a legend.
type Bisect struct {
	Title      string // on the frame: "find · orders p0 · first record at or after 09:00"
	From, To   string // at the rail's ends: "offset 0", "end 12,480"
	Start, End int64
	Lo, Hi     int64
	Marks      []BisectMark
	Testing    int64 // -1 between reads
	Found      int64 // -1 until found
	Done       bool
	Log        []string // styled lines, the newest last
	Good, Bad  string   // the legend's words for good and bad
}

// Render draws the hunt at width × height.
func (b Bisect) Render(k kit.Kit, width, height int) []string {
	w := min(max(width, 48), 88)
	inner := w - 2
	slots := int(min(32, max(1, b.End-b.Start), int64((inner-6)*2/5)))
	// The piece's 2.5 columns a slot, wider when there are few offsets.
	sp := 2.5
	if slots < 32 {
		sp = min(8, float64(inner-6)/float64(max(1, slots-1)))
	}
	slot := func(o int64) int {
		if b.End <= b.Start {
			return 0
		}
		return int(min(int64(slots-1), max(0, (o-b.Start)*int64(slots)/(b.End-b.Start))))
	}
	xOf := func(i int) int { return 2 + int(math.Round(float64(i)*sp)) }

	type cell struct {
		s string
		r kit.Role
	}
	rows := make([][]cell, 4) // marker, rail, bracket, label
	for i := range rows {
		rows[i] = make([]cell, inner)
		for x := range rows[i] {
			rows[i][x] = cell{" ", kit.FG}
		}
	}
	put := func(y, x int, s string, r kit.Role) {
		for _, ch := range s {
			if x >= 0 && x < inner {
				rows[y][x] = cell{string(ch), r}
			}
			x++
		}
	}
	marks := make([]int, slots) // 0 untested, 1 good, -1 bad
	for _, m := range b.Marks {
		if m.Bad {
			marks[slot(m.Offset)] = -1
		} else {
			marks[slot(m.Offset)] = 1
		}
	}
	lo, hi := slot(b.Lo), slot(max(b.Lo, b.Hi-1))
	if b.Hi >= b.End {
		hi = slots - 1
	}
	for i := range slots {
		x := xOf(i)
		in := i >= lo && i <= hi
		if i < slots-1 {
			r := kit.Faint
			if in && i < hi {
				r = kit.Muted
			}
			put(1, x+1, strings.Repeat("─", max(1, xOf(i+1)-x-1)), r)
		}
		ch, r := "○", kit.Faint
		if in {
			r = kit.Muted
		}
		switch marks[i] {
		case 1:
			ch, r = "●", kit.OK
		case -1:
			ch, r = "●", kit.Err
		}
		put(1, x, ch, r)
	}
	switch {
	case b.Done && b.Found >= 0:
		x := xOf(slot(b.Found))
		put(0, x, "▼", kit.Err)
		put(1, x, "◆", kit.Err)
	case b.Testing >= 0:
		x := xOf(slot(b.Testing))
		put(0, x, "▼", kit.Notice)
		put(1, x, "◆", kit.Notice)
	}
	bx0, bx1 := xOf(lo), xOf(hi)
	if bx1-bx0 < 1 {
		bx0 = bx1 - 1 // found: a bracket still shows under the one slot left
	}
	put(2, bx0, strings.Repeat("─", max(1, bx1-bx0+1)), kit.Accent)
	put(2, bx0, "└", kit.Accent)
	put(2, bx1, "┘", kit.Accent)
	label := "found"
	switch {
	case b.Done && b.Found < 0:
		label = "none"
	case !b.Done:
		left := max(0, b.Hi-b.Lo)
		label = fmt.Sprintf("%s left · ~%d steps", Num(float64(left)), int(math.Ceil(math.Log2(float64(max(2, left+1))))))
	}
	put(3, max(0, min(inner-len([]rune(label)), (bx0+bx1)/2-len([]rune(label))/2)), label, kit.Accent)

	body := []string{"", spread(" "+k.S(kit.OK, b.From), k.S(kit.Err, b.To)+" ", inner)}
	for _, row := range rows {
		var sb strings.Builder
		for _, c := range row {
			if c.s == " " {
				sb.WriteString(" ")
				continue
			}
			sb.WriteString(k.S(c.r, c.s))
		}
		body = append(body, sb.String())
	}
	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	vis := max(3, height-2-len(body)-2)
	log := b.Log
	if len(log) > vis {
		log = log[len(log)-vis:]
	}
	for _, l := range log {
		body = append(body, " "+l)
	}
	for len(body) < height-4 {
		body = append(body, "")
	}
	good, bad := b.Good, b.Bad
	if good == "" {
		good, bad = "good", "bad"
	}
	body = append(body, "", " "+k.S(kit.OK, "● "+good)+"  "+k.S(kit.Err, "● "+bad)+"  "+k.S(kit.Notice, "◆ reading")+"  "+k.S(kit.Muted, "○ unread"))
	return kit.Clip(width, k.Draw(kit.Box{Title: k.S(kit.Muted, b.Title), Role: kit.Rule, Width: w}, body))
}
