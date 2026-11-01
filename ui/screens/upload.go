package screens

import (
	"fmt"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// Upload file states.
const (
	Queued = iota
	Sending
	Sent
	Failed
)

// UploadFile is one file in the upload list.
type UploadFile struct {
	Name    string
	Size    int64 // bytes
	Read    int64 // bytes read and sent
	Records int64
	Rate    float64 // records/s while sending
	State   int
	Err     string
}

// Upload is the upload-dropzone piece after the drop: a frame titled with
// the destination, the zone folded to one line, and a row per file with
// its colour, size, rate or state, and a progress bar.
type Upload struct {
	Title    string // "Upload to orders"
	Right    string // on the frame: "cluster 5L6g3nSh"
	Zone     string // the folded zone: "5 files · lines become records"
	Files    []UploadFile
	Foot     string // "2 of 5 sent · 12 of 170 MiB · 40,000 records"
	Action   string // at the right of the foot: "ctrl-c to stop"
	Hint     string
	Complete bool // every file sent: the foot goes green
}

// fileColors are the piece's file squares, in its order.
var fileColors = []kit.Role{kit.Accent2, kit.Err, kit.Accent, kit.OK, kit.Warn}

// Render draws the upload at width × height.
func (u Upload) Render(k kit.Kit, width, height int) []string {
	w := min(max(width, 44), 90)
	inner := w - 2
	body := []string{""}
	// The zone, folded once files are in, dashed as the piece draws it.
	zw := inner - 2
	dash := func(n int) string {
		var b strings.Builder
		for i := range n {
			b.WriteString(map[bool]string{true: "╌", false: "─"}[i%2 == 1])
		}
		return b.String()
	}
	zone := kit.Fit("↑ "+u.Zone, zw-4)
	body = append(body,
		" "+k.S(kit.Faint, "╭"+dash(zw-2)+"╮"),
		" "+k.S(kit.Faint, "╎")+" "+k.S(kit.Muted, zone)+" "+k.S(kit.Faint, "╎"),
		" "+k.S(kit.Faint, "╰"+dash(zw-2)+"╯"),
		"")

	room := max(3, height-2-len(body)-3)
	if u.Hint != "" {
		room--
	}
	shown, more := u.visible(room / 3)
	for _, i := range shown {
		body = append(body, u.row(k, i, inner)...)
	}
	if more != "" {
		body = append(body, " "+k.S(kit.Muted, more))
	}
	for len(body) < height-4-boolInt(u.Hint != "") {
		body = append(body, "")
	}
	footRole := kit.Muted
	if u.Complete {
		footRole = kit.OK
	}
	actRole := kit.Err
	if u.Complete {
		actRole = kit.OK
	}
	if kit.Width(u.Foot)+kit.Width(u.Action)+2 <= inner-2 {
		body = append(body, " "+spread(k.S(footRole, u.Foot), k.S(actRole, u.Action), inner-2))
	} else {
		// Too narrow for both: the foot, then the action under it at the right.
		body = append(body[:len(body)-1], " "+k.S(footRole, cut(u.Foot, inner-2)), " "+lpad(k.S(actRole, u.Action), inner-2))
	}
	out := k.Draw(kit.Box{Title: k.S(kit.FG, u.Title), Right: k.S(kit.Muted, u.Right), Role: kit.Rule, Width: w}, body)
	if u.Hint != "" {
		for _, l := range wrapParts(u.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}

// cut shortens s to w with an ellipsis; unlike kit.Fit it never pads, so
// the result can still be centred.
func cut(s string, w int) string {
	if kit.Width(s) > w {
		return kit.Fit(s, w)
	}
	return s
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// visible is which files fit in n rows: failures and those sending first,
// then queued, then sent, kept in their own order, and what was left out.
func (u Upload) visible(n int) ([]int, string) {
	if len(u.Files) <= n {
		idx := make([]int, len(u.Files))
		for i := range idx {
			idx[i] = i
		}
		return idx, ""
	}
	n = max(1, n-1) // a row for "… N more"
	keep := map[int]bool{}
	for _, state := range []int{Failed, Sending, Queued, Sent} {
		for i, f := range u.Files {
			if len(keep) < n && f.State == state {
				keep[i] = true
			}
		}
	}
	var idx []int
	left := map[int]int{}
	for i, f := range u.Files {
		if keep[i] {
			idx = append(idx, i)
		} else {
			left[f.State]++
		}
	}
	var parts []string
	for _, s := range []struct {
		state int
		word  string
	}{{Sent, "sent"}, {Sending, "sending"}, {Queued, "queued"}} {
		if left[s.state] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", left[s.state], s.word))
		}
	}
	return idx, fmt.Sprintf("… %d more: %s", len(u.Files)-len(idx), strings.Join(parts, ", "))
}

// row is the piece's file: ■ name, size so far, rate or state, then the
// bar and its percent.
func (u Upload) row(k kit.Kit, i, inner int) []string {
	f := u.Files[i]
	frac := 0.0
	if f.Size > 0 {
		frac = min(1, float64(f.Read)/float64(f.Size))
	}
	if f.State == Sent {
		frac = 1
	}
	size := Bytes(f.Size)
	if f.State == Sending {
		size = Bytes(f.Read) + " / " + size
	}
	var state string
	switch f.State {
	case Sent:
		state = k.S(kit.OK, "✓ "+Num(float64(f.Records))+" records")
	case Failed:
		state = k.S(kit.Err, "✗ failed")
	case Sending:
		state = k.S(kit.Rule, Num(f.Rate)+" rec/s")
	default:
		state = k.S(kit.Rule, "Queued")
	}
	nameW := max(8, inner-kit.Width(size)-kit.Width(state)-10)
	if f.State == Sending && nameW < min(16, kit.Width(f.Name)) {
		// Tight: the bar already shows how far; keep the name.
		size = Bytes(f.Read)
		nameW = max(8, inner-kit.Width(size)-kit.Width(state)-10)
	}
	head := " " + k.S(fileColors[i%len(fileColors)], "■") + " " + k.S(kit.FG, kit.Fit(f.Name, min(nameW, 30))) + "  " + k.S(kit.Muted, size)
	out := []string{spread(head, state+" ", inner)}

	barW := inner - 9
	n := int(frac * float64(barW))
	fill := kit.Accent
	pctRole := kit.FG
	switch f.State {
	case Sent:
		fill, pctRole = kit.OK, kit.OK
	case Failed:
		fill, pctRole = kit.Err, kit.Err
	}
	bar := "   " + k.S(fill, strings.Repeat("━", n)) + k.S(kit.Faint, strings.Repeat("─", barW-n)) + k.S(pctRole, fmt.Sprintf("%4d%%", int(frac*100)))
	out = append(out, bar)
	if f.State == Failed && f.Err != "" {
		out[1] = "   " + k.S(kit.Err, kit.Fit(f.Err, inner-4))
	}
	return append(out, "")
}

// Tray is the empty-upload-tray piece: the tray with its count, a tick
// popped above it, a full bar, and what was uploaded.
type Tray struct {
	Count  string // " 5 files "
	Title  string // "Upload complete"
	Detail string // "orders.jsonl and 4 more · 24.6 MiB"
	Action string // "kaas consume orders →"
	Failed bool   // some files failed: a cross instead of the tick, the bar red
	Hint   string
}

var (
	tick  = []string{"      ▄█", "█▄  ▄█▀ ", " ▀██▀   "}
	cross = []string{"▀▄  ▄▀", "  ██  ", "▄▀  ▀▄"}
)

// The piece's tray blues, light to dark; no theme role has them.
const (
	trayMid  = "#1f6feb"
	trayDark = "#0d419d"
)

// Render draws the card, at most the piece's 56 columns.
func (t Tray) Render(k kit.Kit, width int) []string {
	w := min(max(width, 36), 56)
	inner := w - 2
	center := func(s string) string { return strings.Repeat(" ", max(0, (inner-kit.Width(s))/2)) + s }
	mark, role := tick, kit.OK
	if t.Failed {
		mark, role = cross, kit.Err
	}
	body := []string{""}
	for _, l := range mark {
		body = append(body, center(k.S(role, l)))
	}
	body = append(body, "", "")
	tw := 22
	label := cut(t.Count, tw-6)
	// Each part styled on its own: the label's reset would end an outer style.
	front := k.Hex(trayMid, strings.Repeat("█", 3)) + k.S(kit.FG, label) + k.Hex(trayMid, strings.Repeat("█", tw-6-kit.Width(label)+3))
	body = append(body,
		center(k.Hex(trayMid, "▗"+strings.Repeat(" ", tw-2)+"▖")),
		center(front),
		center(k.Hex(trayDark, strings.Repeat("█", tw))),
		center(k.Hex(trayDark, strings.Repeat("▀", tw))),
		"")
	barW := min(30, inner-8)
	fill := kit.OK
	if t.Failed {
		fill = kit.Err
	}
	body = append(body, center(k.S(fill, strings.Repeat("█", barW))+k.S(kit.Muted, " 100%")), "",
		center(k.S(kit.FG, cut(t.Title, inner-2))))
	for _, l := range wrapParts(t.Detail, inner-4) {
		body = append(body, center(k.S(kit.Muted, l)))
	}
	body = append(body, "", center(k.S(kit.Accent, cut(t.Action, inner-2))), "")
	out := k.Draw(kit.Box{Role: kit.Faint, Width: w}, body)
	if t.Hint != "" {
		for _, l := range wrapParts(t.Hint, w-2) {
			out = append(out, " "+k.S(kit.Rule, l))
		}
	}
	return kit.Clip(width, out)
}
