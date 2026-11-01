package screens

import (
	"fmt"
	"math"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// SniffRow is one request in the capture list.
type SniffRow struct {
	N      int64
	Time   float64 // seconds since the capture began
	Client string
	Broker string // "broker 1"
	API    string
	Key    int16
	Len    int // the request's bytes on the wire
	Info   string
}

// SniffMix is one API's share of the recent requests.
type SniffMix struct {
	API   string
	Key   int16
	Count int
}

// Sniffer is the packet-sniffer piece: a capture list with each request
// coloured by its API, the selected one's path and the hex of its first
// bytes, the mix of recent requests, and a status line.
type Sniffer struct {
	Capture string // "proxy on localhost:19092 → localhost:29092"
	Filter  string
	Total   int64
	Dropped int64
	Rows    []SniffRow // oldest first; the screen shows the newest that fit
	Sel     int        // index into Rows; the newest when out of range

	Detail string // "Request 42 v9: 312 B → 1.2 KiB in 3.1ms"
	Path   string // "127.0.0.1:53422 (rdkafka) > broker 1 > "
	Head   []byte // the selected request's first bytes, size prefix included

	Mix     []SniffMix // the biggest first
	MixOf   int        // how many recent requests Mix counts
	Status  string     // "capturing"; "" when stopped
	Rate    string     // "1.2 kB/s"
	Keys    string
	Waiting string // shown in the list while it is empty
}

// APIRole is the colour a request is drawn in, by API key: produce
// orange, fetch blue, metadata yellow, offsets cyan, groups purple,
// transactions green, cluster changes red, handshakes grey.
func APIRole(key int16) kit.Role {
	switch key {
	case 0:
		return kit.Warn
	case 1:
		return kit.Accent
	case 3:
		return kit.Notice
	case 2:
		return kit.Info
	case 8, 9, 10, 11, 12, 13, 14, 15, 16, 42, 47, 68, 69:
		return kit.Accent2
	case 22, 24, 25, 26, 28, 61, 65, 66:
		return kit.OK
	case 19, 20, 21, 30, 31, 33, 37, 43, 44, 45, 51, 57:
		return kit.Err
	case 17, 18, 36:
		return kit.Muted
	}
	return kit.FG
}

// Render draws the capture at width × height.
func (s Sniffer) Render(k kit.Kit, width, height int) []string {
	w := max(width, 60)
	h := max(height, 20)
	type cell struct {
		s string
		r kit.Role
	}
	g := make([][]cell, h)
	for y := range g {
		g[y] = make([]cell, w)
		for x := range g[y] {
			g[y][x] = cell{" ", kit.FG}
		}
	}
	put := func(x, y int, str string, r kit.Role) int {
		for _, ch := range str {
			if y >= 0 && y < h && x >= 0 && x < w {
				g[y][x] = cell{string(ch), r}
			}
			x++
		}
		return x
	}
	hline := func(y int) { put(0, y, strings.Repeat("─", w), kit.Faint) }
	fit := func(str string, n int) string {
		if n <= 0 {
			return ""
		}
		if kit.Width(str) > n {
			rs := []rune(str)
			return string(rs[:max(0, n-1)]) + "…"
		}
		return str
	}

	right := fmt.Sprintf("%s · %s dropped", plural(int(s.Total), "request"), Num(float64(s.Dropped)))
	left := w - kit.Width(right) - 2
	x := put(0, 0, fit(s.Capture+" · filter: ", left), kit.Muted)
	put(x, 0, fit(s.Filter, left-x), kit.Notice)
	put(w-kit.Width(right), 0, right, kit.Muted)

	// Columns; the client's goes first when the screen is narrow.
	wide := w >= 96
	cNo, cTime, cClient, cBroker, cAPI, cLen, cInfo := 1, 7, 18, 34, 44, 60, 69
	if !wide {
		cBroker, cAPI, cLen, cInfo = 18, 28, 44, 53
	}
	heads := [][2]any{{cNo + 2, "No."}, {cTime + 5, "Time"}, {cBroker, "Broker"}, {cAPI, "API"}, {cLen + 3, "Len"}, {cInfo, "Info"}}
	if wide {
		heads = append(heads, [2]any{cClient, "Client"})
	}
	for _, hd := range heads {
		put(hd[0].(int), 1, hd[1].(string), kit.FG)
	}
	hline(2)

	list := h - 12
	sel := s.Sel
	if sel < 0 || sel >= len(s.Rows) {
		sel = len(s.Rows) - 1
	}
	first := max(0, len(s.Rows)-list)
	if sel >= 0 && sel < first {
		first = sel
	}
	if len(s.Rows) == 0 && s.Waiting != "" {
		put(cTime, 3, s.Waiting, kit.Muted)
	}
	for i := range list {
		n := first + i
		if n >= len(s.Rows) {
			break
		}
		r, y := s.Rows[n], 3+i
		isSel := n == sel
		hi := func(r kit.Role) kit.Role {
			if isSel {
				return kit.FG
			}
			return r
		}
		if isSel {
			put(0, y, "▶", kit.FG)
		}
		role := APIRole(r.Key)
		put(cNo, y, lpad(fmt.Sprint(r.N), 5), hi(kit.Rule))
		put(cTime, y, lpad(fmt.Sprintf("%.6f", r.Time), 9), kit.Rule)
		if wide {
			put(cClient, y, fit(r.Client, cBroker-cClient-1), hi(kit.Muted))
		}
		put(cBroker, y, fit(r.Broker, cAPI-cBroker-1), hi(kit.Muted))
		put(cAPI, y, fit(r.API, cLen-cAPI), role)
		put(cLen, y, lpad(Bytes(int64(r.Len)), 7), kit.Rule)
		info := r.Info
		if info == "" {
			info = "·"
		}
		put(cInfo, y, fit(info, w-cInfo), hi(role))
	}

	// The selected request.
	dy := 3 + list
	hline(dy)
	if sel >= 0 {
		r := s.Rows[sel]
		x := put(0, dy+1, fit(s.Detail, w), kit.FG)
		path := s.Path
		if kit.Width(path) > w-x-3-len(r.API) {
			path = "> " // no room for where it came from
		}
		x = put(x+3, dy+1, path, kit.Muted)
		put(x, dy+1, r.API, APIRole(r.Key))
		for row := range 4 {
			y := dy + 2 + row
			if row*8 >= len(s.Head) {
				break
			}
			put(0, y, fmt.Sprintf("%04x", row*8), kit.Rule)
			asc := ""
			for i := range 8 {
				at := row*8 + i
				if at >= len(s.Head) {
					break
				}
				b := s.Head[at]
				role := kit.Muted
				if at == 4 || at == 5 { // the API key
					role = APIRole(r.Key)
				}
				gap := 0
				if i >= 4 {
					gap = 1
				}
				put(6+i*3+gap, y, fmt.Sprintf("%02x", b), role)
				if b >= 33 && b < 127 {
					asc += string(rune(b))
				} else {
					asc += "."
				}
			}
			put(32, y, asc, kit.Rule)
		}
	}

	// The mix of recent requests.
	sx, cols := 46, 2
	if w < 100 {
		cols = 1
	}
	if len(s.Mix) > 0 && w >= sx+25 {
		put(sx, dy+2, fmt.Sprintf("by API (last %s)", Num(float64(s.MixOf))), kit.Muted)
		top := s.Mix[0].Count
		for i, m := range s.Mix {
			if i >= 3*cols {
				break
			}
			x, y := sx+(i%cols)*27, dy+3+i/cols
			if cols == 1 {
				x, y = sx, dy+3+i
			}
			const bw = 8
			n := int(math.Round(float64(m.Count) / float64(max(1, top)) * bw))
			if m.Count > 0 {
				n = max(n, 1) // a request seen is never an empty bar
			}
			role := APIRole(m.Key)
			put(x, y, fit(m.API, 12), role)
			put(x+13, y, strings.Repeat("█", n), role)
			put(x+13+n, y, strings.Repeat("░", bw-n), kit.Faint)
			put(x+13+bw+1, y, lpad(Num(float64(m.Count)), 4), kit.Muted)
		}
	}

	hline(h - 2)
	if s.Status != "" {
		put(0, h-1, "● "+s.Status, kit.Err)
	} else {
		put(0, h-1, "■ stopped", kit.Muted)
	}
	put(13, h-1, s.Rate, kit.Muted)
	put(w-kit.Width(s.Keys), h-1, s.Keys, kit.Rule)

	out := make([]string, h)
	for y, row := range g {
		var sb strings.Builder
		for i := 0; i < len(row); {
			j := i
			var run strings.Builder
			for j < len(row) && row[j].r == row[i].r {
				run.WriteString(row[j].s)
				j++
			}
			sb.WriteString(k.S(row[i].r, run.String()))
			i = j
		}
		out[y] = strings.TrimRight(sb.String(), " ")
	}
	return kit.Clip(width, out)
}
