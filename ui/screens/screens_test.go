package screens

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

var update = flag.Bool("update", false, "rewrite golden files")

func kits(t *testing.T) (plain, color kit.Kit) {
	th, err := theme.Get("amber")
	if err != nil {
		t.Fatal(err)
	}
	return kit.New(th, false), kit.New(th, true)
}

func golden(t *testing.T, name string, b []string) {
	t.Helper()
	got := kit.Join(b)
	p := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("%v (run go test ./ui/screens -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs from golden; rerun with -update if intended\n--- got ---\n%s", name, got)
	}
}

var help = Help{
	Path: "kaas watch", Version: "v0.2.0", Usage: "kaas watch [flags]",
	Short: "Stream cluster events: leader moves, ISR changes, topics created and deleted",
	Flags: []Item{
		{Name: "-t, --topic strings", Desc: "topics to watch closely (repeatable)"},
		{Name: "-o, --output string", Desc: "output format: text|json", Hint: "text"},
		{Name: "    --budget float", Desc: "requests per second", Hint: "20"},
	},
	Global:   []Item{{Name: "-b, --brokers string", Desc: "comma-separated seed brokers", Hint: "localhost:9092"}},
	Examples: []Example{{Comment: "Follow two topics", Cmd: "kaas watch -t orders -t payments"}, {Cmd: "kaas watch -o json | jq .event"}},
	Prompt:   "dev@box: ~/src",
}

func refused() probe.Result {
	return probe.Result{
		Seeds: []string{"localhost:9092", "kafka-1:9092"},
		Took:  3 * time.Millisecond,
		Steps: []probe.Step{
			{Seed: "localhost:9092", Stage: probe.DNS, Detail: "localhost → 127.0.0.1", Took: 400 * time.Microsecond},
			{Seed: "localhost:9092", Stage: probe.TCP, Err: errors.New("connect: connection refused"), Took: time.Millisecond},
			{Seed: "kafka-1:9092", Stage: probe.DNS, Err: errors.New("no such host"), Took: time.Millisecond},
		},
	}
}

func TestGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "help", help.Render(k, 80))
	golden(t, "unreachable", Unreachable(refused()).Render(k, 100))
	golden(t, "unreachable_narrow", Unreachable(refused()).Render(k, 60))
	golden(t, "not_found", NotFound{Kind: "command", Name: "brokrs", Where: "kaas",
		Suggestions: []Item{{Name: "brokers", Desc: "List brokers"}}, Actions: []string{"kaas help"}}.Render(k, 80))
	golden(t, "invalid", Invalid{Title: "kaas snapshot", Action: "kaas snapshot --help", Fields: []Field{
		{Label: "--brokers", Value: "localhost:9092", State: kit.Good},
		{Label: "--output", Value: "yaml", State: kit.Degraded, Focus: true, Msg: `must be one of table, json`},
	}}.Render(k, 80))
	var boot []string
	r := refused()
	boot = append(boot, BootStart(k, r.Seeds)...)
	for _, s := range r.Steps {
		boot = append(boot, BootStep(k, s)...)
	}
	golden(t, "boot", append(boot, BootDone(k, r)...))
	golden(t, "bench", benchFixture().Render(k, 100))
	golden(t, "bench_narrow", benchFixture().Render(k, 64))
	golden(t, "bench_phone", benchFixture().Render(k, 45))
	golden(t, "watch", watchFixture().Render(k, 100, 30))
	golden(t, "table", tableFixture().Render(k, 100))
	golden(t, "table_narrow", tableFixture().Render(k, 44))
	golden(t, "tree", treeFixture().Render(k, 80))
	golden(t, "partitions", partitionsFixture().Render(k, 90))
	golden(t, "partitions_narrow", partitionsFixture().Render(k, 50))
	golden(t, "partitions_alter", alterFixture().Render(k, 90))
	golden(t, "groups", groupsFixture().Render(k, 100))
	golden(t, "groups_narrow", groupsFixture().Render(k, 50))
	golden(t, "proctree", procFixture().Render(k, 100))
	golden(t, "proctree_narrow", procFixture().Render(k, 44))
	golden(t, "watch_short", watchFixture().Render(k, 80, 16))
	golden(t, "watch_phone", watchFixture().Render(k, 45, 30))
}

// TestFits renders every screen, coloured, at a range of widths and checks
// none spills past the terminal.
func tableFixture() Table {
	return Table{
		Title: "Topics", Badge: "cluster 5L6g3nSh · 3 topics",
		Columns: []Column{{Name: "Topic"}, {Name: "Parts", Right: true}, {Name: "RF", Right: true, Drop: 2},
			{Name: "URP", Right: true, Drop: 1}, {Name: "Offline", Right: true, Drop: 3}, {Name: "Records", Right: true, Drop: 4}, {Name: "Status"}},
		Rows: [][]Cell{
			{{Text: "payments"}, {Text: "6"}, {Text: "3"}, {Text: "2", Role: kit.Warn}, {Text: "1", Role: kit.Err}, {Text: "120,431"}, {Text: "● offline", Role: kit.Err}},
			{{Text: "orders"}, {Text: "12"}, {Text: "3"}, {Text: "1", Role: kit.Warn}, {Text: "0"}, {Text: "2,301,877"}, {Text: "● under-replicated", Role: kit.Warn}},
			{{Text: "audit"}, {Text: "4"}, {Text: "3"}, {Text: "0"}, {Text: "0"}, {Text: "0"}, {Text: "● healthy", Role: kit.OK}},
		},
		Sorted: 6, Foot: "sorted by status, worst first", FootRight: "1 internal topic hidden",
		Tiles: []Tile{{Label: "Brokers", Value: "3 · ctrl 2"}, {Label: "Partitions", Value: "22"},
			{Label: "Under-replicated", Value: "3", Role: kit.Warn}, {Label: "Offline", Value: "1", Role: kit.Err},
			{Label: "Records", Value: "2,422,308"}, {Label: "leaders · 3 brokers", Value: "▇█▅", Role: kit.Accent}},
	}
}

func treeFixture() Tree {
	leaf := func(full string) TreeNode {
		n := TreeNode{Dot: kit.OK, Info: "3p · 1,204"}
		switch full {
		case "payments.eu.refunds":
			n.Dot, n.Mark, n.MarkRole, n.Changed = kit.Warn, "U", kit.Warn, true
		case "users.profiles":
			n.Mark, n.MarkRole = "C", kit.Accent2
		}
		return n
	}
	return Tree{
		Title: "TOPICS", Root: "5L6g3nSh", Badge: "cluster 5L6g3nSh · 6 topics",
		Head: "6 topics in 4 prefixes", HeadRight: "1 unhealthy topic",
		Nodes: Nest([]string{"orders", "orders.dlq", "payments.eu.charges", "payments.eu.refunds", "payments.us.charges", "users.profiles"}, ".", leaf),
		Foot:  "• orders  33,308 records", FootRight: "U under-replicated · C compacted",
	}
}

// partitionsFixture has one partition of each kind: healthy, under-
// replicated with an out-of-sync follower, and offline.
func partitionsFixture() Partitions {
	all := []int32{1, 2, 3}
	return Partitions{
		Topic: "orders",
		Info:  []string{"4 partitions · RF 3 · 251,038 records · 609 MiB on disk", "cleanup delete · retention 7d · cluster 5L6g3nSh"},
		Parts: []Part{
			{ID: 0, Leader: 1, Replicas: all, ISR: all, Start: 0, End: 82_113, Bytes: 70 << 20},
			{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3}, Start: 1_200, End: 91_004, Bytes: 80 << 20},
			{ID: 2, Leader: -1, Replicas: []int32{3, 1, 2}, ISR: nil, Start: -1, End: -1, Bytes: -1},
			{ID: 3, Leader: 1, Replicas: []int32{1, 3, 2}, ISR: []int32{1, 3, 2}, Start: 0, End: 79_121, Bytes: 53 << 20},
		},
		Focus: 2, Brokers: all,
		ReplicaBytes: map[int32]map[int32]int64{
			1: {0: 70 << 20, 1: 30 << 20, 3: 53 << 20},
			2: {0: 70 << 20, 1: 80 << 20, 3: 53 << 20},
			3: {0: 70 << 20, 1: 80 << 20, 3: 53 << 20},
		},
		Details: []Detail{
			{Label: "Leaders", Value: "broker 1: 2 · broker 2: 1 · broker 3: 0", Role: kit.Info},
			{Label: "In sync", Value: "2 of 4 partitions", Role: kit.Warn},
			{Label: "Set on topic", Value: "broker defaults"},
		},
		Actions:    []string{"kaas watch -t orders", "kaas topics", "kaas bench produce -t orders"},
		Status:     "Partition 2 has no leader: its producers and consumers are stuck.",
		StatusRole: kit.Err,
		Hint:       "-o json for scripts · kaas topics for every topic",
	}
}

// alterFixture is kaas topics alter's preview: two partitions to add,
// not yet placed, the first marked.
func alterFixture() Partitions {
	p := partitionsFixture()
	p.Parts[2] = Part{ID: 2, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}, Start: 0, End: 88_800, Bytes: 75 << 20}
	p.Parts = append(p.Parts, Part{ID: 4, Leader: -1, Start: 0, End: 0, Bytes: -1, New: true}, Part{ID: 5, Leader: -1, Start: 0, End: 0, Bytes: -1, New: true})
	p.Focus = 4
	p.Details = []Detail{
		{Label: "Change", Value: "4 → 6 partitions · adds p4, p5 · RF 3, placed by the controller", Role: kit.OK},
		{Label: "Keys", Value: "hash over 6 partitions from now on, so a key's new records can land on a different partition from its old ones", Role: kit.Warn},
	}
	p.Actions, p.Status, p.StatusRole = nil, "Partitions can't be removed once added.", kit.Warn
	return p
}

func TestNest(t *testing.T) {
	nodes := Nest([]string{"b", "a.x", "a.y", "deep.one.two.leaf", "c.d"}, ".", func(string) TreeNode { return TreeNode{} })
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	// Folders first, sorted, then leaves; a chain of single folders merges.
	if got := strings.Join(names, " "); got != "a c deep.one.two b" {
		t.Errorf("top level = %q", got)
	}
	if len(nodes[0].Children) != 2 || nodes[0].Children[1].Name != "y" {
		t.Errorf("a/ children = %+v", nodes[0].Children)
	}
	if flat := Nest([]string{"x.y"}, "", func(string) TreeNode { return TreeNode{} }); len(flat) != 1 || flat[0].Name != "x.y" {
		t.Errorf("no separator should not nest: %+v", flat)
	}
}

func TestTableDropsColumnsBeforeNames(t *testing.T) {
	k, _ := kits(t)
	for _, w := range []int{40, 60, 100} {
		out := kit.Join(tableFixture().Render(k, w))
		for _, want := range []string{"payments", "under-replicated", "healthy", "Status ▼"} {
			if !strings.Contains(out, want) {
				t.Errorf("width %d: %q missing\n%s", w, want, out)
			}
		}
	}
	if head := tableFixture().Render(k, 40)[1]; strings.Contains(head, "Records") || !strings.Contains(head, "Topic") {
		t.Errorf("width 40 should drop the Records column first: %q", head)
	}
	empty := tableFixture()
	empty.Rows = nil
	empty.Empty = "No topics yet."
	if out := kit.Join(empty.Render(k, 80)); !strings.Contains(out, "No topics yet.") {
		t.Errorf("empty table lost its message\n%s", out)
	}
}

func TestFits(t *testing.T) {
	_, k := kits(t)
	screens := map[string]func(w int) []string{
		"help":        func(w int) []string { return help.Render(k, w) },
		"unreachable": func(w int) []string { return Unreachable(refused()).Render(k, w) },
		"bench":       func(w int) []string { return benchFixture().Render(k, w) },
		"watch":       func(w int) []string { return watchFixture().Render(k, w, 30) },
		"table":       func(w int) []string { return tableFixture().Render(k, w) },
		"tree":        func(w int) []string { return treeFixture().Render(k, w) },
		"partitions":  func(w int) []string { return partitionsFixture().Render(k, w) },
		"groups":      func(w int) []string { return groupsFixture().Render(k, w) },
		"proctree":    func(w int) []string { return procFixture().Render(k, w) },
		"not_found": func(w int) []string {
			return NotFound{Kind: "topic", Name: strings.Repeat("x", 90), Where: "cluster", Actions: []string{"kaas snapshot"}}.Render(k, w)
		},
		"invalid": func(w int) []string {
			return Invalid{Title: "kaas", Action: "kaas --help", Fields: []Field{{Label: "--x", Value: strings.Repeat("v", 90), State: kit.Degraded, Msg: "bad"}}}.Render(k, w)
		},
	}
	for name, render := range screens {
		for _, w := range []int{40, 60, 80, 120, 200} {
			for i, l := range render(w) {
				if n := kit.Width(l); n > w {
					t.Errorf("%s at %d: line %d is %d wide", name, w, i, n)
					break
				}
			}
		}
	}
}

func TestUnreachableSkipsStagesNeverReached(t *testing.T) {
	f := Unreachable(refused())
	got := map[string]string{}
	for _, p := range f.Pills {
		got[p.Label] = p.State
	}
	if got["dns"] != "1/2" || got["tcp"] != "down" || got["api"] != "skipped" || got["metadata"] != "skipped" {
		t.Errorf("pills = %v", got)
	}
}

func benchFixture() Bench {
	ms := time.Millisecond
	rates := make([]float64, 40)
	for i := range rates {
		rates[i] = 480 + float64(i%5)*6
	}
	rates[0] = 150
	return Bench{
		Title: "bench produce · orders", Right: "localhost:19092 · kafka v4.2",
		Rows: []BenchRow{
			{Name: "100B acks=all none", State: BenchDone, RecsPerSec: 499.5, MBPerSec: 0.05, P50: 5 * ms, P99: 121 * ms, Max: 274 * ms},
			{Name: "100B acks=all lz4", State: BenchDone, RecsPerSec: 312.4, MBPerSec: 0.03, P50: 4200 * time.Microsecond, P99: 38 * ms, Max: 61 * ms},
			{Name: "1KB acks=all none", State: BenchRunning, Acked: 2312, Total: 5000},
			{Name: "1KB acks=all lz4", State: BenchPending},
		},
		Spin:      6,
		Hist:      []int64{0, 10, 2600, 1900, 300, 120, 40, 20, 10, 0},
		Buckets:   []time.Duration{ms, 2 * ms, 5 * ms, 10 * ms, 20 * ms, 50 * ms, 100 * ms, 200 * ms, 500 * ms},
		HistTitle: "latency · 1KB acks=all none", Rate: rates, RateTitle: "throughput · rec/s",
		Foot: "running 3/4 · 5,000 records · 500 rec/s cap",
	}
}

func watchFixture() Watch {
	rates := []float64{0, 0, 152, 503, 498, 501, 498, 503, 498, 500, 501, 501, 345, 0}
	return Watch{
		Title: "watch · cluster 5L6g3nShT", Right: "gen 326 · 1 broker · 2 topics",
		Series: []Series{
			{Name: "orders", Rates: rates, Retained: 5000, Known: true},
			{Name: "payments", Known: false},
		},
		Foot: "budget 38.2 · 1,204 granted · 0 denied · offsets every 1s",
		Toasts: []Toast{
			{Role: kit.Warn, Glyph: "▲", Title: "ISR shrunk", Subject: "orders/1", Detail: "replicas [3] fell out of sync", Age: 4 * time.Second, Life: 0.9},
			{Role: kit.Info, Glyph: "•", Title: "Leader moved", Subject: "orders/1", Detail: "broker 1 → broker 2", Age: 4 * time.Second, Life: 0.9},
			{Role: kit.OK, Glyph: "✓", Title: "Topic created", Subject: "payments", Detail: "2 partitions", Age: 50 * time.Second, Life: 0.2},
		},
	}
}

// TestLiveScreensNeverTruncate: cutting numbers off is how the bench table
// first failed in a 45-column pane, so live screens must lay out, not clip.
func TestLiveScreensNeverTruncate(t *testing.T) {
	k, _ := kits(t)
	quiet := watchFixture()
	quiet.Toasts, quiet.Series = nil, nil
	quiet.Empty, quiet.Quiet = "no topics followed · add -t <topic> to chart it", "no cluster events yet"
	for w := 40; w <= 120; w++ {
		var lines []string
		lines = append(lines, benchFixture().Render(k, w)...)
		lines = append(lines, watchFixture().Render(k, w, 40)...)
		lines = append(lines, quiet.Render(k, w, 40)...)
		lines = append(lines, tableFixture().Render(k, w)...)
		lines = append(lines, treeFixture().Render(k, w)...)
		lines = append(lines, partitionsFixture().Render(k, w)...)
		lines = append(lines, groupsFixture().Render(k, w)...)
		lines = append(lines, procFixture().Render(k, w)...)
		for i, l := range lines {
			if strings.Contains(l, "…") {
				t.Fatalf("width %d line %d truncated: %q", w, i, l)
			}
		}
	}
}

func groupsFixture() Groups {
	return Groups{
		Tabs: []string{"Lag", "Rate", "Members", "Name"}, Sorted: 0,
		Badge: "live · every 2s",
		Rows: []GroupRow{
			{Name: "billing", State: "Stable", Members: 3, Topics: []string{"orders", "payments"}, Lag: 48210, Rate: 1250},
			{Name: "fraud-check", State: "PreparingRebalance", Members: 2, Topics: []string{"payments"}, Lag: 9120, Rate: 0},
			{Name: "search-index", State: "Stable", Members: 6, Topics: []string{"orders"}, Lag: 340, Rate: 2210.5},
			{Name: "audit-sink", State: "Empty", Topics: []string{"audit"}, Lag: 0, Rate: 0},
			{Name: "legacy", State: "Dead", Lag: -1, Rate: -1, Err: true},
		},
		History: []int64{40000, 42000, 41000, 45000, 47000, 46500, 50100, 57670},
		Empty:   "No consumer groups yet.",
		Hint:    "sorted by lag · refreshing every 2s · ctrl-c to stop · --once for a single read",
	}
}

func TestGroupsEmptyAndSort(t *testing.T) {
	k, _ := kits(t)
	g := groupsFixture()
	g.Rows, g.History = nil, nil
	if out := kit.Join(g.Render(k, 80)); !strings.Contains(out, "No consumer groups yet.") {
		t.Errorf("empty groups lost their message\n%s", out)
	}
	g = groupsFixture()
	g.Sorted = 3
	out := kit.Join(g.Render(k, 100))
	if !strings.Contains(out, "Group ▲") || strings.Contains(out, "Lag ▼") {
		t.Errorf("sorted by name should mark Group ▲ only\n%s", out)
	}
}

func TestCatchUp(t *testing.T) {
	for _, c := range []struct {
		row  GroupRow
		want string
	}{
		{GroupRow{Lag: 600, Rate: 10, Members: 1}, "1m"},
		{GroupRow{Lag: 0, Rate: 0}, "caught up"},
		{GroupRow{Lag: 5, Rate: 0, Members: 2}, "stalled"},
		{GroupRow{Lag: 5, Rate: 0}, "no members"},
		{GroupRow{Lag: 5, Rate: -1}, "-"},
	} {
		if got, _ := catchUp(c.row); got != c.want {
			t.Errorf("catchUp(%+v) = %q, want %q", c.row, got, c.want)
		}
	}
}

func TestSpan(t *testing.T) {
	for sec, want := range map[float64]string{0.2: "<1s", 40: "40s", 600: "10m", 7200: "2h", 200000: "2d"} {
		if got := Span(sec); got != want {
			t.Errorf("Span(%v) = %q, want %q", sec, got, want)
		}
	}
}

func procFixture() ProcTree {
	topic := func(name string, leads, args string, role kit.Role, tags ...Cell) ProcNode {
		return ProcNode{Cells: []Cell{{Text: leads, Role: kit.Rule}}, Name: name, NameRole: role, Args: args, Tags: tags}
	}
	return ProcTree{
		Title: "kaas brokers · cluster 5L6g3nSh", IDName: "ID", TreeName: "NODE", Count: "4 brokers",
		Columns: []Column{{Name: "LEADS", Right: true}, {Name: "DISK", Right: true, Drop: 1}},
		Root: ProcNode{Name: "cluster", NameRole: kit.Accent2, Args: "5L6g3nSh · controller 2", Children: []ProcNode{
			{ID: "1", Cells: []Cell{{Text: "9", Role: kit.Warn}, {Text: "1.2 GiB"}}, Name: "broker 1", NameRole: kit.Accent,
				Args: "kafka-1:9092 · rack a", Children: []ProcNode{
					topic("orders", "4", "p0-2 p5", kit.Info),
					topic("payments", "3", "p0-2", kit.Info, Cell{Text: "1 behind", Role: kit.Warn}),
					topic("audit", "", "follows 2", kit.Accent),
				}},
			{ID: "2", Cells: []Cell{{Text: "5"}, {Text: "980 MiB"}}, Name: "broker 2", NameRole: kit.Accent,
				Args: "kafka-2:9092 · rack b", Tags: []Cell{{Text: "controller", Role: kit.Notice}}},
			{ID: "3", Cells: []Cell{{Text: "4"}, {Text: "1.1 GiB"}}, Name: "broker 3", NameRole: kit.Accent, Args: "kafka-3:9092 · rack c"},
			{ID: "4", Cells: []Cell{{Text: "0"}, {Text: "-"}}, Name: "broker 4", NameRole: kit.Err,
				Args: "not in metadata", Tags: []Cell{{Text: "offline", Role: kit.Err}}},
		}},
		Load: "skew", LoadValue: "1.50x", LoadRole: kit.Warn,
		Legend:    []Legend{{"controller", kit.Notice}, {"behind", kit.Warn}, {"offline", kit.Err}},
		FootRight: "4 topics · 18 partitions",
	}
}

func lagFixture() Lag {
	return Lag{
		Title: "consumer lag · ranked", Live: true, Chart: "total lag",
		Rows: []LagRow{
			{Group: "billing", Topic: "orders", Lag: 167641, Trend: 1240, TrendKnown: true},
			{Group: "search-index", Topic: "orders", Lag: 57098, Trend: -3100, TrendKnown: true},
			{Group: "billing", Topic: "payments", Lag: 9120, TrendKnown: true},
			{Group: "fraud-check", Topic: "payments", Lag: 7032, Trend: 600, TrendKnown: true},
			{Group: "audit-sink", Topic: "audit", Lag: 0, TrendKnown: true},
		},
		Hidden:  2,
		History: []float64{190000, 205000, 214000, 209000, 221000, 226000, 224000, 231000, 240000, 236000, 228000, 240891},
		Foot: []Detail{{Label: "min", Value: "190,000"}, {Label: "avg", Value: "222,074"}, {Label: "max", Value: "240,891"},
			{Label: "consumed", Value: "3,461/s", Role: kit.Accent}},
		Hint: "every 2s · ctrl-c to stop · kaas groups for members and rates",
	}
}

func TestLagScreens(t *testing.T) {
	k, c := kits(t)
	golden(t, "lag", lagFixture().Render(k, 100, 40))
	golden(t, "lag_narrow", lagFixture().Render(k, 44, 40))
	up := CaughtUp{Title: "You're all caught up", Detail: "5 groups are at the head of 6 topics.", Action: "kaas groups for members and rates →"}
	golden(t, "caught_up", up.Render(k, 80))
	for w := 40; w <= 120; w++ {
		for _, lines := range [][]string{lagFixture().Render(c, w, 30), up.Render(c, w), lagFixture().Render(k, w, 20)} {
			for i, l := range lines {
				if n := kit.Width(l); n > w {
					t.Fatalf("width %d line %d is %d wide", w, i, n)
				}
				if strings.Contains(l, "…") {
					t.Fatalf("width %d line %d truncated: %q", w, i, l)
				}
			}
		}
	}
}

func TestShort(t *testing.T) {
	for v, want := range map[float64]string{950: "950", 1000: "1k", 1240: "1.2k", 57098: "57k", 2_500_000: "2.5M", 40_000_000: "40M"} {
		if got := Short(v); got != want {
			t.Errorf("Short(%v) = %q, want %q", v, got, want)
		}
	}
}

func channelFixture() Channel {
	at := time.Date(2026, 10, 31, 14, 2, 0, 0, time.UTC)
	tick := func(s int) time.Time { return at.Add(time.Duration(s) * time.Second) }
	return Channel{
		Name: "#orders", Info: "[3 partitions]  from 5 before the end", Server: "localhost:9092",
		Lines: []ChanLine{
			{Kind: LineJoin, Time: tick(0), Nick: "p0", Hue: 0, Text: "reading from offset 1,203"},
			{Kind: LineJoin, Time: tick(0), Nick: "p1", Hue: 1, Text: "reading from offset 998"},
			{Kind: LineMsg, Time: tick(1), Meta: "p0:1203", Nick: "user-42", Hue: 0, Text: `{"order":9001,"amount":12.5,"currency":"EUR"}`},
			{Kind: LineMsg, Time: tick(1), Meta: "p1:998", Nick: "user-7", Hue: 1, Text: `{"order":9002,"amount":3.2,"currency":"USD","items":[{"sku":"A-100","qty":2},{"sku":"B-220","qty":1}],"note":"leave at the door"}`},
			{Kind: LineAct, Time: tick(1), Text: "headers: trace-id=4bf92f3577b34da6 · source=checkout"},
			{Kind: LinePart, Time: tick(2), Meta: "p0:1204", Nick: "user-13", Hue: 0, Text: "tombstone"},
			{Kind: LineMsg, Time: tick(3), Meta: "p1:999", Nick: "·", Hue: 1, Text: "<128 bytes, binary>"},
		},
		Nicks:  []Nick{{Name: "p0", Hue: 0, Count: "2"}, {Name: "p1", Hue: 1, Count: "2"}, {Name: "p2", Hue: 2, Count: "0"}},
		Status: []string{"14:02:03", "orders", "read 4 · 2/s", "3 partitions", "no group, nothing committed"},
		Input:  "ctrl-c to stop · -o json for scripts",
	}
}

func TestChannel(t *testing.T) {
	k, c := kits(t)
	golden(t, "channel", channelFixture().Render(k, 100, 18))
	golden(t, "channel_narrow", channelFixture().Render(k, 50, 24))
	for w := 40; w <= 120; w++ {
		for i, l := range channelFixture().Render(c, w, 20) {
			if n := kit.Width(l); n > w {
				t.Fatalf("width %d line %d is %d wide", w, i, n)
			}
			if strings.Contains(l, "…") {
				t.Fatalf("width %d line %d truncated: %q", w, i, l)
			}
		}
	}
	long := channelFixture()
	long.Lines = []ChanLine{{Kind: LineMsg, Nick: "k", Text: strings.Repeat("x", 2000)}}
	out := kit.Join(long.Render(k, 80, 30))
	if !strings.Contains(out, "more rows, -o json for all of it") {
		t.Errorf("a huge value should be cut after a few rows:\n%s", out)
	}
}
