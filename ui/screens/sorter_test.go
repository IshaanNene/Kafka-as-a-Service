package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func sorterFixture() Sorter {
	return Sorter{
		Meters: []Meter{{Label: "1", Fill: 0.62, Text: "62.0%"}, {Label: "2", Fill: 0.31, Text: "31.0%"}, {Label: "3", Fill: 0.07, Text: "7.0%"}},
		Gauge:  Meter{Label: "Read", Fill: 0.84, Text: "0.8× writes"},
		Stats: []Stat{
			{Label: "Partitions", Value: "14 across 5 topics; 3 brokers"},
			{Label: "Write", Value: "1.2k rec/s · 96 KiB/s"},
			{Label: "Read", Value: "1,010 rec/s by 3 groups"},
		},
		Sort: SortIn,
		Rows: []SortRow{
			{Partition: 2, Leader: "b1", In: 612, Out: 600, Bytes: 49152, Size: 81 << 20, Topic: "orders", Note: "end 1,204,551"},
			{Partition: 0, Leader: "b2", In: 301, Out: 290, Bytes: 24000, Size: 40 << 20, Topic: "orders", Note: "end 602,113"},
			{Partition: 1, Leader: "b3", In: 88, Out: 0, Bytes: 7100, Size: 9 << 20, Topic: "payments.eu.charges", Note: "end 41,920"},
			{Partition: 0, Leader: "b1", In: 3.5, Out: 120, Bytes: 280, Size: 2 << 20, Topic: "audit", Note: "end 9,001"},
			{Partition: 0, Leader: "none", In: -1, Out: -1, Bytes: -1, Size: -1, Topic: "users.profiles", Note: "offline"},
		},
		Hidden: 9,
		Hints:  []Hint{{Key: "^C", Label: "Quit"}, {Key: "--sort ", Label: "in", Hot: true}, {Key: "--top ", Label: "5"}, {Key: "every ", Label: "2s"}},
	}
}

func TestSorter(t *testing.T) {
	k, c := kits(t)
	golden(t, "sorter", sorterFixture().Render(k, 100, 30))
	golden(t, "sorter_narrow", sorterFixture().Render(k, 44, 30))
	golden(t, "sorter_short", sorterFixture().Render(k, 80, 12))
	for w := 40; w <= 200; w++ {
		for i, l := range sorterFixture().Render(c, w, 30) {
			if n := kit.Width(l); n > w {
				t.Fatalf("width %d line %d is %d wide: %q", w, i, n, l)
			}
			if w <= 120 && strings.Contains(l, "…") {
				t.Fatalf("width %d line %d truncated: %q", w, i, l)
			}
		}
	}
	for _, sort := range []string{SortIn, SortOut, SortBytes, SortSize, SortPartition} {
		s := sorterFixture()
		s.Sort = sort
		head := kit.Join(s.Render(k, 40, 30))
		if !strings.Contains(head, "▐") {
			t.Errorf("sorted by %s at 40 lost its column\n%s", sort, head)
		}
	}
	empty := sorterFixture()
	empty.Rows, empty.Hidden, empty.Empty = nil, 0, "No partitions match orders.*"
	if out := kit.Join(empty.Render(k, 80, 30)); !strings.Contains(out, "No partitions match") {
		t.Errorf("empty sorter lost its message\n%s", out)
	}
}
