package main

import (
	"slices"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func TestTopicRows(t *testing.T) {
	part := func(leader int32, isr []int32, start, end int64) snapshot.Partition {
		return snapshot.Partition{Leader: leader, Replicas: []int32{1, 2, 3}, ISR: isr, StartOffset: start, EndOffset: end}
	}
	s := &snapshot.Snapshot{Topics: []snapshot.Topic{
		{Name: "__consumer_offsets", Internal: true, Partitions: []snapshot.Partition{part(1, []int32{1, 2, 3}, 0, 5)}},
		{Name: "audit", Partitions: []snapshot.Partition{part(1, []int32{1, 2, 3}, 0, 10)}},
		{Name: "big", Partitions: []snapshot.Partition{part(2, []int32{1, 2, 3}, 100, 1100), part(3, []int32{1, 2, 3}, 0, 500)}},
		{Name: "lagging", Partitions: []snapshot.Partition{part(1, []int32{1}, 0, 1)}},
		{Name: "dark", Partitions: []snapshot.Partition{part(-1, []int32{}, snapshot.UnknownOffset, snapshot.UnknownOffset)}},
	}}

	rows, internal := topicRows(s, "status", nil)
	if internal != 1 {
		t.Errorf("internal = %d, want 1", internal)
	}
	var order []string
	for _, r := range rows {
		order = append(order, r.name)
	}
	if want := []string{"dark", "lagging", "big", "audit"}; !slices.Equal(order, want) {
		t.Errorf("status order = %v, want %v", order, want)
	}
	if rows[0].status != kit.Down || rows[0].off != 1 || rows[0].records != -1 {
		t.Errorf("dark = %+v, want offline with unknown records", rows[0])
	}
	if rows[1].status != kit.Degraded || rows[1].urp != 1 {
		t.Errorf("lagging = %+v, want under-replicated", rows[1])
	}
	if rows[2].records != 1500 || rows[2].rf != 3 {
		t.Errorf("big = %+v, want 1500 records at RF 3", rows[2])
	}

	rows, _ = topicRows(s, "name", nil)
	if rows[0].name != "audit" {
		t.Errorf("name sort starts with %s", rows[0].name)
	}
	screen := snapshotScreen(s, rows, 1, "name")
	if screen.Sorted != 0 || !screen.Asc || screen.FootRight != "1 internal topic hidden" {
		t.Errorf("screen sort/foot = %d %v %q", screen.Sorted, screen.Asc, screen.FootRight)
	}
}
