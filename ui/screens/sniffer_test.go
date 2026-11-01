package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func snifferFixture() Sniffer {
	rows := []SniffRow{
		{1, 0.000412, "127.0.0.1:53422", "broker 0", "ApiVersions", 18, 62, "kgo 1.22"},
		{2, 0.001830, "127.0.0.1:53422", "broker 0", "Metadata", 3, 41, "all topics"},
		{3, 0.004211, "127.0.0.1:53430", "broker 1", "InitProducerID", 22, 37, "idempotent"},
		{4, 0.009904, "127.0.0.1:53430", "broker 1", "Produce", 0, 1311, "orders p0,p2 · 24 records · acks all"},
		{5, 0.010220, "127.0.0.1:53441", "broker 2", "FindCoordinator", 10, 44, "group billing"},
		{6, 0.012500, "127.0.0.1:53441", "broker 2", "JoinGroup", 11, 160, "billing · consumer · member new"},
		{7, 0.230117, "127.0.0.1:53441", "broker 2", "SyncGroup", 14, 220, "billing · generation 1"},
		{8, 0.240760, "127.0.0.1:53450", "broker 1", "Fetch", 1, 120, "orders p0@0 orders p2@0 · waits 5000ms"},
		{9, 0.512002, "127.0.0.1:53441", "broker 2", "OffsetCommit", 8, 98, "billing · orders p0→24 orders p2→17"},
		{10, 0.733411, "127.0.0.1:53441", "broker 2", "Heartbeat", 12, 53, "billing · generation 1"},
		{11, 1.020044, "127.0.0.1:53460", "broker 0", "DeleteTopics", 20, 40, "orders-old"},
	}
	return Sniffer{
		Capture: "proxy on localhost:19092 → localhost:29092", Filter: "all requests",
		Total: 11, Rows: rows, Sel: 3,
		Detail: "Request 4 v9: 1.3 KiB → 70 B in 4.2ms",
		Path:   "127.0.0.1:53430 (kgo) > broker 1 > ",
		Head:   []byte{0, 0, 5, 27, 0, 0, 0, 9, 0, 0, 0, 4, 0, 3, 'k', 'g', 'o', 0, 0, 0, 0xff, 0xff, 0, 0, 0x75, 0x30, 2, 7, 'o', 'r', 'd', 'e'},
		Mix:    []SniffMix{{"Fetch", 1, 120}, {"Heartbeat", 12, 40}, {"Produce", 0, 24}, {"OffsetCommit", 8, 10}, {"Metadata", 3, 4}, {"JoinGroup", 11, 2}},
		MixOf:  200, Status: "capturing", Rate: "12.4 kB/s",
		Keys: "q quit · ↑↓ select · enter follow",
	}
}

func TestSnifferGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "sniffer", snifferFixture().Render(k, 100, 26))
	golden(t, "sniffer_narrow", snifferFixture().Render(k, 80, 24))

	// Nothing captured yet: the list says why, and nothing is selected.
	s := Sniffer{Capture: "proxy on localhost:19092", Filter: "all requests", Status: "capturing",
		Waiting: "point a client at localhost:19092", Keys: "q quit"}
	out := kit.Join(s.Render(k, 100, 24))
	if !strings.Contains(out, "point a client at") || strings.Contains(out, "▶") {
		t.Errorf("empty:\n%s", out)
	}
	if APIRole(0) == APIRole(1) || APIRole(999) != kit.FG {
		t.Error("produce and fetch share a colour, or an unknown key has one")
	}
}
