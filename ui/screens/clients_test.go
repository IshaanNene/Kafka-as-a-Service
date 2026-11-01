package screens

import (
	"strings"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func clientsFixture() Clients {
	return Clients{
		Tabs: []string{"Rate", "Parts", "Name"}, Badge: "live · every 2s",
		Rows: []ClientRow{
			{Name: "pid 4012", Producer: true, Where: "orders +1", Parts: 6, Rate: 1840, Lag: -1, Idle: time.Second},
			{Name: "billing-svc", Where: "billing", Host: "/10.0.0.4", Parts: 3, Rate: 1210, Lag: 0},
			{Name: "billing-svc", Where: "billing", Host: "/10.0.0.7", Parts: 3, Rate: 640, Lag: 4120},
			{Name: "pid 5001", Producer: true, Where: "payments", Parts: 2, Rate: 220, Lag: -1, Txn: true},
			{Name: "fraud-check", Where: "fraud-check", Host: "/10.0.1.2", Parts: 2, Rate: 0, Lag: 9120},
			{Name: "pid 3990", Producer: true, Where: "audit", Parts: 1, Rate: 0, Lag: -1, Idle: 14 * time.Minute},
			{Name: "search-index", Where: "search-index", Host: "/10.0.2.9", Parts: 6, Rate: -1, Lag: -1},
		},
		History: []float64{3200, 3500, 3410, 3700, 3910, 3800, 3910},
		Empty:   "No clients yet.",
		Hint:    "sorted by rate · every 2s · ctrl-c to stop · consumers from group members, producers from KIP-664; plain producers and group-less readers don't show",
	}
}

func TestClients(t *testing.T) {
	k, c := kits(t)
	golden(t, "clients", clientsFixture().Render(k, 100))
	golden(t, "clients_narrow", clientsFixture().Render(k, 50))
	for w := 40; w <= 200; w++ {
		for i, l := range clientsFixture().Render(c, w) {
			if n := kit.Width(l); n > w {
				t.Fatalf("width %d line %d is %d wide: %q", w, i, n, l)
			}
			if w <= 120 && strings.Contains(l, "…") {
				t.Fatalf("width %d line %d truncated: %q", w, i, l)
			}
		}
	}
	empty := clientsFixture()
	empty.Rows, empty.History = nil, nil
	if out := kit.Join(empty.Render(k, 80)); !strings.Contains(out, "No clients yet.") {
		t.Errorf("empty clients lost their message\n%s", out)
	}
	byName := clientsFixture()
	byName.Sorted = 2
	if out := kit.Join(byName.Render(k, 100)); !strings.Contains(out, "Client ▲") || strings.Contains(out, "Rate ▼") {
		t.Errorf("sorted by name should mark Client ▲ only\n%s", out)
	}
}
