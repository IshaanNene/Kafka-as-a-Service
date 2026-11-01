package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func flameFixture() Flame {
	leaf := func(name string, t float64) FlameNode { return FlameNode{Name: name, Time: t, Self: t} }
	node := func(name string, kids ...FlameNode) FlameNode {
		n := FlameNode{Name: name, Kids: kids}
		for _, k := range kids {
			n.Time += k.Time
		}
		return n
	}
	root := node("all",
		node("Produce",
			node("broker 1", node("orders", leaf("wait", 410), leaf("queue", 120), leaf("send", 40), leaf("ack", 12))),
			node("broker 2", node("payments", leaf("wait", 180), leaf("send", 22), leaf("ack", 6))),
		),
		node("Fetch",
			node("broker 1", node("orders", leaf("wait", 260), leaf("ack", 70), leaf("send", 8))),
			node("broker 2", node("orders +1", leaf("wait", 90), leaf("ack", 30))),
		),
		node("Heartbeat", node("broker 0", leaf("wait", 30), leaf("send", 3))),
		node("Metadata", node("broker 0", leaf("wait", 9))),
	)
	return Flame{
		Title: "latency profile · proxy on localhost:19092 · every client · polls hidden",
		Right: "requests 48,213",
		Root:  root, Sel: []int{0, 0, 0, 0},
		Name: "wait", Share: "31.6% of time", Stats: "12,041 requests · avg 3.4ms · p99 21ms",
		Path: "Produce → broker 1 → orders",
		Hint: "←→ sibling · ↑ up the stack · ↓ down · enter zoom · esc out · i polls · q quit",
	}
}

func TestFlameGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "flame", flameFixture().Render(k, 100, 26))
	golden(t, "flame_narrow", flameFixture().Render(k, 72, 22))
	f := flameFixture()
	f.Root, f.Name, f.Empty = FlameNode{Name: "all"}, "", "nothing yet: point a client at localhost:19092"
	if out := kit.Join(f.Render(k, 100, 26)); !strings.Contains(out, "nothing yet") || strings.Contains(out, "▼") {
		t.Errorf("empty:\n%s", out)
	}
	if heatHex(1, true) != flameHeat[6] || heatHex(0, false) != flameHeat[0] {
		t.Error("heat ends")
	}
}
