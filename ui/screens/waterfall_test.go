package screens

import (
	"strings"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func waterFixture() Waterfall {
	ms := func(f float64) time.Duration { return time.Duration(f * float64(time.Millisecond)) }
	row := func(name string, key int16, status, info string, size int64, start float64, st ...float64) WaterRow {
		r := WaterRow{Name: name, Key: key, Status: status, Info: info, Size: size, Start: ms(start), Done: status != "pending"}
		for i, d := range st {
			r.Stages[i] = ms(d)
		}
		return r
	}
	return Waterfall{
		Title: "Trace", Note: "● billing-svc · 127.0.0.1:53422 · 3 brokers",
		Tabs: APITabs,
		Rows: []WaterRow{
			row("ApiVersions", 18, "ok", "kgo v1.22.2", 412, 0, 1.2, 0.1, 0, 0.6, 0.1),
			row("Metadata", 3, "ok", "orders", 310, 2.1, 0, 0.1, 0, 1.4, 0.1),
			row("FindCoordinator", 10, "ok", "group billing", 52, 4, 0, 0.1, 0, 2.2, 0.1),
			row("JoinGroup", 11, "MEMBER_ID_REQUIRED", "billing · consumer · member new", 74, 8, 1.1, 0.1, 0, 0.8, 0.1),
			row("JoinGroup", 11, "ok", "billing · consumer · member kgo-6a1", 190, 11, 0, 0.1, 0, 302, 0.1),
			row("SyncGroup", 14, "ok", "billing · generation 1", 88, 314, 0, 0.1, 0, 2.5, 0.1),
			row("OffsetFetch", 9, "ok", "billing", 96, 318, 0, 0.1, 0, 1.9, 0.1),
			row("ListOffsets", 2, "ok", "orders p0 latest orders p1 latest", 120, 321, 0.9, 0.1, 0, 1.1, 0.1),
			row("Fetch", 1, "ok", "orders p0@1204 orders p1@988 · waits 500ms", 18200, 324, 0, 0.1, 0, 12, 1.5),
			row("Heartbeat", 12, "ok", "billing · generation 1", 18, 340, 0, 0.1, 6.2, 1.1, 0.1),
			row("Fetch", 1, "ok", "session 7 · nothing new to ask for · waits 500ms", 40, 342, 0, 0.1, 0, 500, 0.1),
			row("Produce", 0, "ok", "audit p0 · 1 record · acks all", 64, 520, 1.4, 0.2, 0, 3.1, 0.1),
			row("Fetch", 1, "pending", "session 7 · nothing new to ask for · waits 500ms", -1, 843, 0, 0.1, 0, 210, 0),
		},
		Sel: 9, Now: 1053 * time.Millisecond, Span: 1200 * time.Millisecond,
		Marks: []WaterMark{
			{At: 316600 * time.Microsecond, Label: "joined billing", Text: "joined billing: 317ms", Role: kit.Accent2},
			{At: 524800 * time.Microsecond, Label: "first produce acked", Text: "first produce acked: 525ms", Role: kit.Warn},
		},
		Summary: "13 requests  |  19.7 KiB answered  |  Finish: 1.05 s",
		Detail:  "▶ Heartbeat · connect - · send 100µs · queue 6.2ms · wait 1.1ms · ack 100µs",
	}
}

func TestWaterfallGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "waterfall", waterFixture().Render(k, 104, 28))
	golden(t, "waterfall_narrow", waterFixture().Render(k, 80, 22))
	out := kit.Join(waterFixture().Render(k, 104, 28))
	for _, want := range []string{"▼", "joined billing", "error", "MEMBER_ID_REQUIRED", "pending", "▫"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q", want)
		}
	}
	if APITab(0) != 1 || APITab(1) != 2 || APITab(12) != 4 || APITab(18) != 8 || APITabs[APITab(20)] != "Admin" {
		t.Error("tabs")
	}
	if s := niceStep(1200 * time.Millisecond); s != 500*time.Millisecond {
		t.Errorf("step %v", s)
	}
}
