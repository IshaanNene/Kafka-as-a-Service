package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func bisectFixture() Bisect {
	return Bisect{
		Title: "find · orders p0 · first record at or after 09:00:00",
		From:  "offset 0", To: "end 12,480",
		Start: 0, End: 12480, Lo: 6241, Hi: 9360, Testing: 7800, Found: -1,
		Marks: []BisectMark{{6240, false}, {9360, true}},
		Log: []string{
			"$ kaas offsets find orders --time 09:00",
			"Bisecting: 12,480 offsets left to read after this",
			"[p0@6240] 08:41:02.118  user-17",
			"  ts 08:41:02  before 09:00",
		},
		Good: "before", Bad: "at or after",
	}
}

func TestBisectGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "bisect", bisectFixture().Render(k, 88, 24))
	done := bisectFixture()
	done.Done, done.Found, done.Testing, done.Lo, done.Hi = true, 8213, -1, 8213, 8213
	golden(t, "bisect_done", done.Render(k, 60, 20))

	// A partition shorter than the rail: a slot per offset.
	short := Bisect{Start: 0, End: 5, Lo: 0, Hi: 5, Testing: 2, Found: -1}
	out := short.Render(k, 88, 14)
	if rail := out[4]; strings.Count(rail, "○")+strings.Count(rail, "◆") != 5 || !strings.Contains(rail, "○─────") {
		t.Errorf("5 offsets should be 5 slots, spread out:\n%s", kit.Join(out))
	}
}
