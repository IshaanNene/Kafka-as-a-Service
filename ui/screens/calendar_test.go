package screens

import (
	"strings"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func pickerFixture() RangePicker {
	d := func(m time.Month, day int) time.Time { return time.Date(2026, m, day, 0, 0, 0, 0, time.UTC) }
	return RangePicker{
		Title: "Read orders between", Month: d(10, 1),
		Start: d(10, 26), End: d(11, 3), Cursor: d(11, 3),
		Day: func(t time.Time) int {
			switch {
			case t.Before(d(10, 9)) || t.After(d(11, 1)):
				return DayOut
			case t.Equal(d(10, 18)):
				return DayEmpty
			}
			return DayData
		},
		From: "Oct 26", To: "Nov 3", Label: "Records", Count: "48,210", Ready: true,
		Hint: "←→↑↓ move · enter pick · [ ] month · esc quit",
	}
}

func TestPickerGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "picker", pickerFixture().Render(k, 80))
	golden(t, "picker_narrow", pickerFixture().Render(k, 40))

	// Before a start: the footer asks for dates and Apply is dim.
	p := pickerFixture()
	p.Start, p.End, p.From, p.To, p.Count, p.Ready = time.Time{}, time.Time{}, "", "", "-", false
	if out := kit.Join(p.Render(k, 80)); strings.Count(out, "Add date") != 2 || !strings.Contains(out, "↖") || strings.Contains(out, "[26]") {
		t.Errorf("unpicked:\n%s", out)
	}
}
