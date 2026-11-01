package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func stepperFixture(current int) Stepper {
	steps := []Step{{Name: "Name"}, {Name: "Layout"}, {Name: "Configs"}, {Name: "Create"}}
	for i := range current {
		steps[i].Done = true
	}
	return Stepper{Title: "Create topic", Steps: steps, Current: current, Back: current > 0, Button: "Continue →",
		Hint: "enter continue · tab next field · esc back · ctrl-c quit"}
}

func TestStepperGoldens(t *testing.T) {
	k, _ := kits(t)

	name := stepperFixture(0)
	name.Heading = "Name your topic"
	named := func(w int) []string {
		s := name
		s.Body = append(Input(k, "Topic name", "payments.eu.refunds", min(44, s.BodyWidth(w)), true, ""), "")
		s.Body = append(s.Body, k.S(kit.Muted, "Family"), k.S(kit.Rule, "payments › eu › ")+k.S(kit.Accent, "refunds"), k.S(kit.Muted, "2 topics share payments.eu"))
		return s.Render(k, w, 22)
	}
	golden(t, "stepper", named(76))
	golden(t, "stepper_narrow", named(50))

	configs := stepperFixture(2)
	configs.Heading = "Set what differs from the broker"
	for i, c := range []struct {
		name, desc, value string
		set               bool
	}{
		{"Retention", "how long records stay", "30d", true},
		{"Cleanup", "delete or compact", "compact", true},
		{"Min in-sync", "replicas to ack", "default", false},
		{"Compression", "on disk", "default", false},
	} {
		configs.Body = append(configs.Body, Card(k, c.name, c.desc, c.value, c.set, i == 2, 50)...)
	}
	golden(t, "stepper_configs", configs.Render(k, 76, 22))

	done := stepperFixture(4)
	done.Current = 3
	done.Heading = "Created"
	done.Back, done.Button, done.Pressed = false, "Done", true
	for _, l := range Burst(k) {
		done.Body = append(done.Body, "                "+l)
	}
	done.Body = append(done.Body, "", "         "+k.S(kit.FG, "payments.eu.refunds is ready"), "        "+k.S(kit.Muted, "12 partitions · RF 3 · 2 configs"), "", "    "+Progress(k, 1, 40))
	golden(t, "stepper_done", done.Render(k, 76, 22))
}

func TestStepperFits(t *testing.T) {
	k, _ := kits(t)
	s := stepperFixture(1)
	s.Heading = "Lay out its partitions"
	s.Body = Radios(k, "Replication factor", []string{"1", "2", "3"}, 2, true)
	for _, w := range []int{40, 50, 76, 120, 200} {
		out := s.Render(k, w, 22)
		for _, l := range out {
			if kit.Width(l) > max(w, 36) || kit.Width(l) > 90 {
				t.Errorf("width %d: line %d wide", w, kit.Width(l))
			}
		}
		if j := kit.Join(out); !strings.Contains(j, "Step 2 of 4") || !strings.Contains(j, "Continue →") || !strings.Contains(j, "(✓)") || strings.Contains(j, "…") {
			t.Errorf("width %d lost the step, button or tick:\n%s", w, j)
		}
	}
}
