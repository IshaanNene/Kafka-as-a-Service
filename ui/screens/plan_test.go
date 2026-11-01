package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func planFixture() Plan {
	return Plan{
		Command: "kaas offsets reset billing -t orders -t payments --to 2h",
		Lead:    "kaas will perform the following actions:",
		Blocks: []PlanBlock{
			{Change: PlanChange, Comment: "billing · orders", Resource: `offsets "billing" "orders"`, Attrs: []PlanAttr{
				{Key: "p0", From: "1,204", To: "980", Note: "re-reads 224"},
				{Key: "p1", From: "2,010", To: "1,700", Note: "re-reads 310"},
			}, Hidden: "(1 unchanged partition hidden)"},
			{Change: PlanAdd, Comment: "billing · payments", Resource: `offsets "billing" "payments"`, Attrs: []PlanAttr{
				{Key: "p0", To: "0", Note: "start of the log"},
			}},
		},
		Add: 1, Change: 2, Note: "billing reads 534 records again · to the first record after 2026-11-01 10:00",
		Footer: "group: billing  ·  state: Empty", Phase: "awaiting approval", PhaseOf: kit.Accent, Lock: "members: 0",
	}
}

func TestPlanGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "plan", planFixture().Render(k, 90))

	applied := planFixture()
	applied.Ask, applied.Answer = "Do you want to perform these actions?", "yes"
	applied.Apply = []ApplyLine{
		{Name: "orders", Status: "Committing...", Role: kit.Notice, Detail: "[2 partitions]"},
		{Name: "orders", Status: "Commit complete after 4ms", Role: kit.Notice},
		{Name: "payments", Status: "Committing...", Role: kit.OK, Detail: "[1 partition]"},
		{Name: "payments", Status: "Commit complete after 3ms", Role: kit.OK},
	}
	applied.Done, applied.Phase, applied.PhaseOf = "Reset complete! Offsets: 1 added, 2 changed.", "applied", kit.OK
	golden(t, "plan_applied", applied.Render(k, 90))

	// Narrow: the footer keeps the phase and drops the counts.
	out := kit.Join(planFixture().Render(k, 50))
	if !strings.Contains(out, "awaiting approval") || strings.Contains(out, "members: 0") {
		t.Errorf("narrow footer:\n%s", out)
	}
}
