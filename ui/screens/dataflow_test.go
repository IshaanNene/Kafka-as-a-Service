package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func flowFixture() DataFlow {
	return DataFlow{
		Sources: []FlowEnd{
			{Name: "kafka:orders", Sub: "group etl", Count: "12,408 read", State: FlowLive},
			{Name: "listen://:8080", Sub: "webhooks", Count: "311 read", State: FlowIdle},
			{Name: "file:backfill.jsonl", Count: "5,000 read", State: FlowDone},
		},
		Sinks:    []FlowEnd{{Name: "kafka:lake", Sub: "acks all · idempotent", Count: "17,512 written", State: FlowLive, Fill: 0.6}},
		Steps:    []FlowStep{{Text: "read"}, {Text: "where value.amount > 100", Active: true}, {Text: "write ×1"}, {Text: "ack"}},
		InFlight: 240, Batch: 500,
		Stats: []FlowStat{
			{"written", "17,512", kit.OK}, {"filtered", "190", kit.FG}, {"rejected", "17", kit.Err}, {"rate", "4.1k/s", kit.FG},
		},
		Job:   "pipe  00:42",
		Frame: 3,
	}
}

func TestDataFlowGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "dataflow", flowFixture().Render(k, 96))
	fan := flowFixture()
	fan.Sources = fan.Sources[:1]
	fan.Sinks = []FlowEnd{
		{Name: "file:out.jsonl", Count: "9 written", State: FlowLive},
		{Name: "https://hooks.example.com/x", Count: "0 written", State: FlowFailed, Detail: "503 after 6 tries"},
	}
	golden(t, "dataflow_fanout", fan.Render(k, 80))
	many := flowFixture()
	many.Sinks = append(many.Sinks, many.Sinks[0], many.Sinks[0], FlowEnd{Name: "ledger:x", State: FlowFailed})
	out := kit.Join(many.Render(k, 96))
	for _, want := range []string{"+2 more", "✗ ledger:x fai", "transform", "in flight 240", "rejected 17"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q:\n%s", want, out)
		}
	}
	if n := len(flowFixture().Render(k, 60)); n != FlowHeight() {
		t.Errorf("%d rows", n)
	}
}
