package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func vimFixture() ModalEditor {
	return ModalEditor{
		File: "orders.properties",
		Lines: strings.Split(`# kaas config edit · topic orders
# A line without # is set on the topic; comment it out or delete it to

[retention]
# delete.retention.ms = 86400000  # 1d
# retention.bytes     = -1  # unlimited
retention.ms          = 3d

[cleanup]
cleanup.policy        = compact,delete  # default delete`, "\n"),
		Row: 6, Col: 26, Mode: ModeNormal, Dirty: true, Cursor: true, Last: "cw",
	}
}

func TestModalEditorGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "modal_editor", vimFixture().Render(k, 80, 16))
	m := vimFixture()
	m.Mode, m.Cursor = ModeCommand, true
	m.Command = ":w"
	m.Diff = []ModalDiff{
		{Sign: "~", Key: "retention.ms", From: "7d", To: "3d"},
		{Sign: "+", Key: "max.message.bytes", To: "8.0 MiB"},
		{Sign: "-", Key: "min.insync.replicas", From: "2", To: "1"},
	}
	m.DiffTitle, m.DiffAsk = "3 changes to orders, checked by the cluster", "apply them? y applies · n keeps editing"
	golden(t, "modal_editor_diff", m.Render(k, 80, 16))
	out := kit.Join(m.Render(k, 80, 16))
	for _, want := range []string{"7d → 3d", "→ 1 (unset)", "apply them?", "COMMAND", ":w"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q:\n%s", want, out)
		}
	}
	m = vimFixture()
	m.Mode, m.Msg, m.MsgRole = ModeInsert, "", kit.OK
	if out := kit.Join(m.Render(k, 80, 16)); !strings.Contains(out, "-- INSERT --") || !strings.Contains(out, "[+]") || !strings.Contains(out, "~") {
		t.Errorf("insert:\n%s", out)
	}
}
