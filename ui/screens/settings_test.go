package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func settingsFixture() Settings {
	return Settings{
		Title: "Config · topic orders",
		Badge: "✓ 2 set on the topic · 1 from brokers",
		Rows: []SettingRow{
			{Heading: "Retention"},
			{Key: "retention.ms", Detail: "How long records are kept", Control: SettingSlider, Pos: 0.42, Value: "1d", Set: true, Tag: "topic", TagRole: kit.OK},
			{Key: "retention.bytes", Detail: "Bytes kept per partition before deleting", Control: SettingSlider, Pos: 1, Value: "unlimited"},
			{Key: "delete.retention.ms", Detail: "How long compaction keeps delete tombstones", Control: SettingSlider, Pos: 0.35, Value: "1d"},
			{Heading: "Cleanup"},
			{Key: "cleanup.policy", Detail: "Delete old segments, compact by key, or both", Value: "compact,delete", Set: true, Tag: "topic", TagRole: kit.OK},
			{Key: "min.cleanable.dirty.ratio", Detail: "Share of the log uncompacted before cleaning", Control: SettingSlider, Pos: 0.5, Value: "0.5"},
			{Heading: "Messages"},
			{Key: "compression.type", Detail: "Codec the broker stores batches in", Value: "zstd", Set: true, Tag: "broker", TagRole: kit.Accent2},
			{Key: "message.downconversion.enable", Detail: "Convert batches for old consumers", Control: SettingToggle, On: true},
			{Heading: "Replication"},
			{Key: "min.insync.replicas", Detail: "Replicas that must have a write for acks=all", Control: SettingSlider, Pos: 0.5, Value: "2 of 3", Set: true, Tag: "topic", TagRole: kit.OK},
			{Key: "unclean.leader.election.enable", Detail: "Let an out-of-sync replica lead, losing data", Control: SettingToggle},
		},
		Sel:  1,
		Foot: []string{"retention.ms = 86400000 (1d) · set on the topic · default 7d", "How long records are kept before the log deletes them."},
		Keys: "↑↓ move · s set only · q quit",
	}
}

func TestSettingsGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "settings", settingsFixture().Render(k, 100, 24))
	s := settingsFixture()
	s.Sel = 12
	golden(t, "settings_scrolled", s.Render(k, 72, 14))
	out := kit.Join(s.Render(k, 72, 14))
	if !strings.Contains(out, "unclean.leader") || strings.Contains(out, "RETENTION") {
		t.Errorf("scrolled to the end:\n%s", out)
	}
	if out := kit.Join((Settings{Title: "Config", Empty: "nothing set"}).Render(k, 80, 12)); !strings.Contains(out, "nothing set") {
		t.Errorf("empty:\n%s", out)
	}
}
