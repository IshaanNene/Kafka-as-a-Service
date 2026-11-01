package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func uploadFixture() Upload {
	return Upload{
		Title: "Upload to orders", Right: "cluster 5L6g3nSh",
		Zone: "5 files · 24.6 MiB · each line becomes a record",
		Files: []UploadFile{
			{Name: "orders-2026-10.jsonl", Size: 12 << 20, Read: 12 << 20, Records: 48_211, State: Sent},
			{Name: "orders-2026-11.jsonl", Size: 9 << 20, Read: 5 << 20, Records: 20_112, Rate: 18_400, State: Sending},
			{Name: "refunds.csv", Size: 2 << 20, Read: 600 << 10, Records: 1_904, Rate: 6_100, State: Sending},
			{Name: "keys.txt", Size: 900 << 10, Read: 120 << 10, Records: 311, State: Failed, Err: `line 312: no ":" between key and value`},
			{Name: "late.jsonl", Size: 700 << 10, State: Queued},
		},
		Foot: "1 of 5 sent · 17.6 of 24.6 MiB · 70,538 records", Action: "ctrl-c to stop",
		Hint: "--key-separator ':' · 4 at a time",
	}
}

func TestUploadGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "upload", uploadFixture().Render(k, 72, 26))
	golden(t, "upload_narrow", uploadFixture().Render(k, 48, 26))

	// More files than fit: the busy ones stay, the rest are counted.
	many := uploadFixture()
	for range 6 {
		many.Files = append(many.Files, UploadFile{Name: "done.jsonl", Size: 1, Read: 1, State: Sent})
	}
	out := kit.Join(many.Render(k, 72, 20))
	if !strings.Contains(out, "keys.txt") || !strings.Contains(out, "orders-2026-11") || !strings.Contains(out, "… ") || !strings.Contains(out, "sent") {
		t.Errorf("overflow should keep the failed and sending files and count the rest:\n%s", out)
	}

	golden(t, "tray", Tray{Count: " 5 files ", Title: "Upload complete", Detail: "orders-2026-10.jsonl and 4 more · 24.6 MiB · 81,300 records",
		Action: "kaas consume orders →"}.Render(k, 56))
	golden(t, "tray_failed", Tray{Count: " 4 of 5 ", Title: "1 file failed", Detail: `keys.txt line 312: no ":" between key and value`,
		Action: "fix it and send keys.txt again", Failed: true}.Render(k, 56))
}
