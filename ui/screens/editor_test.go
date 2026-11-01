package screens

import (
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func editorFixture() Editor {
	return Editor{
		Title: "orders · kaas produce",
		Tabs:  []EditorTab{{Name: "value", Dirty: true}, {Name: "key"}, {Name: "headers"}},
		Path:  "orders / value",
		Lines: []string{
			"{",
			`  "order_id": "A-1043",`,
			`  "customer": "user-17",`,
			`  "items": [{"sku": "B-220", "qty": 2}],`,
			`  "total": 49.90,`,
			`  "gift": false,`,
			`  "note": null`,
			"}",
		},
		Line: 4, Col: 12, Cursor: true, Lang: LangJSON, Branch: "orders",
		Fine: "valid JSON", Notice: "↑ sent to p2 @ 1,043",
		Hint: "ctrl-s send · tab key, headers · ctrl-c quit",
	}
}

func TestEditor(t *testing.T) {
	k, c := kits(t)
	golden(t, "editor", editorFixture().Render(k, 100, 18))
	golden(t, "editor_narrow", editorFixture().Render(k, 50, 18))
	bad := editorFixture()
	bad.Lines[4] = `  "total": ,`
	bad.Fine, bad.Notice, bad.Problem, bad.ProblemLine = "", "", "line 5, col 12: looking for beginning of value", 5
	golden(t, "editor_problem", bad.Render(k, 100, 18))
	for w := 40; w <= 200; w++ {
		for i, l := range editorFixture().Render(c, w, 20) {
			if n := kit.Width(l); n > w {
				t.Fatalf("width %d line %d is %d wide: %q", w, i, n, l)
			}
		}
	}
	// The cursor past the right edge shifts the view so it stays visible.
	long := editorFixture()
	long.Lines[1] = `  "order_id": "` + strings.Repeat("x", 120) + `",`
	long.Line, long.Col = 1, len([]rune(long.Lines[1]))
	if out := kit.Join(long.Render(k, 80, 18)); !strings.Contains(out, "▌") {
		t.Errorf("cursor scrolled out of view\n%s", out)
	}
}

func TestHighlight(t *testing.T) {
	got := highlight([]rune(`"k": "v", 12, true, nope`), LangJSON)
	want := map[int]paintColour{0: {role: kit.Accent2}, 5: {hex: strColour}, 10: {role: kit.Accent}, 14: {role: kit.Accent}, 20: {role: kit.Err}, 3: {role: kit.Muted}}
	for i, c := range want {
		if got[i] != c {
			t.Errorf("rune %d = %+v, want %+v", i, got[i], c)
		}
	}
	h := highlight([]rune("trace=abc"), LangHeaders)
	if h[0].role != kit.Accent2 || h[5].role != kit.Err || h[6].hex != strColour {
		t.Errorf("headers = %+v", h)
	}
}
