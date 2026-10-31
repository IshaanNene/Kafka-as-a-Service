package theme

import "testing"

func TestBuiltinsComplete(t *testing.T) {
	for _, name := range Names() {
		th, err := Get(name)
		if err != nil {
			t.Fatal(err)
		}
		for role, hex := range map[string]string{
			"BG": th.BG, "FG": th.FG, "Muted": th.Muted, "Accent": th.Accent, "Accent2": th.Accent2,
			"OK": th.OK, "Warn": th.Warn, "Err": th.Err, "Info": th.Info, "Selection": th.Selection, "Rule": th.Rule, "Faint": th.Faint,
		} {
			if _, _, _, ok := parseHex(hex); !ok {
				t.Errorf("%s.%s = %q, not #rrggbb", name, role, hex)
			}
		}
		for ramp, hexes := range map[string][]string{"Heat": th.Heat, "Chart": th.Chart, "Hot": th.Hot, "Cool": th.Cool} {
			if len(hexes) < 2 {
				t.Errorf("%s: %s ramp too short", name, ramp)
			}
			for _, hex := range hexes {
				if _, _, _, ok := parseHex(hex); !ok {
					t.Errorf("%s.%s has %q, not #rrggbb", name, ramp, hex)
				}
			}
		}
	}
	if _, err := Get("nope"); err == nil {
		t.Error("unknown theme should error")
	}
}

func TestTintFollowsBrightness(t *testing.T) {
	th, _ := Get("amber")
	if got := th.Tint("#000000"); got != th.Heat[0] {
		t.Errorf("black -> %s, want darkest heat %s", got, th.Heat[0])
	}
	if got := th.Tint("#ffffff"); got != th.Heat[len(th.Heat)-1] {
		t.Errorf("white -> %s, want brightest heat", got)
	}
	if got := th.Tint("not-a-colour"); got != th.FG {
		t.Errorf("bad hex -> %s, want FG", got)
	}
}

func TestMagicKeepsArtColours(t *testing.T) {
	th, _ := Get("magic")
	if got := th.Tint("#ffa657"); got != "#ffa657" {
		t.Errorf("magic recoloured #ffa657 to %s; it should keep the piece's own colour", got)
	}
}

func TestRampSpansEnds(t *testing.T) {
	r := []string{"#000000", "#111111", "#222222"}
	if Ramp(r, 0, 5) != r[0] || Ramp(r, 4, 5) != r[2] {
		t.Errorf("ramp ends: %s .. %s", Ramp(r, 0, 5), Ramp(r, 4, 5))
	}
}
