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
			"OK": th.OK, "Warn": th.Warn, "Err": th.Err, "Info": th.Info, "Selection": th.Selection, "Rule": th.Rule,
		} {
			if _, _, _, ok := parseHex(hex); !ok {
				t.Errorf("%s.%s = %q, not #rrggbb", name, role, hex)
			}
		}
		if len(th.Heat) < 2 {
			t.Errorf("%s: heat ramp too short", name)
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
