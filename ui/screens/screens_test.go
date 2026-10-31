package screens

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

var update = flag.Bool("update", false, "rewrite golden files")

func kits(t *testing.T) (plain, color kit.Kit) {
	th, err := theme.Get("amber")
	if err != nil {
		t.Fatal(err)
	}
	return kit.New(th, false), kit.New(th, true)
}

func golden(t *testing.T, name string, b []string) {
	t.Helper()
	got := kit.Join(b)
	p := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("%v (run go test ./ui/screens -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs from golden; rerun with -update if intended\n--- got ---\n%s", name, got)
	}
}

var help = Help{
	Path: "kaas watch", Version: "v0.2.0", Usage: "kaas watch [flags]",
	Short: "Stream cluster events: leader moves, ISR changes, topics created and deleted",
	Flags: []Item{
		{Name: "-t, --topic strings", Desc: "topics to watch closely (repeatable)"},
		{Name: "-o, --output string", Desc: "output format: text|json", Hint: "text"},
		{Name: "    --budget float", Desc: "requests per second", Hint: "20"},
	},
	Global:   []Item{{Name: "-b, --brokers string", Desc: "comma-separated seed brokers", Hint: "localhost:9092"}},
	Examples: []Example{{Comment: "Follow two topics", Cmd: "kaas watch -t orders -t payments"}, {Cmd: "kaas watch -o json | jq .event"}},
	Prompt:   "dev@box: ~/src",
}

func refused() probe.Result {
	return probe.Result{
		Seeds: []string{"localhost:9092", "kafka-1:9092"},
		Took:  3 * time.Millisecond,
		Steps: []probe.Step{
			{Seed: "localhost:9092", Stage: probe.DNS, Detail: "localhost → 127.0.0.1", Took: 400 * time.Microsecond},
			{Seed: "localhost:9092", Stage: probe.TCP, Err: errors.New("connect: connection refused"), Took: time.Millisecond},
			{Seed: "kafka-1:9092", Stage: probe.DNS, Err: errors.New("no such host"), Took: time.Millisecond},
		},
	}
}

func TestGoldens(t *testing.T) {
	k, _ := kits(t)
	golden(t, "help", help.Render(k, 80))
	golden(t, "unreachable", Unreachable(refused()).Render(k, 100))
	golden(t, "unreachable_narrow", Unreachable(refused()).Render(k, 60))
	golden(t, "not_found", NotFound{Kind: "command", Name: "brokrs", Where: "kaas",
		Suggestions: []Item{{Name: "brokers", Desc: "List brokers"}}, Actions: []string{"kaas help"}}.Render(k, 80))
	golden(t, "invalid", Invalid{Title: "kaas snapshot", Action: "kaas snapshot --help", Fields: []Field{
		{Label: "--brokers", Value: "localhost:9092", State: kit.Good},
		{Label: "--output", Value: "yaml", State: kit.Degraded, Focus: true, Msg: `must be one of table, json`},
	}}.Render(k, 80))
	var boot []string
	r := refused()
	boot = append(boot, BootStart(k, r.Seeds)...)
	for _, s := range r.Steps {
		boot = append(boot, BootStep(k, s)...)
	}
	golden(t, "boot", append(boot, BootDone(k, r)...))
}

// TestFits renders every screen, coloured, at a range of widths and checks
// none spills past the terminal.
func TestFits(t *testing.T) {
	_, k := kits(t)
	screens := map[string]func(w int) []string{
		"help":        func(w int) []string { return help.Render(k, w) },
		"unreachable": func(w int) []string { return Unreachable(refused()).Render(k, w) },
		"not_found": func(w int) []string {
			return NotFound{Kind: "topic", Name: strings.Repeat("x", 90), Where: "cluster", Actions: []string{"kaas snapshot"}}.Render(k, w)
		},
		"invalid": func(w int) []string {
			return Invalid{Title: "kaas", Action: "kaas --help", Fields: []Field{{Label: "--x", Value: strings.Repeat("v", 90), State: kit.Degraded, Msg: "bad"}}}.Render(k, w)
		},
	}
	for name, render := range screens {
		for _, w := range []int{40, 60, 80, 120, 200} {
			for i, l := range render(w) {
				if n := kit.Width(l); n > w {
					t.Errorf("%s at %d: line %d is %d wide", name, w, i, n)
					break
				}
			}
		}
	}
}

func TestUnreachableSkipsStagesNeverReached(t *testing.T) {
	f := Unreachable(refused())
	got := map[string]string{}
	for _, p := range f.Pills {
		got[p.Label] = p.State
	}
	if got["dns"] != "1/2" || got["tcp"] != "down" || got["api"] != "skipped" || got["metadata"] != "skipped" {
		t.Errorf("pills = %v", got)
	}
}
