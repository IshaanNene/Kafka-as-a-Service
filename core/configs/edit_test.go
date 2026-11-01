package configs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestFileRoundTrip(t *testing.T) {
	es := []Entry{
		{Key: "retention.ms", Value: "259200000", Level: LevelTopic, Default: "604800000", HasDef: true},
		{Key: "cleanup.policy", Value: "delete"},
		{Key: "compression.type", Value: "zstd", Level: LevelBroker},
		{Key: "secret", Sensitive: true, Null: true, Level: LevelTopic},
		{Key: "max.message.bytes", Value: "1048588"},
		{Key: "segment.ms", Value: "604800000"},
		{Key: "log.dirs", Value: "/data", ReadOnly: true},
	}
	pad := func(k string) string { return fmt.Sprintf("%-17s", k) } // the longest key's width
	f := File("kaas config edit · topic orders", es, false, LevelTopic, 3)
	for _, want := range []string{
		"[retention]\n" + pad("retention.ms") + " = 259200000  # 3d · default 7d\n",
		"# " + pad("cleanup.policy") + " = delete\n",
		"# " + pad("compression.type") + " = zstd  # from a broker override\n",
	} {
		if !strings.Contains(f, want) {
			t.Errorf("no %q in:\n%s", want, f)
		}
	}
	if strings.Contains(f, "secret") {
		t.Errorf("sensitive config written:\n%s", f)
	}
	got, err := ParseFile(f)
	if err != nil || len(got) != 1 || got["retention.ms"] != "259200000" {
		t.Fatalf("parsed %v, %v", got, err)
	}
	// Unchanged: nothing to do; the sensitive one isn't unset.
	if cs, err := Diff(es, LevelTopic, got); err != nil || len(cs) != 0 {
		t.Errorf("unchanged diff %+v %v", cs, err)
	}
	// Change one, set one, drop one, in readable units.
	edited := strings.Replace(f, pad("retention.ms")+" = 259200000", "# retention.ms = 259200000", 1)
	edited = strings.Replace(edited, "# "+pad("cleanup.policy")+" = delete", "cleanup.policy = compact", 1)
	edited += "max.message.bytes = 8MiB\nsegment.ms = 12h\n"
	got, err = ParseFile(edited)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := Diff(es, LevelTopic, got)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 4 || cs[0].Key != "cleanup.policy" || cs[0].From != "delete" || cs[0].To != "compact" ||
		cs[1].Key != "max.message.bytes" || cs[1].To != "8388608" || cs[1].From != "1048588" ||
		cs[2].Key != "retention.ms" || cs[2].Op != OpUnset || cs[2].To != "604800000" ||
		cs[3].To != "43200000" {
		t.Errorf("diff %+v", cs)
	}

	for _, bad := range []string{"retention.ms\n", "a b = 1\n", "x = 1\nx = 2\n"} {
		var le LineError
		if _, err := ParseFile(bad); !errors.As(err, &le) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	for want, bad := range map[string]map[string]string{
		"duration":                 {"retention.ms": "soon"},
		"did you mean segment.ms?": {"segmnet.ms": "1"},
		"read-only":                {"log.dirs": "/x"},
	} {
		if _, err := Diff(es, LevelTopic, bad); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: %v, want %q", bad, err, want)
		}
	}
	for in, want := range map[string]string{"3d": "259200000", "1w": "604800000", "forever": "-1", "90s": "90000", "-1": "-1"} {
		if got, err := Normalize("retention.ms", in); err != nil || got != want {
			t.Errorf("retention.ms %s = %s, %v", in, got, err)
		}
	}
	if got, _ := Normalize("log.retention.hours", "2d"); got != "48" {
		t.Errorf("hours %s", got)
	}
	if _, err := Normalize("log.retention.hours", "90m"); err == nil {
		t.Error("90m is not whole hours")
	}
	if got, _ := Normalize("segment.bytes", "1GiB"); got != "1073741824" {
		t.Errorf("bytes %s", got)
	}
}

func TestApply(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	ctx := context.Background()
	adm := kadm.NewClient(cl)
	cs := []Change{{Key: "retention.ms", Op: OpSet, To: "3600000"}}
	if err := Apply(ctx, adm, "orders", nil, cs, true); err != nil {
		t.Fatal(err)
	}
	es, _ := Describe(ctx, cl, "orders", nil)
	for _, e := range es {
		if e.Key == "retention.ms" && e.Level == LevelTopic {
			t.Fatal("validate changed it")
		}
	}
	if err := Apply(ctx, adm, "orders", nil, cs, false); err != nil {
		t.Fatal(err)
	}
	es, _ = Describe(ctx, cl, "orders", nil)
	found := false
	for _, e := range es {
		found = found || e.Key == "retention.ms" && e.Level == LevelTopic && e.Value == "3600000"
	}
	if !found {
		t.Fatal("not applied")
	}
	if err := Apply(ctx, adm, "orders", nil, []Change{{Key: "retention.ms", Op: OpUnset}}, false); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, adm, "nope", nil, cs, false); err == nil {
		t.Error("an unknown topic was altered")
	}
}
