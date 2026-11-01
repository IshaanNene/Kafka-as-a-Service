package configs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestDescribe(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	ctx := context.Background()
	adm := kadm.NewClient(cl)
	day, gzip := "86400000", "gzip"
	if _, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Name: "retention.ms", Value: &day}}, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := adm.AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Name: "compression.type", Value: &gzip}}, 0); err != nil {
		t.Fatal(err)
	}
	es, err := Describe(ctx, cl, "orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Entry{}
	for _, e := range es {
		got[e.Key] = e
	}
	ret, comp := got["retention.ms"], got["compression.type"]
	if ret.Value != day || ret.Level != LevelTopic || !ret.HasDef || ret.Default != "604800000" {
		t.Errorf("retention.ms %+v", ret)
	}
	if comp.Value != "gzip" || comp.Level != LevelBroker || !comp.Set() {
		t.Errorf("compression.type %+v", comp)
	}
	if d, forever, ok := Duration(ret); !ok || forever || d != 24*time.Hour {
		t.Errorf("duration %v %v %v", d, forever, ok)
	}
	secs := Sections(es, false)
	if secs[0].Name != "Retention" || secs[len(secs)-1].Name != "Other" && len(secs) < 3 {
		t.Errorf("sections %+v", secs)
	}
	if _, err := Describe(ctx, cl, "nope", nil); !errors.Is(err, ErrNoTopic) {
		t.Errorf("unknown topic: %v", err)
	}
	b := int32(0)
	bs, err := Describe(ctx, cl, "", &b)
	if err != nil || len(bs) == 0 {
		t.Fatalf("broker: %v, %d", err, len(bs))
	}
	for _, e := range bs {
		if e.Key == "super.users" && (!e.Sensitive || !e.Null) {
			t.Errorf("super.users %+v", e)
		}
	}
}

func TestKinds(t *testing.T) {
	for _, tc := range []struct {
		e    Entry
		want Kind
	}{
		{Entry{Key: "unclean.leader.election.enable", Value: "false", Type: kmsg.ConfigTypeBoolean}, KindBool},
		{Entry{Key: "retention.ms", Value: "-1"}, KindDuration},
		{Entry{Key: "log.retention.hours", Value: "168"}, KindDuration},
		{Entry{Key: "segment.bytes", Value: "1073741824"}, KindBytes},
		{Entry{Key: "min.cleanable.dirty.ratio", Value: "0.5", Type: kmsg.ConfigTypeDouble}, KindRatio},
		{Entry{Key: "min.insync.replicas", Value: "2", Type: kmsg.ConfigTypeInt}, KindCount},
		{Entry{Key: "cleanup.policy", Value: "compact", Type: kmsg.ConfigTypeList}, KindText},
	} {
		if got := KindOf(tc.e); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.e.Key, got, tc.want)
		}
	}
	if d, _, _ := Duration(Entry{Key: "log.retention.hours", Value: "168"}); d != 7*24*time.Hour {
		t.Errorf("hours %v", d)
	}
	if _, forever, _ := Duration(Entry{Key: "retention.ms", Value: "-1"}); !forever {
		t.Error("forever")
	}
	if s := Summary(Entry{Key: "x.y", Doc: "Does a thing. More words."}); s != "Does a thing" {
		t.Errorf("summary %q", s)
	}
}
