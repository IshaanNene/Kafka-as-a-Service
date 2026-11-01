package quotas

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestApplyList(t *testing.T) {
	_, cl := kafkatest.Fake(t)
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user := Quota{User: "billing", Values: map[string]float64{"producer_byte_rate": 1 << 20, "request_percentage": 25}}
	def := Quota{DefaultClientID: true, Values: map[string]float64{"consumer_byte_rate": 5 << 20}}
	if err := Apply(ctx, adm, []Change{{Quota: user}, {Quota: def}}, true); err != nil {
		t.Fatal(err)
	}
	if qs, _ := List(ctx, adm); len(qs) != 0 {
		t.Fatalf("validate changed quotas: %+v", qs)
	}
	if err := Apply(ctx, adm, []Change{{Quota: user}, {Quota: def}}, false); err != nil {
		t.Fatal(err)
	}
	qs, err := List(ctx, adm)
	if err != nil || len(qs) != 2 {
		t.Fatalf("list: %+v %v", qs, err)
	}
	if qs[0].Entity() != "client-id=<default>" || qs[1].Entity() != "user=billing" || qs[1].Values["request_percentage"] != 25 {
		t.Fatalf("listed: %+v", qs)
	}
	if err := Apply(ctx, adm, []Change{{Quota: Quota{User: "billing"}, Remove: []string{"request_percentage"}}}, false); err != nil {
		t.Fatal(err)
	}
	qs, _ = List(ctx, adm)
	if _, still := qs[1].Values["request_percentage"]; still || len(qs[1].Values) != 1 {
		t.Fatalf("after remove: %+v", qs[1])
	}
}

func TestCheck(t *testing.T) {
	for _, bad := range []Quota{{}, {User: "a", IP: "1.2.3.4"}, {User: "a", Values: map[string]float64{"speed": 1}}} {
		if Check(bad) == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
	if err := Check(Quota{User: "a", ClientID: "c", Values: map[string]float64{"producer_byte_rate": 1}}); err != nil {
		t.Error(err)
	}
}
