package offsets

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestBisect(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	// p0: 1,000 records a second apart; values turn "v2" from offset 613.
	pc, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	var rs []*kgo.Record
	for i := range 1000 {
		v := "v1"
		if i >= 613 {
			v = "v2"
		}
		rs = append(rs, &kgo.Record{Topic: "orders", Partition: 0, Timestamp: base.Add(time.Duration(i) * time.Second), Value: []byte(v + ":" + strconv.Itoa(i))})
	}
	if err := pc.ProduceSync(context.Background(), rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	pc.Close()
	ctx := context.Background()
	r, err := NewReader(ctx, cl, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rec, err := r.At(ctx, 500); err != nil || rec == nil || rec.Offset != 500 {
		t.Fatalf("At(500) = %v, %v", rec, err)
	}
	if rec, err := r.At(ctx, 1000); err != nil || rec != nil {
		t.Fatalf("At(end) = %v, %v", rec, err)
	}

	steps := 0
	v2 := func(rec *kgo.Record) bool { return string(rec.Value[:2]) == "v2" }
	h, err := Bisect(ctx, r, 0, 1000, v2, func(Hunt) { steps++ })
	if err != nil || !h.Done || h.Found == nil || h.Found.Offset != 613 {
		t.Fatalf("v2 hunt: found %v, %v", h.Found, err)
	}
	if n := len(h.Probes); n > 11 || steps != 2*n+1 {
		t.Errorf("%d probes, %d steps", n, steps)
	}
	at := base.Add(250*time.Second + 500*time.Millisecond)
	h, _ = Bisect(ctx, r, 0, 1000, func(rec *kgo.Record) bool { return !rec.Timestamp.Before(at) }, nil)
	if h.Found == nil || h.Found.Offset != 251 {
		t.Errorf("time hunt found %v", h.Found)
	}
	// Nothing bad: none found; all bad: the first record.
	if h, _ = Bisect(ctx, r, 0, 1000, func(*kgo.Record) bool { return false }, nil); h.Found != nil || h.Lo != 1000 {
		t.Errorf("none bad: %+v", h)
	}
	if h, _ = Bisect(ctx, r, 0, 1000, func(*kgo.Record) bool { return true }, nil); h.Found == nil || h.Found.Offset != 0 {
		t.Errorf("all bad: %v", h.Found)
	}
	// An empty partition has nothing to hunt.
	r1, _ := NewReader(ctx, cl, "orders", 1)
	if h, err = Bisect(ctx, r1, 0, 0, v2, nil); err != nil || h.Found != nil || len(h.Probes) != 0 {
		t.Errorf("empty: %+v, %v", h, err)
	}
	if _, err := NewReader(ctx, cl, "nope", 0); !errors.Is(err, ErrNoTopic) {
		t.Errorf("unknown topic: %v", err)
	}
	if _, err := NewReader(ctx, cl, "orders", 9); err == nil {
		t.Error("unknown partition: no error")
	}
}
