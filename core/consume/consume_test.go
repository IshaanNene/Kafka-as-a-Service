package consume

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestParseFrom(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for in, want := range map[string]string{
		"": "the end", "end": "the end", "beginning": "the beginning", "earliest": "the beginning",
		"-10": "10 before the end", "15m": "records since 11:45:00", "2026-10-31T09:30:00Z": "records since 09:30:00",
	} {
		f, err := ParseFrom(in, now)
		if err != nil || f.String() != want {
			t.Errorf("ParseFrom(%q) = %q, %v; want %q", in, f, err, want)
		}
	}
	for _, bad := range []string{"-0", "-x", "yesterday", "-5m"} {
		if _, err := ParseFrom(bad, now); err == nil {
			t.Errorf("ParseFrom(%q) should fail", bad)
		}
	}
}

// seed writes n records to each of orders' two partitions.
func seed(t *testing.T, n int) (*kfake.Cluster, *kadm.Client) {
	t.Helper()
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := range 2 * n {
		r := &kgo.Record{Topic: "orders", Partition: int32(i % 2), Key: []byte(fmt.Sprint("k", i)), Value: []byte(fmt.Sprint("v", i))}
		if err := prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	return c, kadm.NewClient(cl)
}

func TestPlanAndRead(t *testing.T) {
	c, adm := seed(t, 10)
	ctx := context.Background()
	now := time.Now()

	plan := func(from string, parts ...int32) []Start {
		t.Helper()
		f, _ := ParseFrom(from, now)
		p, err := Plan(ctx, adm, "orders", parts, f)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := plan("end"); len(p) != 2 || p[0].Offset != 10 || p[0].End != 10 {
		t.Errorf("from end: %+v", p)
	}
	if p := plan("-3", 1); len(p) != 1 || p[0].Partition != 1 || p[0].Offset != 7 {
		t.Errorf("-3 on partition 1: %+v", p)
	}
	if p := plan("-50"); p[0].Offset != 0 {
		t.Errorf("-50 should clamp to the log start: %+v", p)
	}
	if p := plan("1h"); p[0].Offset != 0 {
		t.Errorf("an hour ago is before every record: %+v", p)
	}
	if _, err := Plan(ctx, adm, "orders", []int32{7}, From{}); err == nil {
		t.Error("partition 7 does not exist")
	}

	read := func(o Options) []Record {
		t.Helper()
		o.Seeds, o.Topic = c.ListenAddrs(), "orders"
		var got []Record
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := Read(rctx, o, nil, func(r Record) error { got = append(got, r); return nil }); err != nil {
			t.Fatal(err)
		}
		if rctx.Err() != nil {
			t.Fatal("read did not stop on its own")
		}
		return got
	}
	if got := read(Options{Plan: plan("beginning"), Exit: true}); len(got) != 20 {
		t.Errorf("--from beginning --exit read %d records, want 20", len(got))
	}
	got := read(Options{Plan: plan("-2", 0), Exit: true})
	if len(got) != 2 || got[0].Offset != 8 || string(got[1].Value) != "v18" || got[0].Partition != 0 {
		t.Errorf("-2 on partition 0: %+v", got)
	}
	if got := read(Options{Plan: plan("beginning"), Max: 5}); len(got) != 5 {
		t.Errorf("--max 5 read %d", len(got))
	}
	if got := read(Options{Plan: plan("end"), Exit: true}); len(got) != 0 {
		t.Errorf("from the end with --exit reads nothing, got %d", len(got))
	}
}

func TestText(t *testing.T) {
	if s, ok := Text([]byte(`{"id":1}`)); !ok || s != `{"id":1}` {
		t.Errorf("json: %q %v", s, ok)
	}
	if s, ok := Text([]byte{0xff, 0x00, 0x12}); ok || s != "<3 bytes, binary>" {
		t.Errorf("binary: %q %v", s, ok)
	}
	if _, ok := Text([]byte("bell\x07")); ok {
		t.Error("control characters are binary")
	}
}

func TestWindow(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(2, "orders"))
	ctx := context.Background()
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	pc, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	var rs []*kgo.Record
	for p := range int32(2) {
		for i := range 60 { // a record a minute on each partition
			rs = append(rs, &kgo.Record{Topic: "orders", Partition: p, Timestamp: base.Add(time.Duration(i) * time.Minute), Value: []byte(strconv.Itoa(i))})
		}
	}
	if err := pc.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	pc.Close()
	adm := kadm.NewClient(cl)

	// 10:00 to 10:20 past base: offsets 10 to 19 on each partition.
	f := From{kind: fromTime, since: base.Add(10 * time.Minute), now: base}.Until(base.Add(20 * time.Minute))
	plan, err := Plan(ctx, adm, "orders", nil, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 2 || plan[0].Offset != 10 || plan[0].Stop != 20 || plan[1].Stop != 20 {
		t.Fatalf("plan = %+v", plan)
	}
	var got []int64
	err = Read(ctx, Options{Seeds: c.ListenAddrs(), Topic: "orders", Plan: plan}, nil, func(r Record) error {
		got = append(got, r.Offset)
		return nil
	})
	if err != nil || len(got) != 20 || slices.Max(got) != 19 || slices.Min(got) != 10 {
		t.Errorf("read %v, %v; want offsets 10-19 on both partitions", got, err)
	}
	// A window with nothing in it ends at once.
	empty, _ := Plan(ctx, adm, "orders", nil, From{kind: fromTime, since: base.Add(90 * time.Minute)}.Until(base.Add(95*time.Minute)))
	if err := Read(ctx, Options{Seeds: c.ListenAddrs(), Topic: "orders", Plan: empty}, nil, func(Record) error { t.Error("read a record"); return nil }); err != nil {
		t.Error(err)
	}

	counts, err := Counts(ctx, adm, "orders", []time.Time{base, base.Add(30 * time.Minute), base.Add(3 * time.Hour)})
	if err != nil || counts[0] != 0 || counts[1] != 60 || counts[2] != 120 {
		t.Errorf("counts = %v, %v", counts, err)
	}
	if old, err := Oldest(ctx, cl, adm, "orders"); err != nil || !old.Equal(base) {
		t.Errorf("oldest = %v, %v; want %v", old, err, base)
	}

	if _, err := ParseUntil("1h", time.Now()); err != nil {
		t.Error(err)
	}
	if _, err := ParseUntil(time.Now().Add(time.Hour).Format(time.RFC3339), time.Now()); err == nil {
		t.Error("a future --to was taken")
	}
}
