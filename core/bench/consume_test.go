package bench

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsume(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(3, "bench"), kfake.SeedTopics(1, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	seeds := []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}
	if _, err := Produce(context.Background(), "bench", Case{Size: 100, Acks: "all", Compression: "none", Records: 900}, Options{Client: seeds}); err != nil {
		t.Fatal(err)
	}
	// 900 plus produce's warm-up record.
	for _, cs := range FetchMatrix([]int{1 << 10, 1 << 20}, []time.Duration{50 * time.Millisecond}, 600) {
		t.Run(cs.Name(), func(t *testing.T) {
			var last Progress
			r, err := Consume(context.Background(), "bench", cs, Options{Tick: time.Millisecond, Client: seeds,
				Report: func(p Progress) { last = p }})
			if err != nil {
				t.Fatal(err)
			}
			if r.Records != 600 || r.Bytes != 60_000 || r.Errors != 0 || r.Polls == 0 {
				t.Errorf("records=%d bytes=%d errors=%d polls=%d", r.Records, r.Bytes, r.Errors, r.Polls)
			}
			var n int64
			for _, h := range r.Hist {
				n += h
			}
			if n != r.Polls || r.P50 > r.Max || r.RecsPerSec <= 0 {
				t.Errorf("histogram %d of %d polls, %+v", n, r.Polls, r.Stats)
			}
			if last.Acked > 600 {
				t.Errorf("progress past the target: %+v", last)
			}
		})
	}

	r, err := Consume(context.Background(), "bench", FetchCase{FetchSize: 1 << 20, MaxWait: 10 * time.Millisecond, Records: 1 << 30},
		Options{Client: seeds})
	if err != nil || r.Records != 901 || r.Case.Records != 901 {
		t.Errorf("more than the topic holds should read it all: %d records, %v", r.Records, err)
	}
	if _, err := Consume(context.Background(), "empty", FetchCase{FetchSize: 1 << 20, MaxWait: time.Millisecond, Records: 1},
		Options{Client: seeds}); !errors.Is(err, ErrEmpty) {
		t.Errorf("empty topic: %v", err)
	}
	if f, _ := (FetchCase{FetchSize: 10, MaxWait: time.Second, Records: 1}).Validate(); f != "fetch-size" {
		t.Errorf("tiny fetch size passed validation")
	}
}
