package bench

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduce(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(3, "bench"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, cs := range Matrix([]int{100}, []string{"1", "all"}, []string{"none", "lz4"}, 500, 0) {
		t.Run(cs.Name(), func(t *testing.T) {
			var ticks int
			r, err := Produce(context.Background(), "bench", cs, Options{
				Tick:   5 * time.Millisecond,
				Report: func(Progress) { ticks++ },
				Client: []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)},
			})
			if err != nil {
				t.Fatal(err)
			}
			if r.Records != 500 || r.Errors != 0 || r.Bytes != 50_000 {
				t.Errorf("records=%d errors=%d bytes=%d", r.Records, r.Errors, r.Bytes)
			}
			if r.P50 > r.P95 || r.P95 > r.P99 || r.P99 > r.P999 || r.P999 > r.Max || r.P50 <= 0 {
				t.Errorf("percentiles out of order: %+v", r)
			}
			var n int64
			for _, h := range r.Hist {
				n += h
			}
			if n != r.Records {
				t.Errorf("histogram holds %d of %d", n, r.Records)
			}
		})
	}
}

func TestThroughputCap(t *testing.T) {
	c, err := kfake.NewCluster(kfake.SeedTopics(1, "bench"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := Produce(context.Background(), "bench", Case{Size: 10, Acks: "all", Compression: "none", Records: 100, Throughput: 500},
		Options{Client: []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}})
	if err != nil {
		t.Fatal(err)
	}
	if r.RecsPerSec > 600 {
		t.Errorf("rate %.0f/s ignores the 500/s cap", r.RecsPerSec)
	}
}

func TestValidate(t *testing.T) {
	for _, tt := range []struct {
		c     Case
		field string
	}{
		{Case{Size: 1, Acks: "2", Compression: "none", Records: 1}, "acks"},
		{Case{Size: 1, Acks: "all", Compression: "brotli", Records: 1}, "compression"},
		{Case{Size: 0, Acks: "all", Compression: "none", Records: 1}, "size"},
	} {
		if f, _ := tt.c.Validate(); f != tt.field {
			t.Errorf("%+v: field %q, want %q", tt.c, f, tt.field)
		}
	}
}

func TestBytes(t *testing.T) {
	for n, want := range map[int64]string{100: "100B", 1024: "1KB", 1536: "1.5KB", 1 << 20: "1MB"} {
		if got := Bytes(n); got != want {
			t.Errorf("Bytes(%d) = %s, want %s", n, got, want)
		}
	}
}
