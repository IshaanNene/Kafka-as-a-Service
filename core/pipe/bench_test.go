package pipe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// Benchmarks give the cost of each stage a record passes through, so a
// change that slows one shows in benchstat before it shows in production.
//
//	go test ./core/pipe -run '^$' -bench . -benchmem

type nopSink struct{}

func (nopSink) Write(context.Context, []Record) error { return nil }
func (nopSink) Close() error                          { return nil }

func benchRecords(n int) []Record {
	rs := make([]Record, n)
	for i := range rs {
		rs[i] = Record{Key: []byte(fmt.Sprintf("c-%04d", i%5000)),
			Value: []byte(fmt.Sprintf(`{"order_id":"o-%08d","amount":%d.50,"status":"placed","items":3}`, i, i%500))}
	}
	return rs
}

// BenchmarkPipe is the engine alone: batches through to a sink that does
// nothing, with and without a filter.
func BenchmarkPipe(b *testing.B) {
	rs := benchRecords(500)
	where, _ := NewFilter(`value.amount > 100 && value.status == "placed"`)
	for _, c := range []struct {
		name  string
		where *Filter
	}{{"all", nil}, {"where", where}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			batches := make([][]Record, b.N)
			for i := range batches {
				batches[i] = rs
			}
			b.ResetTimer()
			p := New([]Endpoint{{Spec: "mem", Source: &memSource{batches: batches}}}, []Endpoint{{Spec: "nop", Sink: nopSink{}}}, Options{Where: c.where})
			if err := p.Run(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N*len(rs))/b.Elapsed().Seconds(), "records/s")
		})
	}
}

func BenchmarkFilter(b *testing.B) {
	f, _ := NewFilter(`value.amount > 100 && value.status == "placed"`)
	r := benchRecords(1)[0]
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.Match(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec(b *testing.B) {
	rs := benchRecords(1000)
	for _, format := range []string{FormatJSONL, FormatCSV} {
		var buf bytes.Buffer
		e, _ := newEncoder(&buf, format)
		_ = e.write(rs)
		data := buf.Bytes()
		b.Run("encode/"+format, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				e, _ := newEncoder(io.Discard, format)
				if err := e.write(rs); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("decode/"+format, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if _, err := Decode(bytes.NewReader(data), format); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGen(b *testing.B) {
	g, _ := OpenSource("gen:orders?seed=1&batch=500", Env{})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := g.Source.Next(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*500)/b.Elapsed().Seconds(), "records/s")
}

// BenchmarkKafkaCopy is a whole topic-to-topic copy on kfake: the client
// and the pipe together, without a network or a disk.
func BenchmarkKafkaCopy(b *testing.B) {
	c, _ := kafkatest.Fake(b, kfake.NumBrokers(1), kfake.SeedTopics(6, "src"))
	seeds := c.ListenAddrs()
	seed(b, seeds, "src", 20_000)
	env := Env{Brokers: seeds}
	b.ResetTimer()
	for i := range b.N {
		in, _ := OpenSource("kafka:src?stop=end", env)
		out, _ := OpenSink(fmt.Sprintf("kafka:dst-%d", i), env)
		if err := c.CreateTopic(fmt.Sprintf("dst-%d", i), 6, nil); err != nil {
			b.Fatal(err)
		}
		if err := New([]Endpoint{in}, []Endpoint{out}, Options{}).Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*20_000)/b.Elapsed().Seconds(), "records/s")
}
