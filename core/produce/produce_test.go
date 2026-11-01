package produce

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestParse(t *testing.T) {
	if h, err := ParseHeader("trace= a=b"); err != nil || h != (Header{Key: "trace", Value: " a=b"}) {
		t.Errorf("ParseHeader = %+v, %v", h, err)
	}
	for _, bad := range []string{"nokey", "=v", " =v"} {
		if _, err := ParseHeader(bad); err == nil {
			t.Errorf("ParseHeader(%q) should fail", bad)
		}
	}
	hs, line, err := ParseHeaders("trace=1\n\nsource=api\n")
	if err != nil || len(hs) != 2 || hs[1].Value != "api" {
		t.Errorf("ParseHeaders = %+v, %d, %v", hs, line, err)
	}
	if _, line, err := ParseHeaders("a=1\noops"); err == nil || line != 2 {
		t.Errorf("bad second line: line %d, %v", line, err)
	}

	if r, err := ParseLine("user-1:{\"a\":1}", ":"); err != nil || string(r.Key) != "user-1" || string(r.Value) != `{"a":1}` || r.Partition != -1 {
		t.Errorf("ParseLine with sep = %+v, %v", r, err)
	}
	if r, _ := ParseLine("a:b", ""); r.Key != nil || string(r.Value) != "a:b" {
		t.Errorf("ParseLine without sep = %+v", r)
	}
	if _, err := ParseLine("no separator", ":"); err == nil {
		t.Error("a line missing the separator should fail")
	}
}

func TestCheckJSON(t *testing.T) {
	for _, ok := range []string{"", "  \n", `{"a": [1, true, null]}`, "[]\n"} {
		if p := CheckJSON(ok); p != nil {
			t.Errorf("CheckJSON(%q) = %+v, want nil", ok, p)
		}
	}
	for text, want := range map[string][2]int{
		"{\n  \"a\": 1,\n  \"b\": }":   {3, 8},
		"{\n  \"a\": 1\n  \"b\": 2\n}": {3, 3},
		"{\"a\": 1} {":                 {1, 10},
		"{\n  \"a\": ":                 {2, 7},
	} {
		p := CheckJSON(text)
		if p == nil || p.Line != want[0] || p.Col != want[1] {
			t.Errorf("CheckJSON(%q) = %+v, want line %d col %d", text, p, want[0], want[1])
		}
	}
	if !LooksJSON("  {") || LooksJSON("hello") {
		t.Error("LooksJSON")
	}
	if got := string(Compact("{\n  \"a\": 1\n}")); got != `{"a":1}` {
		t.Errorf("Compact = %s", got)
	}
	if got := string(Compact("not json\n")); got != "not json\n" {
		t.Errorf("Compact kept non-JSON as is: %q", got)
	}
}

func TestSend(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(3, "orders"))
	cl := kafkatest.Client(t, c.ListenAddrs()...)
	manual, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer manual.Close()
	ctx := context.Background()
	res := Send(ctx, manual, "orders", []Record{
		{Value: []byte("a"), Partition: 2, Headers: []Header{{Key: "trace", Value: "1"}}},
		{Value: []byte("b"), Partition: 2},
	})
	if res[0].Err != nil || res[0].Partition != 2 || res[0].Offset != 0 || res[1].Offset != 1 || res[0].Timestamp.IsZero() {
		t.Errorf("manual results = %+v", res)
	}
	// Acks from different partitions arrive in any order; each result
	// must still be its own record's.
	var spread []Record
	for i := range 30 {
		spread = append(spread, Record{Value: []byte("s"), Partition: int32(2 - i%3)})
	}
	for i, r := range Send(ctx, manual, "orders", spread) {
		if r.Err != nil || r.Partition != spread[i].Partition {
			t.Fatalf("result %d = %+v, want partition %d", i, r, spread[i].Partition)
		}
	}
	res = Send(ctx, cl, "nope", []Record{{Value: []byte("x"), Partition: -1}})
	if res[0].Err == nil || res[0].Partition != -1 {
		t.Errorf("unknown topic should fail: %+v", res)
	}
}
