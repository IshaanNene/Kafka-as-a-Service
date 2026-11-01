package pipe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// Contract tests pin the formats other programs depend on, so a change
// that would break them fails here, the way a schema registry refuses an
// incompatible schema. kaas has no registry of its own; these stand in
// for the JSON every surface reads and writes.

// TestRecordFormatV1 reads lines written in the first version of the
// record format, as kaas consume -o json, pipe files and plugins write
// it. A later kaas must keep reading them: fields may be added, never
// renamed or reinterpreted, and unknown fields are ignored.
func TestRecordFormatV1(t *testing.T) {
	b, err := os.ReadFile("testdata/contract/records-v1.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	rs, err := Decode(bytes.NewReader(b), FormatJSONL)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Key: []byte("c-1"), Value: []byte(`{"amount":3}`), Partition: 0, Offset: 12,
			Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Headers: []Header{{Key: "trace", Value: "ab"}}},
		{Key: []byte{0xff}, Value: []byte{0, 1}, Partition: 1, Offset: 4, Time: time.Date(2026, 1, 2, 3, 4, 6, 5e8, time.UTC)},
		{Key: nil, Value: nil},
		{Value: []byte("only a value")},
		{Topic: "orders", Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte("v")},
	}
	if len(rs) != len(want) {
		t.Fatalf("%d records, want %d", len(rs), len(want))
	}
	for i, w := range want {
		g := rs[i]
		if !bytes.Equal(g.Key, w.Key) || (g.Key == nil) != (w.Key == nil) || !bytes.Equal(g.Value, w.Value) || (g.Value == nil) != (w.Value == nil) ||
			g.Partition != w.Partition || g.Offset != w.Offset || !g.Time.Equal(w.Time) || g.Topic != w.Topic || len(g.Headers) != len(w.Headers) {
			t.Errorf("line %d: %+v\nwant %+v", i+1, g, w)
		}
	}
}

// TestRecordFormatFields pins the names a written record uses.
func TestRecordFormatFields(t *testing.T) {
	b, _ := json.Marshal(ToJSON(Record{Topic: "t", Key: []byte("k"), Value: []byte{0xff},
		Time: time.Unix(0, 0).UTC(), Headers: []Header{{Key: "h", Value: "v"}}}))
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"topic", "partition", "offset", "timestamp", "key", "value", "encoding", "headers"} {
		if _, ok := m[f]; !ok {
			t.Errorf("no %q in %s", f, b)
		}
	}
	if m["encoding"] != "base64" || m["key"] != "aw==" {
		t.Errorf("a binary value base64s the key too: %s", b)
	}
}

// TestPluginProtocol pins what kaas writes to a sink plugin and reads
// back, and what it reads from a source plugin, byte for byte where it
// matters.
func TestPluginProtocol(t *testing.T) {
	line, _ := json.Marshal(pluginLine{RecordJSON: ToJSON(Record{Key: []byte("k"), Value: []byte("v")}), Seq: 3})
	if want := `{"key":"k","value":"v","seq":3}`; string(line) != want {
		t.Errorf("to a sink plugin:\n%s\nwant\n%s", line, want)
	}
	for in, want := range map[string]string{
		`{"ack":3}`:                          "ack 3",
		`{"error":"quota","seq":2}`:          "error quota",
		`{"id":"a1","key":"k","value":"v"}`:  `record "a1" k=v`,
		`{"id":7,"value":"v","headers":[]}`:  "record 7 =v",
		`{"value":"v","something_new":true}`: "record  =v",
	} {
		var l pluginLine
		if err := json.Unmarshal([]byte(in), &l); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		got := ""
		switch {
		case l.Ack != nil:
			got = fmt.Sprint("ack ", *l.Ack)
		case l.Error != "":
			got = "error " + l.Error
		default:
			r, _ := FromJSON(l.RecordJSON)
			got = "record " + string(l.ID) + " " + string(r.Key) + "=" + string(r.Value)
		}
		if got != want {
			t.Errorf("%s read as %q, want %q", in, got, want)
		}
	}
}

// TestGenContract pins each generator's fields and their JSON types:
// demos, load tests and filters written against them keep working.
func TestGenContract(t *testing.T) {
	for name, fields := range map[string]map[string]string{
		"orders":   {"order_id": "string", "customer": "string", "amount": "number", "currency": "string", "items": "number", "status": "string"},
		"clicks":   {"user": "string", "session": "string", "page": "string", "ms": "number", "device": "string"},
		"payments": {"payment_id": "string", "order_id": "string", "amount": "number", "method": "string", "ok": "bool"},
		"iot":      {"device": "string", "temp": "number", "humidity": "number", "battery": "number"},
	} {
		g, err := OpenSource("gen:"+name+"?count=50&seed=9", Env{})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := g.Source.Next(context.Background())
		for _, r := range b.Records {
			var m map[string]any
			if err := json.Unmarshal(r.Value, &m); err != nil {
				t.Fatalf("%s: %s", name, r.Value)
			}
			if len(m) != len(fields) {
				t.Errorf("%s has fields %v, want %v", name, m, fields)
			}
			for f, typ := range fields {
				got := "missing"
				switch m[f].(type) {
				case string:
					got = "string"
				case float64:
					got = "number"
				case bool:
					got = "bool"
				}
				if got != typ {
					t.Errorf("%s.%s is %s, want %s", name, f, got, typ)
				}
			}
		}
	}
}
