package pipe

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// FuzzDecodeJSONL reads what a file, an HTTP body or a plugin might hold:
// never a panic, and whatever reads as records writes back out and reads
// as the same records.
func FuzzDecodeJSONL(f *testing.F) {
	f.Add(`{"key":"a","value":"1","headers":[{"key":"h","value":"v"}]}` + "\n")
	f.Add(`{"key":null,"value":"/w==","encoding":"base64","partition":3,"offset":9}`)
	f.Add(`{"value":"x","encoding":"rot13"}`)
	f.Add("\n\n{}\n")
	f.Add(`{"timestamp":"2026-01-02T03:04:05.123456789Z","value":""}`)
	f.Fuzz(func(t *testing.T, in string) {
		rs, err := Decode(strings.NewReader(in), FormatJSONL)
		if err != nil {
			return
		}
		var buf bytes.Buffer
		e, _ := newEncoder(&buf, FormatJSONL)
		if err := e.write(rs); err != nil {
			t.Fatal(err)
		}
		again, err := Decode(&buf, FormatJSONL)
		if err != nil {
			t.Fatalf("re-reading %q: %v", buf.String(), err)
		}
		if len(again) != len(rs) {
			t.Fatalf("%d records, then %d", len(rs), len(again))
		}
		for i := range rs {
			a, b := rs[i], again[i]
			if !bytes.Equal(a.Key, b.Key) || !bytes.Equal(a.Value, b.Value) || (a.Key == nil) != (b.Key == nil) ||
				(a.Value == nil) != (b.Value == nil) || !a.Time.Equal(b.Time) || len(a.Headers) != len(b.Headers) {
				t.Fatalf("record %d: %+v, then %+v", i, a, b)
			}
		}
	})
}

// FuzzCSVValues: any key and value survive a CSV file.
func FuzzCSVValues(f *testing.F) {
	f.Add("k", "plain")
	f.Add("", "with, \"quotes\"\nand a newline")
	f.Fuzz(func(t *testing.T, key, value string) {
		if strings.ContainsRune(key+value, '\r') {
			t.Skip("CSV turns \\r\\n into \\n inside quotes, by RFC 4180")
		}
		var buf bytes.Buffer
		e, _ := newEncoder(&buf, FormatCSV)
		at := time.Unix(1700000000, 0).UTC()
		if err := e.write([]Record{{Key: []byte(key), Value: []byte(value), Time: at}}); err != nil {
			t.Fatal(err)
		}
		rs, err := Decode(&buf, FormatCSV)
		if err != nil || len(rs) != 1 {
			t.Fatalf("%v, %d records from %q", err, len(rs), buf.String())
		}
		if string(rs[0].Key) != key || string(rs[0].Value) != value || !rs[0].Time.Equal(at) {
			t.Fatalf("%q/%q came back %q/%q", key, value, rs[0].Key, rs[0].Value)
		}
	})
}

// FuzzFilter: a filter over any record answers or errors; it never
// panics or runs away, whatever the value holds.
func FuzzFilter(f *testing.F) {
	f.Add(`{"amount": 5, "tags": ["a"]}`, "k", "region", "eu")
	f.Add(`[1, [2, [3, [4]]]]`, "", "", "")
	f.Add(`not json`, "\xff", "h", "\x00")
	filters := []*Filter{}
	for _, e := range []string{
		`value.amount > 100`,
		`"a" in value.tags && headers["region"] == "eu"`,
		`size(raw) < 10 || key.startsWith("k")`,
		`value.exists(x, x == 3)`,
	} {
		fl, err := NewFilter(e)
		if err != nil {
			f.Fatal(err)
		}
		filters = append(filters, fl)
	}
	f.Fuzz(func(t *testing.T, value, key, hk, hv string) {
		r := Record{Key: []byte(key), Value: []byte(value), Headers: []Header{{Key: hk, Value: hv}}}
		for _, fl := range filters {
			_, _ = fl.Match(r)
		}
	})
}
