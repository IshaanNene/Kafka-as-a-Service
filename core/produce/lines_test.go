package produce

import (
	"errors"
	"strings"
	"testing"
)

func TestEachLine(t *testing.T) {
	in := "a:1\r\nb:2\n\nc:3"
	var keys, values []string
	var reads []int64
	err := EachLine(strings.NewReader(in), ":", func(n int, r Record, read int64) error {
		keys, values, reads = append(keys, string(r.Key)), append(values, string(r.Value)), append(reads, read)
		return nil
	})
	// The blank line has no separator: an error naming line 3.
	if err == nil || err.Error() != `line 3: no ":" between key and value` {
		t.Fatalf("err = %v", err)
	}
	if strings.Join(keys, ",") != "a,b" || strings.Join(values, ",") != "1,2" || reads[0] != 5 || reads[1] != 9 {
		t.Errorf("keys %q values %q reads %v", keys, values, reads)
	}

	// Without a separator every line is a value; the last needs no newline
	// and the reads end at the size.
	var n int
	var last int64
	if err := EachLine(strings.NewReader(in), "", func(_ int, _ Record, read int64) error { n++; last = read; return nil }); err != nil || n != 4 || last != int64(len(in)) {
		t.Errorf("plain: %d lines, last read %d, err %v", n, last, err)
	}

	stop := errors.New("stop")
	if err := EachLine(strings.NewReader(in), "", func(int, Record, int64) error { return stop }); !errors.Is(err, stop) {
		t.Errorf("fn's error = %v", err)
	}
	if err := EachLine(strings.NewReader(""), "", func(int, Record, int64) error { t.Error("called on empty input"); return nil }); err != nil {
		t.Error(err)
	}
	long := strings.Repeat("x", MaxLine+1)
	if err := EachLine(strings.NewReader(long), "", func(int, Record, int64) error { return nil }); err == nil {
		t.Error("an over-long line should fail")
	}
}
