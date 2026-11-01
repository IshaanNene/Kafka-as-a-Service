package produce

import (
	"strings"
	"testing"
)

// FuzzParseLine: a key and value split from a line always join back into
// it, so no byte a producer pipes in is lost or moved.
func FuzzParseLine(f *testing.F) {
	f.Add("k:v", ":")
	f.Add("no separator", "\t")
	f.Add("a::b", "::")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, line, sep string) {
		r, err := ParseLine(line, sep)
		switch {
		case err != nil:
			if sep == "" || strings.Contains(line, sep) {
				t.Fatalf("%q split on %q: %v", line, sep, err)
			}
		case sep == "":
			if string(r.Value) != line || r.Key != nil {
				t.Fatalf("%q read as %q/%q", line, r.Key, r.Value)
			}
		default:
			if string(r.Key)+sep+string(r.Value) != line || strings.Contains(string(r.Key), sep) {
				t.Fatalf("%q split on %q as %q, %q", line, sep, r.Key, r.Value)
			}
		}
	})
}
