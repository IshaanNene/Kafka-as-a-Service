package configs

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// FuzzParseFile: whatever is typed into kaas config edit, parsing never
// panics, and what it reads writes back out and reads the same again.
func FuzzParseFile(f *testing.F) {
	f.Add("[retention]\nretention.ms = 7d  # a week\ncleanup.policy = delete\n")
	f.Add("# only a comment\n\n")
	f.Add("a = b\na = c\n")
	f.Add("=\nkey with space = 1\n")
	f.Fuzz(func(t *testing.T, text string) {
		got, err := ParseFile(text)
		if err != nil {
			var le LineError
			if !errors.As(err, &le) {
				t.Fatalf("%T: %v", err, err)
			}
			return
		}
		keys := make([]string, 0, len(got))
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s = %s\n", k, got[k])
		}
		again, err := ParseFile(sb.String())
		if err != nil {
			t.Fatalf("written back %q: %v", sb.String(), err)
		}
		for k, v := range got {
			if again[k] != v {
				t.Fatalf("%s = %q, then %q", k, v, again[k])
			}
		}
	})
}
