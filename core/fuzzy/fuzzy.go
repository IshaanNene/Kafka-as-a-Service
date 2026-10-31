// Package fuzzy finds the names a typo most likely meant.
package fuzzy

import (
	"sort"
	"strings"
)

// Suggest returns up to n candidates closest to q by edit distance, best
// first, dropping any too far away to be a plausible typo.
func Suggest(q string, candidates []string, n int) []string {
	q = strings.ToLower(q)
	type scored struct {
		s string
		d int
	}
	var out []scored
	for _, c := range candidates {
		lc := strings.ToLower(c)
		d := levenshtein(q, lc)
		if strings.Contains(lc, q) || strings.Contains(q, lc) {
			d = min(d, 1) // a substring hit is nearly an exact match
		}
		if d <= max(2, len(q)/3) {
			out = append(out, scored{c, d})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].d != out[j].d {
			return out[i].d < out[j].d
		}
		return out[i].s < out[j].s
	})
	res := make([]string, 0, n)
	for i := 0; i < len(out) && i < n; i++ {
		res = append(res, out[i].s)
	}
	return res
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}
