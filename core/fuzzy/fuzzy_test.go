package fuzzy

import "testing"

func TestSuggest(t *testing.T) {
	got := Suggest("oders", []string{"orders", "orders-v2", "payments", "topics"}, 3)
	if len(got) == 0 || got[0] != "orders" {
		t.Errorf("Suggest = %v, want orders first", got)
	}
	if got := Suggest("zzzzzz", []string{"orders"}, 3); len(got) != 0 {
		t.Errorf("Suggest far = %v, want none", got)
	}
}
