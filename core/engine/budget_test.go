package engine

import "testing"

func TestBudget(t *testing.T) {
	b := NewBudget(0.001, 5) // effectively no refill during the test
	if !b.Take(3) {
		t.Fatal("first take should fit the burst")
	}
	if b.Take(3) {
		t.Fatal("second take should exceed the remaining 2 tokens")
	}
	if !b.Take(2) {
		t.Fatal("remaining 2 tokens should be spendable")
	}
	s := b.Stats()
	if s.Granted != 5 || s.Denied != 3 {
		t.Errorf("stats = %+v, want granted 5 denied 3", s)
	}
	if s.Tokens >= 1 {
		t.Errorf("tokens = %v, want < 1", s.Tokens)
	}
}
