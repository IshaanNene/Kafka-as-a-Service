package engine

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Budget caps the admin requests the engine sends to one cluster. It is a
// token bucket: a refresh that cannot pay is skipped, and the data it would
// have refreshed simply gets older. The engine never queues or retries past
// the budget, so a slow or overloaded cluster sees less load, not more.
type Budget struct {
	lim     *rate.Limiter
	granted atomic.Uint64
	denied  atomic.Uint64
}

// NewBudget allows perSecond requests on average with bursts up to burst.
func NewBudget(perSecond float64, burst int) *Budget {
	return &Budget{lim: rate.NewLimiter(rate.Limit(perSecond), burst)}
}

// Take spends n tokens if available and reports whether it did.
func (b *Budget) Take(n int) bool {
	if b.lim.AllowN(time.Now(), n) {
		b.granted.Add(uint64(n))
		return true
	}
	b.denied.Add(uint64(n))
	return false
}

// BudgetStats feeds the debug overlay.
type BudgetStats struct {
	Tokens  float64 // currently available
	Granted uint64  // requests paid for
	Denied  uint64  // requests skipped for lack of tokens
}

func (b *Budget) Stats() BudgetStats {
	return BudgetStats{
		Tokens:  b.lim.TokensAt(time.Now()),
		Granted: b.granted.Load(),
		Denied:  b.denied.Load(),
	}
}
