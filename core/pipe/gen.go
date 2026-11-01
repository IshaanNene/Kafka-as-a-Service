package pipe

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"sort"
	"strconv"
	"time"
)

func init() {
	Register(Kind{
		Scheme:  "gen",
		Summary: "made-up data for load tests and demos: " + genNames(),
		Example: "gen:orders?rate=500 · gen:iot?count=10000&seed=7",
		Source:  openGen,
	})
}

// generators make the n-th record of a data set from r. Values are JSON
// a filter can reach into, keyed so records of one entity share a
// partition.
var generators = map[string]func(r *rand.Rand, n int64) (key string, value string){
	"orders": func(r *rand.Rand, n int64) (string, string) {
		c := fmt.Sprintf("c-%04d", r.IntN(5000))
		status := []string{"placed", "placed", "placed", "paid", "shipped", "cancelled"}[r.IntN(6)]
		return c, fmt.Sprintf(`{"order_id":"o-%08d","customer":%q,"amount":%.2f,"currency":%q,"items":%d,"status":%q}`,
			n, c, 5+r.Float64()*495, []string{"USD", "EUR", "GBP", "INR"}[r.IntN(4)], 1+r.IntN(9), status)
	},
	"clicks": func(r *rand.Rand, n int64) (string, string) {
		u := fmt.Sprintf("u-%05d", r.IntN(20000))
		page := []string{"/", "/search", "/product", "/cart", "/checkout", "/account"}[r.IntN(6)]
		return u, fmt.Sprintf(`{"user":%q,"session":"s-%06d","page":%q,"ms":%d,"device":%q}`,
			u, r.IntN(1e6), page, 20+r.IntN(4000), []string{"web", "ios", "android"}[r.IntN(3)])
	},
	"payments": func(r *rand.Rand, n int64) (string, string) {
		o := fmt.Sprintf("o-%08d", r.Int64N(max(n, 1)))
		return o, fmt.Sprintf(`{"payment_id":"p-%08d","order_id":%q,"amount":%.2f,"method":%q,"ok":%t}`,
			n, o, 5+r.Float64()*495, []string{"card", "card", "wallet", "bank"}[r.IntN(4)], r.IntN(50) != 0)
	},
	"iot": func(r *rand.Rand, n int64) (string, string) {
		d := fmt.Sprintf("d-%03d", r.IntN(500))
		return d, fmt.Sprintf(`{"device":%q,"temp":%.1f,"humidity":%d,"battery":%d}`,
			d, 15+r.Float64()*20, 20+r.IntN(70), r.IntN(101))
	},
}

func genNames() string {
	names := make([]string, 0, len(generators))
	for n := range generators {
		names = append(names, n)
	}
	sort.Strings(names)
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// genSource makes records at rate= a second (0 for as fast as the sinks
// take them), count= of them in all (0 for no end), the same ones for
// the same seed=.
type genSource struct {
	name  string
	make  func(*rand.Rand, int64) (string, string)
	r     *rand.Rand
	rate  float64
	count int64
	batch int
	n     int64
	start time.Time
	now   func() time.Time
}

func openGen(u *url.URL, _ Env) (Source, error) {
	name := pathOf(u)
	g, ok := generators[name]
	if !ok {
		return nil, fmt.Errorf("gen:%s: want one of %s", name, genNames())
	}
	q := u.Query()
	num := func(key string) (float64, error) {
		v := q.Get(key)
		if v == "" {
			return 0, nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 {
			return 0, fmt.Errorf("%s=%s: want a number, 0 or more", key, v)
		}
		return f, nil
	}
	rate, err := num("rate")
	if err != nil {
		return nil, err
	}
	count, err := num("count")
	if err != nil {
		return nil, err
	}
	seed, err := num("seed")
	if err != nil {
		return nil, err
	}
	if q.Get("seed") == "" {
		seed = float64(time.Now().UnixNano() % (1 << 40))
	}
	batch, err := batchSize(q.Get("batch"), 100)
	if err != nil {
		return nil, err
	}
	return &genSource{name: name, make: g, r: rand.New(rand.NewPCG(uint64(seed), 0x6b616173)), rate: rate, count: int64(count), batch: batch, now: time.Now}, nil
}

func (g *genSource) Next(ctx context.Context) (Batch, error) {
	if g.count > 0 && g.n >= g.count {
		return Batch{}, io.EOF
	}
	if g.start.IsZero() {
		g.start = g.now()
	}
	size := int64(g.batch)
	if g.count > 0 {
		size = min(size, g.count-g.n)
	}
	if g.rate > 0 {
		// Due: how many records the rate allows by now. Wait for the
		// next one when none are, and never send more than are due.
		due := int64(g.now().Sub(g.start).Seconds()*g.rate) + 1 - g.n
		if due <= 0 {
			at := g.start.Add(time.Duration(float64(g.n) / g.rate * float64(time.Second)))
			select {
			case <-ctx.Done():
				return Batch{}, ctx.Err()
			case <-time.After(time.Until(at)):
			}
			due = 1
		}
		size = min(size, due)
	}
	b := Batch{Records: make([]Record, size)}
	now := g.now()
	for i := range b.Records {
		k, v := g.make(g.r, g.n)
		b.Records[i] = Record{Key: []byte(k), Value: []byte(v), Time: now,
			Headers: []Header{{Key: "source", Value: "kaas-gen/" + g.name}}}
		g.n++
	}
	return b, nil
}

func (g *genSource) Close() error { return nil }
