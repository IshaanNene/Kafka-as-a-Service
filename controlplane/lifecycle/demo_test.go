package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

// run steps d a second at a time for dur, from *now.
func run(d *Demo, now *time.Time, dur time.Duration) {
	for end := now.Add(dur); now.Before(end); {
		*now = now.Add(time.Second)
		d.Step(*now)
	}
}

func service(t *testing.T, d *Demo, name string) Service {
	t.Helper()
	all, _ := d.Services(context.Background())
	for _, s := range all {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no service %s", name)
	return Service{}
}

func TestDemoLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	d := NewDemo(now)
	d.Now = func() time.Time { return now }
	ctx := context.Background()

	if s := service(t, d, "edge-staging"); s.Phase != Provisioning {
		t.Errorf("edge-staging starts %s, want provisioning", s.Phase)
	}
	if s := service(t, d, "analytics"); s.Phase != Rolling || s.Roll == nil {
		t.Errorf("analytics starts %s with roll %v, want rolling", s.Phase, s.Roll)
	}
	run(d, &now, time.Minute)
	if s := service(t, d, "edge-staging"); s.Phase != Ready || s.Brokers.Ready != 3 || s.History[0].Kind != "created" {
		t.Errorf("edge-staging after a minute: %s, %d ready, history %v", s.Phase, s.Brokers.Ready, s.History)
	}
	if s := service(t, d, "analytics"); s.Phase != Ready || s.History[0].Kind != "rolled" {
		t.Errorf("analytics after a minute: %s, history %v", s.Phase, s.History[0])
	}

	// Create, then roll and resize it.
	if err := d.Create(ctx, Spec{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"}); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(ctx, Spec{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"}); !errors.Is(err, ErrExists) {
		t.Errorf("second create: %v", err)
	}
	if err := d.Roll(ctx, "orders"); !errors.Is(err, ErrBusy) {
		t.Errorf("rolling a cluster still provisioning: %v, want ErrBusy", err)
	}
	run(d, &now, 30*time.Second)
	if err := d.Roll(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	run(d, &now, 40*time.Second)
	if s := service(t, d, "orders"); s.Phase != Ready || s.Roll != nil {
		t.Fatalf("orders after its roll: %s, %v", s.Phase, s.Roll)
	}
	if err := d.Scale(ctx, "orders", 5); err != nil {
		t.Fatal(err)
	}
	if s := service(t, d, "orders"); s.Phase != Scaling {
		t.Errorf("orders scaling up is %s", s.Phase)
	}
	run(d, &now, 30*time.Second)
	if s := service(t, d, "orders"); s.Phase != Ready || len(s.Brokers.Nodes) != 5 || s.Brokers.Ready != 5 {
		t.Errorf("orders after scaling to 5: %s, %d nodes, %d ready", s.Phase, len(s.Brokers.Nodes), s.Brokers.Ready)
	}
	if err := d.Scale(ctx, "orders", 2); err != nil {
		t.Fatal(err)
	}
	run(d, &now, 30*time.Second)
	if s := service(t, d, "orders"); s.Phase != Ready || len(s.Brokers.Nodes) != 2 {
		t.Errorf("orders after scaling to 2: %s, %d nodes", s.Phase, len(s.Brokers.Nodes))
	}
}

// The autoscaled demo cluster follows its load up and down.
func TestDemoAutoscale(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	d := NewDemo(now)
	d.Now = func() time.Time { return now }
	sizes := map[int]bool{}
	for range 15 * 60 {
		now = now.Add(time.Second)
		d.Step(now)
		s := service(t, d, "payments")
		if s.Phase == Ready {
			sizes[len(s.Brokers.Nodes)] = true
		}
		if n := len(s.Brokers.Nodes); n < 3 || n > 9 {
			t.Fatalf("payments has %d brokers, outside 3..9", n)
		}
	}
	if len(sizes) < 3 {
		t.Errorf("payments only ran at sizes %v over 15 minutes of a rising and falling load", sizes)
	}
}
