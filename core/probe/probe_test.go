package probe

import (
	"context"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestReachable(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(1, "orders"))
	var reported int
	r := Run(context.Background(), c.ListenAddrs()[:1], Options{Report: func(Step) { reported++ }})
	if !r.OK() || r.Err() != nil {
		t.Fatalf("probe failed: %v", r.Err())
	}
	if r.Brokers != 3 || r.Topics != 1 || r.Version == "" {
		t.Errorf("result = %+v", r)
	}
	if reported != len(r.Steps) || len(r.Steps) != 4 {
		t.Errorf("reported %d of %d steps, want 4", reported, len(r.Steps))
	}
}

func TestRefusedStopsAtTCP(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // nothing listens here now
	r := Run(context.Background(), []string{addr}, Options{})
	if r.OK() {
		t.Fatal("probe of a closed port succeeded")
	}
	steps := r.Seed(addr)
	if !steps[DNS].OK() || steps[TCP].OK() {
		t.Errorf("want dns ok, tcp failed: %+v", r.Steps)
	}
	if _, ok := steps[API]; ok {
		t.Error("api ran after tcp failed")
	}
}

func TestNotKafka(t *testing.T) {
	// Accepts and immediately closes: TCP passes, the protocol does not.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	r := Run(context.Background(), []string{l.Addr().String()}, Options{})
	steps := r.Seed(l.Addr().String())
	if !steps[TCP].OK() || steps[API].OK() || r.OK() {
		t.Errorf("want tcp ok, api failed: %+v", r.Steps)
	}
}
