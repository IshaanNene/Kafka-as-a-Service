package operator

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaMeter measures load by asking the brokers for every partition's
// high-water mark: one ListOffsets request per broker per measurement,
// whatever the partition count, so it costs the cluster next to nothing.
type KafkaMeter struct {
	mu      sync.Mutex
	clients map[string]*kadm.Client
}

// Offsets sums the high-water marks of every partition outside Kafka's
// own topics (__consumer_offsets and the like), whose writes aren't load
// anyone produced.
func (m *KafkaMeter) Offsets(ctx context.Context, bootstrap string) (int64, error) {
	adm, err := m.client(bootstrap)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	offs, err := adm.ListEndOffsets(ctx)
	if err != nil {
		return 0, err
	}
	var sum int64
	offs.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && !strings.HasPrefix(o.Topic, "__") {
			sum += o.Offset
		}
	})
	return sum, nil
}

func (m *KafkaMeter) client(bootstrap string) (*kadm.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[bootstrap]; ok {
		return c, nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ClientID("kaas-operator"))
	if err != nil {
		return nil, err
	}
	if m.clients == nil {
		m.clients = map[string]*kadm.Client{}
	}
	m.clients[bootstrap] = kadm.NewClient(cl)
	return m.clients[bootstrap], nil
}

// Close closes every connection.
func (m *KafkaMeter) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.clients {
		c.Close()
	}
	m.clients = nil
}
