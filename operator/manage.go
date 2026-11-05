package operator

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
)

// Manager is lifecycle.Manager over the KafkaServices in one namespace.
// It only writes specs; the operator, wherever it runs, does the rest.
type Manager struct {
	Client    client.Client
	Namespace string
	// Context names the Kubernetes context, for the console's header.
	Context string
	Now     func() time.Time
}

var _ lifecycle.Manager = (*Manager)(nil)

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Where says which namespace, in which context.
func (m *Manager) Where() string {
	if m.Context == "" {
		return "namespace " + m.Namespace
	}
	return m.Context + " · namespace " + m.Namespace
}

// Services lists the namespace's KafkaServices.
func (m *Manager) Services(ctx context.Context) ([]lifecycle.Service, error) {
	var list KafkaServiceList
	if err := m.Client.List(ctx, &list, client.InNamespace(m.Namespace)); err != nil {
		return nil, err
	}
	out := make([]lifecycle.Service, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, View(&list.Items[i]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Create adds a KafkaService.
func (m *Manager) Create(ctx context.Context, s lifecycle.Spec) error {
	if err := s.Validate(); err != nil {
		return err
	}
	ks := &KafkaService{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "KafkaService"},
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: m.Namespace},
		Spec:       KafkaServiceSpec{Version: s.Version, Brokers: int32(s.Brokers), Storage: s.Storage, Autoscale: fromPolicy(s.Autoscale)},
	}
	err := m.Client.Create(ctx, ks)
	if apierrors.IsAlreadyExists(err) {
		return lifecycle.ErrExists
	}
	return err
}

// Roll asks for every broker to restart.
func (m *Manager) Roll(ctx context.Context, name string) error {
	return m.edit(ctx, name, func(ks *KafkaService) error {
		if ks.Status.Roll != nil {
			return fmt.Errorf("%w: already restarting", lifecycle.ErrBusy)
		}
		ks.Spec.RestartAt = &metav1.Time{Time: m.now()}
		return nil
	})
}

// Scale sets the broker count and turns autoscaling off.
func (m *Manager) Scale(ctx context.Context, name string, brokers int) error {
	if err := lifecycle.ValidateScale(brokers); err != nil {
		return err
	}
	return m.edit(ctx, name, func(ks *KafkaService) error {
		ks.Spec.Brokers, ks.Spec.Autoscale = int32(brokers), nil
		return nil
	})
}

// SetAutoscale turns autoscaling on with p, or off when p is nil. Off,
// the cluster keeps the size autoscaling last chose.
func (m *Manager) SetAutoscale(ctx context.Context, name string, p *lifecycle.Autoscale) error {
	if p != nil {
		if err := p.Validate(); err != nil {
			return err
		}
	}
	return m.edit(ctx, name, func(ks *KafkaService) error {
		if p == nil && ks.Spec.Autoscale != nil && ks.Status.Brokers > 0 {
			ks.Spec.Brokers = ks.Status.Brokers
		}
		ks.Spec.Autoscale = fromPolicy(p)
		return nil
	})
}

// edit changes a KafkaService's spec, retrying if someone else wrote it
// in between.
func (m *Manager) edit(ctx context.Context, name string, f func(*KafkaService) error) error {
	for range 5 {
		ks := &KafkaService{}
		err := m.Client.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: name}, ks)
		if apierrors.IsNotFound(err) {
			return lifecycle.ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := f(ks); err != nil {
			return err
		}
		err = m.Client.Update(ctx, ks)
		if !apierrors.IsConflict(err) {
			return err
		}
	}
	return fmt.Errorf("%w: kept changing while being edited", lifecycle.ErrBusy)
}

// View is a KafkaService as the console shows it.
func View(ks *KafkaService) lifecycle.Service {
	st := ks.Status
	s := lifecycle.Service{
		Name: ks.Name, Namespace: ks.Namespace,
		Phase: lifecycle.Phase(st.Phase), Message: st.Message,
		Version: ks.Spec.Version, Storage: ks.Spec.Storage, Bootstrap: st.Bootstrap,
		Created:     ks.CreationTimestamp.Time,
		Brokers:     lifecycle.Brokers{Desired: int(st.Brokers), Ready: int(st.ReadyBrokers), Nodes: []lifecycle.Node{}},
		Load:        []lifecycle.Sample{},
		History:     []lifecycle.Event{},
		Recommended: int(st.Recommended), Reason: st.Reason,
	}
	if s.Phase == "" {
		s.Phase, s.Message = lifecycle.Provisioning, "waiting for the operator"
	}
	if s.Brokers.Desired == 0 {
		s.Brokers.Desired = int(ks.Spec.Brokers)
	}
	if a := ks.Spec.Autoscale; a != nil {
		p := toPolicy(a)
		s.Auto = &p
	}
	if st.Roll != nil {
		s.Roll = &lifecycle.Progress{Done: int(st.Roll.Done), Total: int(st.Roll.Total)}
	}
	for _, n := range st.Nodes {
		s.Brokers.Nodes = append(s.Brokers.Nodes, lifecycle.Node{ID: int(n.ID), Ready: n.Ready, State: n.State})
	}
	for _, l := range st.Load {
		s.Load = append(s.Load, lifecycle.Sample{T: l.T, V: float64(l.V)})
	}
	for _, h := range st.History {
		s.History = append(s.History, lifecycle.Event{At: h.At.Time, Kind: h.Kind, Detail: h.Detail})
	}
	return s
}

func fromPolicy(p *lifecycle.Autoscale) *AutoscaleSpec {
	if p == nil {
		return nil
	}
	return &AutoscaleSpec{Min: int32(p.Min), Max: int32(p.Max), Target: int32(p.Target), ScaleDownAfter: metav1.Duration{Duration: p.ScaleDownAfter.Duration}}
}
