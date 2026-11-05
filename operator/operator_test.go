package operator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
)

const ns = "kafka"

// world is a fake API server, a clock and a meter whose high-water mark
// grows at whatever rate the test sets.
type world struct {
	t     *testing.T
	c     client.Client
	r     *Reconciler
	m     *Manager
	now   time.Time
	rate  int64 // records a second
	total int64
}

func (w *world) Offsets(context.Context, string) (int64, error) { return w.total, nil }

func newWorld(t *testing.T) *world {
	w := &world{t: t, now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	w.c = fake.NewClientBuilder().WithScheme(Scheme()).WithStatusSubresource(&KafkaService{}).Build()
	clock := func() time.Time { return w.now }
	w.r = &Reconciler{Client: w.c, Scheme: Scheme(), Meter: w, Interval: 30 * time.Second, Now: clock}
	w.m = &Manager{Client: w.c, Namespace: ns, Now: clock}
	return w
}

// tick moves the clock on, adds the records produced meanwhile and
// reconciles name.
func (w *world) tick(name string, d time.Duration) {
	w.t.Helper()
	w.now = w.now.Add(d)
	w.total += w.rate * int64(d/time.Second)
	if _, err := w.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) service(name string) *KafkaService {
	w.t.Helper()
	ks := &KafkaService{}
	if err := w.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, ks); err != nil {
		w.t.Fatal(err)
	}
	return ks
}

func (w *world) object(kind string, name string) *unstructured.Unstructured {
	w.t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(map[string]schema.GroupVersionKind{"Kafka": KafkaGVK, "KafkaNodePool": NodePoolGVK, "KafkaRebalance": RebalanceGVK}[kind])
	if err := w.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, u); err != nil {
		w.t.Fatalf("%s %s: %v", kind, name, err)
	}
	return u
}

// strimzi plays Strimzi: a ready Kafka and a running pod for every
// controller and broker the pools ask for, created at the current time.
func (w *world) strimzi(name string) {
	w.t.Helper()
	ctx := context.Background()
	k := w.object("Kafka", name)
	_ = unstructured.SetNestedSlice(k.Object, []any{map[string]any{"type": "Ready", "status": "True"}}, "status", "conditions")
	_ = unstructured.SetNestedSlice(k.Object, []any{map[string]any{"name": "plain", "bootstrapServers": name + "-kafka-bootstrap.kafka.svc:9092"}}, "status", "listeners")
	var pods corev1.PodList
	_ = w.c.List(ctx, &pods, client.InNamespace(ns))
	have := map[string]bool{}
	for _, p := range pods.Items {
		have[p.Name] = true
	}
	id, moved := 0, false
	for _, pool := range []string{name + "-controllers", name + "-brokers"} {
		replicas, _, _ := unstructured.NestedInt64(w.object("KafkaNodePool", pool).Object, "spec", "replicas")
		for i := 0; i < int(replicas); i++ {
			pod := fmt.Sprintf("%s-%s-%d", name, pool, id)
			id++
			if have[pod] {
				delete(have, pod)
				continue
			}
			w.pod(name, pool, pod)
			moved = true
		}
	}
	for pod := range have { // scaled down
		_ = w.c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: pod}})
		moved = true
	}
	// Brokers came or went: Strimzi's auto-rebalance moved partitions and
	// is idle again, a moment later.
	if moved {
		_ = unstructured.SetNestedMap(k.Object, map[string]any{
			"state": "Idle", "lastTransitionTime": w.now.Add(time.Second).Format(time.RFC3339Nano),
		}, "status", "autoRebalance")
	}
	if err := w.c.Update(ctx, k); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) pod(cluster, pool, name string) {
	w.t.Helper()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, CreationTimestamp: metav1.NewTime(w.now),
			Labels: map[string]string{clusterLabel: cluster, poolLabel: pool},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	if err := w.c.Create(context.Background(), p); err != nil {
		w.t.Fatal(err)
	}
}

func TestCreate(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	if err := w.m.Create(ctx, lifecycle.Spec{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"}); err != nil {
		t.Fatal(err)
	}
	if err := w.m.Create(ctx, lifecycle.Spec{Name: "orders", Version: "4.2.0", Brokers: 3, Storage: "100Gi"}); !errors.Is(err, lifecycle.ErrExists) {
		t.Errorf("creating twice: %v, want ErrExists", err)
	}
	w.tick("orders", time.Second)

	k := w.object("Kafka", "orders")
	if v, _, _ := unstructured.NestedString(k.Object, "spec", "kafka", "version"); v != "4.2.0" {
		t.Errorf("Kafka version %q", v)
	}
	if rf, _, _ := unstructured.NestedString(k.Object, "spec", "kafka", "config", "min.insync.replicas"); rf != "2" {
		t.Errorf("min.insync.replicas %q on three brokers, want 2", rf)
	}
	if refs := k.GetOwnerReferences(); len(refs) != 1 || refs[0].Name != "orders" {
		t.Errorf("Kafka owner references %v, want the service", refs)
	}
	brokers := w.object("KafkaNodePool", "orders-brokers")
	if n, _, _ := unstructured.NestedInt64(brokers.Object, "spec", "replicas"); n != 3 {
		t.Errorf("broker pool has %d replicas, want 3", n)
	}
	if n, _, _ := unstructured.NestedInt64(w.object("KafkaNodePool", "orders-controllers").Object, "spec", "replicas"); n != Controllers {
		t.Errorf("controller pool has %d replicas", n)
	}
	if a := w.object("KafkaRebalance", "orders-rebalance").GetAnnotations(); a["strimzi.io/rebalance-template"] != "true" {
		t.Errorf("rebalance template annotations %v", a)
	}
	if s := w.service("orders"); s.Status.Phase != "provisioning" {
		t.Errorf("phase %q before Strimzi is ready, want provisioning", s.Status.Phase)
	}

	// A second pass with nothing changed writes nothing.
	rv := brokers.GetResourceVersion()
	w.tick("orders", time.Second)
	if w.object("KafkaNodePool", "orders-brokers").GetResourceVersion() != rv {
		t.Error("a steady reconcile rewrote the broker pool")
	}

	w.strimzi("orders")
	w.tick("orders", time.Second)
	s := w.service("orders")
	if s.Status.Phase != "ready" || s.Status.ReadyBrokers != 3 || len(s.Status.Nodes) != 3 {
		t.Fatalf("after Strimzi: phase %q, %d ready, nodes %v", s.Status.Phase, s.Status.ReadyBrokers, s.Status.Nodes)
	}
	if s.Status.History[0].Kind != "created" {
		t.Errorf("history %v, want created first", s.Status.History)
	}
	v, _ := w.m.Services(ctx)
	if len(v) != 1 || v[0].Phase != lifecycle.Ready || v[0].Bootstrap != "orders-kafka-bootstrap.kafka.svc:9092" {
		t.Errorf("Services() = %+v", v)
	}
}

func TestAutoscale(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	policy := &lifecycle.Autoscale{Min: 3, Max: 6, Target: 1000, ScaleDownAfter: lifecycle.Duration{Duration: 5 * time.Minute}}
	if err := w.m.Create(ctx, lifecycle.Spec{Name: "events", Version: "4.2.0", Brokers: 3, Storage: "10Gi", Autoscale: policy}); err != nil {
		t.Fatal(err)
	}
	w.tick("events", time.Second)
	w.strimzi("events")
	w.tick("events", time.Second)    // ready
	w.tick("events", 30*time.Second) // the first reading is a baseline

	replicas := func() int64 {
		n, _, _ := unstructured.NestedInt64(w.object("KafkaNodePool", "events-brokers").Object, "spec", "replicas")
		return n
	}
	w.rate = 4500
	w.tick("events", 30*time.Second)
	if replicas() != 5 {
		t.Fatalf("4,500 rec/s at 1,000 a broker: pool at %d, want 5", replicas())
	}
	if s := w.service("events"); s.Status.History[0].Kind != "autoscaled" || len(s.Status.Load) == 0 {
		t.Errorf("history %v, load %v", s.Status.History, s.Status.Load)
	}
	w.strimzi("events")

	// Quiet: the brokers stay for the scale-down window, then go.
	w.rate = 500
	for range 8 {
		w.tick("events", 30*time.Second)
	}
	if replicas() != 5 {
		t.Errorf("after 4 quiet minutes the pool is at %d, want 5 still", replicas())
	}
	for range 4 {
		w.tick("events", 30*time.Second)
	}
	if replicas() != 3 {
		t.Errorf("after 6 quiet minutes the pool is at %d, want the minimum 3", replicas())
	}

	// By hand: a fixed size, autoscaling off. Strimzi's last idle state
	// is from later in the same second as the resize, as when a cluster
	// is resized the moment it's first ready; it isn't a rebalance for
	// the resize.
	idle := w.now.Add(time.Second + 300*time.Millisecond).Format(time.RFC3339Nano)
	k := w.object("Kafka", "events")
	_ = unstructured.SetNestedMap(k.Object, map[string]any{"state": "Idle", "lastTransitionTime": idle}, "status", "autoRebalance")
	if err := w.c.Update(ctx, k); err != nil {
		t.Fatal(err)
	}
	if err := w.m.Scale(ctx, "events", 4); err != nil {
		t.Fatal(err)
	}
	w.tick("events", time.Second)
	if replicas() != 4 || w.service("events").Spec.Autoscale != nil {
		t.Errorf("after Scale(4): pool %d, autoscale %v", replicas(), w.service("events").Spec.Autoscale)
	}

	// The new broker is up, but Strimzi hasn't moved partitions on to it:
	// its auto-rebalance hasn't changed state since the resize.
	w.strimzi("events")
	k = w.object("Kafka", "events")
	_ = unstructured.SetNestedField(k.Object, idle, "status", "autoRebalance", "lastTransitionTime")
	if err := w.c.Update(ctx, k); err != nil {
		t.Fatal(err)
	}
	w.tick("events", time.Second)
	if s := w.service("events").Status; s.Phase != "scaling" || s.ReadyBrokers != 4 {
		t.Errorf("4 brokers up, partitions not moved: phase %q with %d ready, want scaling", s.Phase, s.ReadyBrokers)
	}
	// One rebalance is done and Strimzi is idle, but it has another
	// queued for the same broker, as it does on kind.
	_ = unstructured.SetNestedMap(k.Object, map[string]any{
		"state": "Idle", "lastTransitionTime": w.now.Add(time.Second).Format(time.RFC3339Nano),
		"modes": []any{map[string]any{"mode": "add-brokers", "brokers": []any{int64(6)}}},
	}, "status", "autoRebalance")
	if err := w.c.Update(ctx, k); err != nil {
		t.Fatal(err)
	}
	w.tick("events", 2*time.Second)
	if s := w.service("events").Status; s.Phase != "scaling" || s.ScaledAt == nil {
		t.Errorf("idle with a rebalance queued: phase %q, scaledAt %v, want scaling", s.Phase, s.ScaledAt)
	}
	unstructured.RemoveNestedField(k.Object, "status", "autoRebalance", "modes")
	_ = unstructured.SetNestedField(k.Object, w.now.Add(time.Second).Format(time.RFC3339Nano), "status", "autoRebalance", "lastTransitionTime")
	if err := w.c.Update(ctx, k); err != nil {
		t.Fatal(err)
	}
	w.tick("events", 2*time.Second)
	if s := w.service("events").Status; s.Phase != "ready" || s.ScaledAt != nil {
		t.Errorf("after the rebalance: phase %q, scaledAt %v, want ready", s.Phase, s.ScaledAt)
	}
}

func TestRoll(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	if err := w.m.Create(ctx, lifecycle.Spec{Name: "pay", Version: "4.2.0", Brokers: 3, Storage: "10Gi"}); err != nil {
		t.Fatal(err)
	}
	w.tick("pay", time.Second)
	w.strimzi("pay")
	w.tick("pay", time.Second)

	w.now = w.now.Add(time.Minute)
	if err := w.m.Roll(ctx, "pay"); err != nil {
		t.Fatal(err)
	}
	w.tick("pay", time.Second)
	s := w.service("pay")
	if s.Status.Phase != "rolling" || s.Status.Roll == nil || s.Status.Roll.Total != 6 || s.Status.Roll.Done != 0 {
		t.Fatalf("after Roll: phase %q, roll %+v", s.Status.Phase, s.Status.Roll)
	}
	if err := w.m.Roll(ctx, "pay"); !errors.Is(err, lifecycle.ErrBusy) {
		t.Errorf("rolling a rolling cluster: %v, want ErrBusy", err)
	}
	var pods corev1.PodList
	_ = w.c.List(ctx, &pods, client.InNamespace(ns))
	for _, p := range pods.Items {
		if p.Annotations[rollAnnotation] != "true" {
			t.Errorf("pod %s isn't marked to restart", p.Name)
		}
	}

	// Strimzi restarts the pods; recreated, they count as done.
	for i, p := range pods.Items {
		_ = w.c.Delete(ctx, &p)
		w.pod("pay", p.Labels[poolLabel], p.Name)
		w.tick("pay", time.Second)
		if s := w.service("pay"); i < len(pods.Items)-1 && (s.Status.Roll == nil || s.Status.Roll.Done != int32(i+1)) {
			t.Fatalf("after restarting %d pods: roll %+v", i+1, s.Status.Roll)
		}
	}
	s = w.service("pay")
	if s.Status.Phase != "ready" || s.Status.Roll != nil || s.Status.History[0].Kind != "rolled" {
		t.Errorf("after the roll: phase %q, roll %+v, history %v", s.Status.Phase, s.Status.Roll, s.Status.History[0])
	}
}

func TestManagerErrors(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	if err := w.m.Roll(ctx, "nope"); !errors.Is(err, lifecycle.ErrNotFound) {
		t.Errorf("Roll of a missing cluster: %v", err)
	}
	if err := w.m.Create(ctx, lifecycle.Spec{Name: "Bad Name", Version: "4.2.0", Brokers: 3, Storage: "1Gi"}); err == nil {
		t.Error("created a cluster with an invalid name")
	}
	if err := w.m.Scale(ctx, "nope", 0); err == nil {
		t.Error("scaled to zero brokers")
	}
}

func TestCovers(t *testing.T) {
	have := map[string]any{"replicas": float64(3), "roles": []any{"broker"}, "extra": "defaulted"}
	if !covers(have, map[string]any{"replicas": int64(3), "roles": []any{"broker"}}) {
		t.Error("a defaulted field or a float64 count made specs differ")
	}
	if covers(have, map[string]any{"replicas": int64(4)}) {
		t.Error("3 replicas covers 4")
	}
}

// The example manifest is a valid KafkaService.
func TestExampleManifest(t *testing.T) {
	b, err := os.ReadFile("../deploy/operator/example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var ks KafkaService
	if err := yaml.UnmarshalStrict(b, &ks); err != nil {
		t.Fatal(err)
	}
	spec := lifecycle.Spec{Name: ks.Name, Version: ks.Spec.Version, Brokers: int(ks.Spec.Brokers), Storage: ks.Spec.Storage}
	if a := ks.Spec.Autoscale; a != nil {
		p := toPolicy(a)
		spec.Autoscale = &p
	}
	if err := spec.Validate(); err != nil {
		t.Errorf("example.yaml: %v", err)
	}
	if ks.Spec.Autoscale == nil || ks.Spec.Autoscale.ScaleDownAfter.Duration != 10*time.Minute {
		t.Errorf("example autoscale %+v", ks.Spec.Autoscale)
	}
}
