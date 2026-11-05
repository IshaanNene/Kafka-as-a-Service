package operator

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
)

// Meter reads a cluster's total high-water mark across every partition;
// two readings apart in time make a produce rate.
type Meter interface {
	Offsets(ctx context.Context, bootstrap string) (int64, error)
}

// Reconciler makes a KafkaService's Strimzi resources match it.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Meter  Meter
	// Interval is how often load is measured and autoscaling decides.
	Interval time.Duration
	Now      func() time.Time
}

const (
	keepLoad    = 120 // samples: an hour at the default interval
	keepHistory = 20
)

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Reconciler) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return 30 * time.Second
}

// Reconcile is one pass over one KafkaService.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ks := &KafkaService{}
	if err := r.Get(ctx, req.NamespacedName, ks); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !ks.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // owner references take the Strimzi resources with it
	}
	var before KafkaServiceStatus
	jsonCopy(&ks.Status, &before)
	now := r.now()

	r.size(ctx, ks, now)
	resized := before.Brokers != 0 && ks.Status.Brokers != before.Brokers
	for _, obj := range render(ks, ks.Status.Brokers) {
		if err := r.upsert(ctx, ks, obj); err != nil {
			return ctrl.Result{}, fmt.Errorf("%s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	pods, err := r.pods(ctx, ks)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.roll(ctx, ks, pods, now); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.observe(ctx, ks, pods, resized, now); err != nil {
		return ctrl.Result{}, err
	}
	ks.Status.ObservedGeneration = ks.Generation
	if !reflect.DeepEqual(before, ks.Status) {
		if err := r.Status().Update(ctx, ks); apierrors.IsConflict(err) {
			// The cache was behind our own last write, or someone edited
			// the spec; the event for that change brings a fresh pass.
			return ctrl.Result{RequeueAfter: time.Second}, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}
	}
	// A roll moves quickly and people watch it; otherwise the next
	// measurement is the next thing to do.
	if ks.Status.Roll != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// size measures load and settles the broker count in Status.Brokers.
func (r *Reconciler) size(ctx context.Context, ks *KafkaService, now time.Time) {
	st := &ks.Status
	sampled := r.measure(ctx, ks, now)
	a := ks.Spec.Autoscale
	if a == nil {
		if st.Brokers != 0 && st.Brokers != ks.Spec.Brokers {
			record(ks, now, "scaled", fmt.Sprintf("%d → %d brokers, by hand", st.Brokers, ks.Spec.Brokers))
		}
		st.Brokers = ks.Spec.Brokers
		st.Marks, st.Recommended, st.Reason = nil, 0, ""
		return
	}
	policy := toPolicy(a)
	current := st.Brokers
	if current == 0 {
		current = ks.Spec.Brokers
	}
	if clamped := min(max(current, a.Min), a.Max); clamped != current {
		if st.Brokers != 0 {
			record(ks, now, "scaled", fmt.Sprintf("%d → %d brokers, to fit the autoscaling bounds", current, clamped))
		}
		current = clamped
	}
	if sampled && len(st.Load) > 0 {
		rec, why := lifecycle.Recommend(policy, float64(st.Load[len(st.Load)-1].V))
		st.Recommended, st.Reason = int32(rec), why
		st.Marks = append(st.Marks, Mark{At: metav1.NewTime(now), Brokers: int32(rec)})
		st.Marks = fromMarks(lifecycle.Trim(toMarks(st.Marks), policy.ScaleDownAfter.Duration, now))
	}
	// Hold the size still while brokers restart or partitions move:
	// two sources of partition movement at once is more than anyone can
	// reason about. Strimzi can start another rebalance after the one
	// for the last change, so the last pass's phase counts too.
	moving := st.ScaledAt != nil || st.Phase == string(lifecycle.Scaling)
	switch {
	case len(st.Marks) == 0:
	case st.Recommended == current && (st.Roll != nil || moving):
		st.Reason = "sized for the load"
	case st.Roll != nil:
		st.Reason = fmt.Sprintf("load fits %d; held while brokers restart", st.Recommended)
	case moving:
		st.Reason = fmt.Sprintf("load fits %d; held until partitions finish moving", st.Recommended)
	default:
		n, why := lifecycle.Decide(policy, int(current), toMarks(st.Marks), now)
		st.Reason = why
		if int32(n) != current {
			record(ks, now, "autoscaled", fmt.Sprintf("%d → %d brokers: %s", current, n, why))
			current = int32(n)
		}
	}
	st.Brokers = current
}

// measure takes a load sample once an interval, reporting whether it did.
func (r *Reconciler) measure(ctx context.Context, ks *KafkaService, now time.Time) bool {
	st := &ks.Status
	if r.Meter == nil || !st.Ready {
		return false
	}
	if st.MeasuredAt != nil && now.Sub(st.MeasuredAt.Time) < r.interval()*9/10 {
		return false
	}
	off, err := r.Meter.Offsets(ctx, Bootstrap(ks))
	if err != nil {
		ctrl.LoggerFrom(ctx).Info("couldn't measure load", "error", err.Error())
		return false
	}
	took := false
	if st.MeasuredAt != nil && off >= st.Offsets {
		secs := now.Sub(st.MeasuredAt.Time).Seconds()
		st.Load = append(st.Load, LoadSample{T: now.UnixMilli(), V: int64(float64(off-st.Offsets) / secs)})
		if len(st.Load) > keepLoad {
			st.Load = st.Load[len(st.Load)-keepLoad:]
		}
		took = true
	}
	st.Offsets, st.MeasuredAt = off, &metav1.Time{Time: now}
	return took
}

// upsert creates obj, or updates the one there when it differs in
// anything obj sets. Fields Strimzi or the API server default are left
// alone, so a steady cluster costs no writes.
func (r *Reconciler) upsert(ctx context.Context, ks *KafkaService, want *unstructured.Unstructured) error {
	if err := controllerutil.SetControllerReference(ks, want, r.Scheme); err != nil {
		return err
	}
	have := &unstructured.Unstructured{}
	have.SetGroupVersionKind(want.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(want), have)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, want)
	}
	if err != nil {
		return err
	}
	if covers(have.Object["spec"], want.Object["spec"]) && covers(toAny(have.GetLabels()), toAny(want.GetLabels())) &&
		covers(toAny(have.GetAnnotations()), toAny(want.GetAnnotations())) && len(have.GetOwnerReferences()) > 0 {
		return nil
	}
	have.Object["spec"] = want.Object["spec"]
	have.SetLabels(merge(have.GetLabels(), want.GetLabels()))
	have.SetAnnotations(merge(have.GetAnnotations(), want.GetAnnotations()))
	have.SetOwnerReferences(want.GetOwnerReferences())
	return r.Update(ctx, have)
}

// covers is whether have holds everything in want: maps may carry more
// keys, everything else must match. Numbers compare by value, since JSON
// round trips turn int64 into float64.
func covers(have, want any) bool {
	switch w := want.(type) {
	case map[string]any:
		h, ok := have.(map[string]any)
		if !ok {
			return len(w) == 0 && have == nil
		}
		for k, v := range w {
			if !covers(h[k], v) {
				return false
			}
		}
		return true
	case []any:
		h, ok := have.([]any)
		if !ok || len(h) != len(w) {
			return false
		}
		for i := range w {
			if !covers(h[i], w[i]) {
				return false
			}
		}
		return true
	case int64, int, float64, int32:
		return number(have) == number(want)
	}
	return reflect.DeepEqual(have, want)
}

func number(v any) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case float64:
		return n
	}
	return -1
}

func toAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func merge(a, b map[string]string) map[string]string {
	if a == nil {
		a = map[string]string{}
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}

// pods is every Kafka pod of the service, controllers and brokers.
func (r *Reconciler) pods(ctx context.Context, ks *KafkaService) ([]corev1.Pod, error) {
	var list corev1.PodList
	err := r.List(ctx, &list, client.InNamespace(ks.Namespace), client.MatchingLabels{clusterLabel: ks.Name})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if pool := p.Labels[poolLabel]; pool == brokerPool(ks) || pool == controllerPool(ks) {
			out = append(out, p)
		}
	}
	return out, nil
}

// roll starts a requested roll and follows it: a pod is done once it has
// been recreated since the request and is ready again.
func (r *Reconciler) roll(ctx context.Context, ks *KafkaService, pods []corev1.Pod, now time.Time) error {
	st := &ks.Status
	at := ks.Spec.RestartAt
	if at == nil || (st.RolledAt != nil && !at.After(st.RolledAt.Time)) {
		st.Roll = nil
		return nil
	}
	if st.Roll == nil {
		record(ks, now, "rolling", fmt.Sprintf("restarting %d pods, one at a time", len(pods)))
	}
	done := int32(0)
	for i := range pods {
		p := &pods[i]
		if !p.CreationTimestamp.Before(at) {
			if ready(p) {
				done++
			}
			continue
		}
		if p.Annotations[rollAnnotation] == "true" {
			continue
		}
		patch := client.MergeFrom(p.DeepCopy())
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		p.Annotations[rollAnnotation] = "true"
		if err := r.Patch(ctx, p, patch); err != nil {
			return fmt.Errorf("marking pod %s to restart: %w", p.Name, err)
		}
	}
	st.Roll = &RollStatus{Done: done, Total: int32(len(pods))}
	if done == int32(len(pods)) {
		st.Roll, st.RolledAt = nil, at.DeepCopy()
		record(ks, now, "rolled", fmt.Sprintf("every pod restarted in %s", now.Sub(at.Time).Round(time.Second)))
	}
	return nil
}

// observe reads the brokers and Strimzi's view into the status.
// resized is whether this pass changed the broker count.
func (r *Reconciler) observe(ctx context.Context, ks *KafkaService, pods []corev1.Pod, resized bool, now time.Time) error {
	st := &ks.Status
	st.Nodes, st.ReadyBrokers = nil, 0
	for i := range pods {
		p := &pods[i]
		if p.Labels[poolLabel] != brokerPool(ks) {
			continue
		}
		n := NodeStatus{ID: nodeID(p.Name), Ready: ready(p), State: podState(p, ks.Spec.RestartAt)}
		if n.Ready {
			st.ReadyBrokers++
		}
		st.Nodes = append(st.Nodes, n)
	}
	sortNodes(st.Nodes)

	k := &unstructured.Unstructured{}
	k.SetGroupVersionKind(KafkaGVK)
	if err := r.Get(ctx, client.ObjectKey{Namespace: ks.Namespace, Name: ks.Name}, k); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	st.Bootstrap = Bootstrap(ks)
	if b := listener(k, "plain"); b != "" {
		st.Bootstrap = b
	}
	kafkaReady, failure := strimziState(k)
	rebalancing, _, _ := unstructured.NestedString(k.Object, "status", "autoRebalance", "state")
	transition, _, _ := unstructured.NestedString(k.Object, "status", "autoRebalance", "lastTransitionTime")
	// Strimzi lists brokers still to be rebalanced on to or off in modes.
	// It can go idle between two rebalances for one resize, with the
	// second still queued there, so idle alone isn't done.
	queued, _, _ := unstructured.NestedSlice(k.Object, "status", "autoRebalance", "modes")
	settled := rebalancing == "Idle" && len(queued) == 0
	switch {
	case resized:
		st.ScaledAt, st.RebalancedBefore = &metav1.Time{Time: now}, transition
	case st.ScaledAt != nil && settled && transition != st.RebalancedBefore:
		st.ScaledAt, st.RebalancedBefore = nil, ""
	}

	phase, msg := lifecycle.Ready, ""
	switch {
	case failure != "":
		phase, msg = lifecycle.Failed, failure
	case st.Roll != nil:
		phase, msg = lifecycle.Rolling, fmt.Sprintf("%d of %d pods restarted", st.Roll.Done, st.Roll.Total)
	case !st.Ready:
		if kafkaReady && st.ReadyBrokers >= st.Brokers {
			st.Ready = true
			record(ks, now, "created", fmt.Sprintf("serving on %d brokers", st.ReadyBrokers))
		} else {
			phase, msg = lifecycle.Provisioning, fmt.Sprintf("%d of %d brokers up", st.ReadyBrokers, st.Brokers)
		}
	case rebalancing != "" && rebalancing != "Idle":
		phase, msg = lifecycle.Scaling, "moving partitions ("+rebalancing+")"
	case rebalancing == "Idle" && len(queued) > 0:
		phase, msg = lifecycle.Scaling, "partitions queued to move"
	case int32(len(st.Nodes)) != st.Brokers:
		phase, msg = lifecycle.Scaling, fmt.Sprintf("%d brokers, going to %d", len(st.Nodes), st.Brokers)
	case st.ScaledAt != nil:
		phase, msg = lifecycle.Scaling, fmt.Sprintf("%d brokers; waiting for Strimzi to move partitions", st.Brokers)
	case st.ReadyBrokers < st.Brokers:
		phase, msg = lifecycle.Degraded, fmt.Sprintf("%d of %d brokers ready", st.ReadyBrokers, st.Brokers)
	}
	if phase == lifecycle.Failed && st.Phase != string(lifecycle.Failed) {
		record(ks, now, "failed", msg)
	}
	st.Phase, st.Message = string(phase), msg
	cond := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: titleCase(string(phase)), Message: msg, ObservedGeneration: ks.Generation}
	if phase == lifecycle.Ready {
		cond.Status, cond.Message = metav1.ConditionTrue, fmt.Sprintf("%d brokers serving", st.ReadyBrokers)
	}
	meta.SetStatusCondition(&st.Conditions, cond)
	return nil
}

// strimziState reads the Kafka's Ready condition: whether it's ready,
// and the error if Strimzi gave up on it. Strimzi reports a failed
// reconciliation as NotReady with the exception's name as the reason;
// "Creating" and the like are just progress.
func strimziState(k *unstructured.Unstructured) (bool, string) {
	conds, _, _ := unstructured.NestedSlice(k.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		typ, _ := m["type"].(string)
		status, _ := m["status"].(string)
		reason, _ := m["reason"].(string)
		msg, _ := m["message"].(string)
		switch {
		case typ == "Ready" && status == "True":
			return true, ""
		case typ == "NotReady" && status == "True" && strings.HasSuffix(reason, "Exception"):
			if msg == "" {
				msg = reason
			}
			return false, "Strimzi: " + msg
		}
	}
	return false, ""
}

func listener(k *unstructured.Unstructured, name string) string {
	ls, _, _ := unstructured.NestedSlice(k.Object, "status", "listeners")
	for _, l := range ls {
		m, _ := l.(map[string]any)
		if m["name"] == name {
			s, _ := m["bootstrapServers"].(string)
			return s
		}
	}
	return ""
}

func ready(p *corev1.Pod) bool {
	if !p.DeletionTimestamp.IsZero() {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podState(p *corev1.Pod, restartAt *metav1.Time) string {
	switch {
	case !p.DeletionTimestamp.IsZero():
		return "stopping"
	case p.Status.Phase == corev1.PodPending:
		return "pending"
	case ready(p):
		if p.Annotations[rollAnnotation] == "true" {
			return "queued"
		}
		return "running"
	case restartAt != nil && !p.CreationTimestamp.Before(restartAt):
		return "restarting"
	}
	return "starting"
}

// nodeID is the node ID Strimzi puts at the end of a pod's name.
func nodeID(pod string) int32 {
	n, _ := strconv.Atoi(pod[strings.LastIndexByte(pod, '-')+1:])
	return int32(n)
}

func sortNodes(ns []NodeStatus) {
	for i := 1; i < len(ns); i++ {
		for j := i; j > 0 && ns[j].ID < ns[j-1].ID; j-- {
			ns[j], ns[j-1] = ns[j-1], ns[j]
		}
	}
}

func record(ks *KafkaService, now time.Time, kind, detail string) {
	h := append([]HistoryEvent{{At: metav1.NewTime(now), Kind: kind, Detail: detail}}, ks.Status.History...)
	if len(h) > keepHistory {
		h = h[:keepHistory]
	}
	ks.Status.History = h
}

func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func toPolicy(a *AutoscaleSpec) lifecycle.Autoscale {
	return lifecycle.Autoscale{Min: int(a.Min), Max: int(a.Max), Target: int(a.Target), ScaleDownAfter: lifecycle.Duration{Duration: a.ScaleDownAfter.Duration}}
}

func toMarks(ms []Mark) []lifecycle.Mark {
	out := make([]lifecycle.Mark, len(ms))
	for i, m := range ms {
		out[i] = lifecycle.Mark{At: m.At.Time, Brokers: int(m.Brokers)}
	}
	return out
}

func fromMarks(ms []lifecycle.Mark) []Mark {
	out := make([]Mark, len(ms))
	for i, m := range ms {
		out[i] = Mark{At: metav1.NewTime(m.At), Brokers: int32(m.Brokers)}
	}
	return out
}
