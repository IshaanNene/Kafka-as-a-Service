//go:build e2e

// Package e2e drives the kaas operator through a managed cluster's life
// against real Strimzi: create, a steady state, a roll, and resizing in
// both directions, through operator.Manager, the console's way in. It
// needs a cluster with Strimzi and the operator running in it, from the
// current kubectl context:
//
//	task dev && task operator:kind
//	go test -tags e2e ./test/e2e -timeout 45m -v
//
// Autoscaling on load isn't here: it takes a producer in the cluster and
// a scale-down window, and its rules are unit-tested. USAGE has the
// manual walkthrough (task operator:load).
package e2e

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
	"github.com/IshaanNene/Kafka-as-a-Service/operator"
)

const (
	ns           = "kafka"
	operatorNS   = "kaas-system"
	stageTimeout = 12 * time.Minute
)

type env struct {
	t     *testing.T
	ctx   context.Context
	c     client.Client
	kube  *kubernetes.Clientset
	m     *operator.Manager
	name  string
	phase string // the last phase logged
}

func TestOperator(t *testing.T) {
	cfg, err := config.GetConfig()
	if err != nil {
		t.Fatalf("no Kubernetes to talk to: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, ctx: context.Background(), c: c, kube: kube, m: &operator.Manager{Client: c, Namespace: ns}}
	e.name = fmt.Sprintf("e2e-%d", time.Now().Unix()%100000)
	start := time.Now()

	if err := e.m.Create(e.ctx, lifecycle.Spec{Name: e.name, Version: "4.2.0", Brokers: 3, Storage: "5Gi"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.remove)

	t.Run("create", func(t *testing.T) {
		e.wait("ready on 3 brokers", func(s *operator.KafkaService) bool {
			return s.Status.Phase == "ready" && s.Status.ReadyBrokers == 3
		})
		if s := e.service(); s.Status.Bootstrap != e.name+"-kafka-bootstrap.kafka.svc:9092" {
			t.Errorf("bootstrap %q", s.Status.Bootstrap)
		}
	})

	// The operator compares what it wants with what's there on every
	// pass and writes only differences. generation moves only when a
	// spec does, unlike resourceVersion, which Strimzi's status updates
	// move too.
	t.Run("steady", func(t *testing.T) {
		before := e.generations()
		time.Sleep(70 * time.Second) // two passes at the default 30s
		if after := e.generations(); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Errorf("a steady cluster's specs were rewritten: generations %v, then %v", before, after)
		}
	})

	t.Run("roll", func(t *testing.T) {
		if err := e.m.Roll(e.ctx, e.name); err != nil {
			t.Fatal(err)
		}
		at := e.service().Spec.RestartAt.Time
		e.wait("rolled", func(s *operator.KafkaService) bool {
			return s.Status.Phase == "ready" && s.Status.RolledAt != nil && !s.Status.RolledAt.Before(&metav1.Time{Time: at})
		})
		pods := e.pods()
		if len(pods) != 3+operator.Controllers {
			t.Errorf("%d pods after the roll, want %d", len(pods), 3+operator.Controllers)
		}
		for _, p := range pods {
			if p.CreationTimestamp.Time.Before(at) {
				t.Errorf("pod %s from %s wasn't restarted for a roll at %s", p.Name, p.CreationTimestamp, at)
			}
		}
	})

	// A resize is done when Strimzi has moved partitions, not when the
	// pods are ready: new brokers are up a minute or so before the
	// rebalance on to them starts, and old ones are only stopped after
	// the rebalance off them.
	for _, step := range []struct {
		n     int
		moves string // the auto-rebalance state Strimzi must pass through
	}{{4, "RebalanceOnScaleUp"}, {3, "RebalanceOnScaleDown"}} {
		n := step.n
		t.Run(fmt.Sprintf("scale to %d", n), func(t *testing.T) {
			if err := e.m.Scale(e.ctx, e.name, n); err != nil {
				t.Fatal(err)
			}
			rebalanced := false
			e.wait(fmt.Sprintf("ready on %d brokers", n), func(s *operator.KafkaService) bool {
				state, queued := e.rebalance()
				if state == step.moves {
					rebalanced = true
				}
				// Until the operator has seen the new size, the status is
				// the old one's.
				if s.Status.ObservedGeneration != s.Generation || s.Status.Brokers != int32(n) {
					return false
				}
				if s.Status.Phase != "ready" {
					return false
				}
				if !rebalanced {
					t.Fatalf("ready on %d of %d brokers before Strimzi went through %s", s.Status.ReadyBrokers, n, step.moves)
				}
				if state != "Idle" || queued > 0 {
					t.Fatalf("ready while Strimzi's auto-rebalance is %s with %d queued", state, queued)
				}
				return s.Status.ReadyBrokers == int32(n) && s.Status.ScaledAt == nil
			})
			if got := len(e.pods()) - operator.Controllers; got != n {
				t.Errorf("%d broker pods, want %d", got, n)
			}
		})
	}

	t.Run("operator log", func(t *testing.T) {
		for _, line := range e.operatorErrors(start) {
			t.Errorf("operator: %s", line)
		}
	})
}

// wait polls the service until done says so, logging phase changes.
func (e *env) wait(what string, done func(*operator.KafkaService) bool) {
	e.t.Helper()
	deadline := time.Now().Add(stageTimeout)
	for time.Now().Before(deadline) {
		s := e.service()
		if line := fmt.Sprintf("%s %d/%d %s", s.Status.Phase, s.Status.ReadyBrokers, s.Status.Brokers, s.Status.Message); line != e.phase {
			e.t.Logf("%s  %s", time.Now().Format("15:04:05"), line)
			e.phase = line
		}
		if done(s) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	e.t.Fatalf("not %s after %s", what, stageTimeout)
}

func (e *env) service() *operator.KafkaService {
	e.t.Helper()
	ks := &operator.KafkaService{}
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: ns, Name: e.name}, ks); err != nil {
		e.t.Fatal(err)
	}
	return ks
}

func (e *env) strimzi(kind, name string) *unstructured.Unstructured {
	e.t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(operator.KafkaGVK.GroupVersion().WithKind(kind))
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: ns, Name: name}, u); err != nil {
		e.t.Fatal(err)
	}
	return u
}

// rebalance is the Kafka's auto-rebalance state, Idle between moves,
// and how many add or remove requests Strimzi still has queued.
func (e *env) rebalance() (string, int) {
	k := e.strimzi("Kafka", e.name)
	state, _, _ := unstructured.NestedString(k.Object, "status", "autoRebalance", "state")
	modes, _, _ := unstructured.NestedSlice(k.Object, "status", "autoRebalance", "modes")
	return state, len(modes)
}

func (e *env) generations() map[string]int64 {
	out := map[string]int64{}
	for _, r := range []struct{ kind, name string }{
		{"Kafka", e.name},
		{"KafkaNodePool", e.name + "-controllers"},
		{"KafkaNodePool", e.name + "-brokers"},
		{"KafkaRebalance", e.name + "-rebalance"},
	} {
		out[r.kind+"/"+r.name] = e.strimzi(r.kind, r.name).GetGeneration()
	}
	return out
}

// pods are the cluster's controller and broker pods.
func (e *env) pods() []corev1.Pod {
	e.t.Helper()
	var list corev1.PodList
	if err := e.c.List(e.ctx, &list, client.InNamespace(ns), client.MatchingLabels{"strimzi.io/cluster": e.name}, client.HasLabels{"strimzi.io/pool-name"}); err != nil {
		e.t.Fatal(err)
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	return out
}

// operatorErrors are the error lines the operator's pods logged since.
func (e *env) operatorErrors(since time.Time) []string {
	e.t.Helper()
	pods, err := e.kube.CoreV1().Pods(operatorNS).List(e.ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=kaas-operator"})
	if err != nil {
		e.t.Fatal(err)
	}
	if len(pods.Items) == 0 {
		e.t.Fatal("no operator pods in " + operatorNS + ": run task operator:kind")
	}
	var out []string
	for _, p := range pods.Items {
		r, err := e.kube.CoreV1().Pods(operatorNS).GetLogs(p.Name, &corev1.PodLogOptions{SinceTime: &metav1.Time{Time: since}}).Stream(e.ctx)
		if err != nil {
			e.t.Fatal(err)
		}
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			if strings.Contains(sc.Text(), `"level":"ERROR"`) {
				out = append(out, p.Name+": "+sc.Text())
			}
		}
		r.Close()
	}
	return out
}

// remove deletes the service, checks its Strimzi resources go with it,
// and deletes the broker volumes it leaves on purpose.
func (e *env) remove() {
	ks := &operator.KafkaService{}
	ks.Name, ks.Namespace = e.name, ns
	if err := e.c.Delete(e.ctx, ks); err != nil && !apierrors.IsNotFound(err) {
		e.t.Errorf("delete: %v", err)
		return
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		k := &unstructured.Unstructured{}
		k.SetGroupVersionKind(operator.KafkaGVK)
		err := e.c.Get(e.ctx, client.ObjectKey{Namespace: ns, Name: e.name}, k)
		if apierrors.IsNotFound(err) {
			break
		}
		if time.Now().After(deadline) {
			e.t.Errorf("the Kafka outlived its KafkaService by %s", 3*time.Minute)
			break
		}
		time.Sleep(2 * time.Second)
	}
	_ = e.c.DeleteAllOf(e.ctx, &corev1.PersistentVolumeClaim{}, client.InNamespace(ns), client.MatchingLabels{"strimzi.io/cluster": e.name})
}
