package operator

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options configure the operator.
type Options struct {
	// Namespace limits the operator to one namespace; empty watches all.
	Namespace string
	// Interval is how often each cluster's load is measured.
	Interval time.Duration
	// Probes is the health-probe address (/healthz, /readyz); empty
	// turns them off.
	Probes string
	// LeaderElection lets several replicas run with one active.
	LeaderElection bool
}

// Scheme knows KafkaService, Kubernetes' own types and Strimzi's.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = AddToScheme(s)
	for _, gvk := range []schema.GroupVersionKind{KafkaGVK, NodePoolGVK, RebalanceGVK} {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		list := gvk
		list.Kind += "List"
		s.AddKnownTypeWithName(list, &unstructured.UnstructuredList{})
	}
	return s
}

// Run runs the operator until ctx ends.
func Run(ctx context.Context, cfg *rest.Config, o Options) error {
	scheme := Scheme()
	opts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: o.Probes,
		LeaderElection:         o.LeaderElection,
		LeaderElectionID:       "kaas-operator.kaas.io",
		// Hand the lease over on shutdown, so a rollout's new replica
		// takes over at once and the old one exits cleanly. Safe because
		// the process ends as soon as the manager stops.
		LeaderElectionReleaseOnCancel: true,
	}
	if o.Namespace != "" {
		opts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{o.Namespace: {}}}
	}
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return err
	}
	if o.Probes != "" {
		_ = mgr.AddHealthzCheck("ping", healthz.Ping)
		_ = mgr.AddReadyzCheck("ping", healthz.Ping)
	}
	meter := &KafkaMeter{}
	defer meter.Close()
	r := &Reconciler{Client: mgr.GetClient(), Scheme: scheme, Meter: meter, Interval: o.Interval}
	owned := func(gvk schema.GroupVersionKind) client.Object {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		return u
	}
	err = ctrl.NewControllerManagedBy(mgr).
		For(&KafkaService{}).
		Owns(owned(KafkaGVK)).
		Owns(owned(NodePoolGVK)).
		Named("kafkaservice").
		Complete(r)
	if err != nil {
		return err
	}
	return mgr.Start(ctx)
}
