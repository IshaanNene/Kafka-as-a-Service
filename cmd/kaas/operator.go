package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-logr/logr"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/IshaanNene/Kafka-as-a-Service/operator"
)

func newOperatorCmd() *cobra.Command {
	var (
		o           operator.Options
		development bool
	)
	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Run the Kubernetes operator for managed Kafka clusters",
		Long: `Run the operator that turns KafkaService resources (kaas.io/v1alpha1)
into Strimzi Kafka clusters, rolls their brokers on request and sizes the
broker pool to the load it measures.

It needs Strimzi installed, the KafkaService CRD applied
(deploy/operator/crd.yaml) and RBAC to match (deploy/operator/rbac.yaml).
Outside a cluster it uses KUBECONFIG, which is how to try it against kind.`,
		Example: `  # Against the current kubectl context, one namespace
  kaas operator --namespace kafka
  # In a cluster, as deploy/operator/operator.yaml runs it
  kaas operator --leader-elect --probes :8081`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if o.Interval < 5*time.Second {
				return usageError(cmd, "interval", o.Interval.String(), "measure at most every 5s")
			}
			// JSON for log collectors; text when a person is reading.
			var h slog.Handler = slog.NewJSONHandler(os.Stderr, nil)
			if development {
				h = slog.NewTextHandler(os.Stderr, nil)
			}
			log.SetLogger(logr.FromSlogHandler(h))
			cfg, err := config.GetConfig()
			if err != nil {
				return fmt.Errorf("no Kubernetes to talk to: %w", err)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "kaas operator  %s\n", scope(o.Namespace))
			return operator.Run(cmd.Context(), cfg, o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.Namespace, "namespace", envOr("KAAS_NAMESPACE", ""), "watch one namespace (default every namespace)")
	f.DurationVar(&o.Interval, "interval", 30*time.Second, "how often to measure each cluster's load")
	f.StringVar(&o.Probes, "probes", "", "serve /healthz and /readyz on this address")
	f.BoolVar(&o.LeaderElection, "leader-elect", false, "run with others, one active")
	f.BoolVar(&development, "dev-log", false, "human-readable logs")
	return cmd
}

func scope(ns string) string {
	if ns == "" {
		return "watching every namespace"
	}
	return "watching namespace " + ns
}

// kubeManager is the console's view of the KafkaServices in namespace,
// through the current kubeconfig context or the in-cluster account.
func kubeManager(namespace string) (*operator.Manager, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("no Kubernetes to talk to: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		return nil, err
	}
	if namespace == "" {
		return nil, errors.New("--namespace is required with --kube")
	}
	m := &operator.Manager{Client: c, Namespace: namespace}
	if raw, err := clientcmd.NewDefaultClientConfigLoadingRules().Load(); err == nil {
		m.Context = raw.CurrentContext
	}
	return m, nil
}
