package operator

import (
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Strimzi's resources, on its v1 API (Strimzi 1.0 and later). They're
// handled as unstructured objects: Strimzi publishes no Go types, and
// the few fields set here are clearer written out than imported.
var (
	KafkaGVK     = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
	NodePoolGVK  = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaNodePool"}
	RebalanceGVK = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaRebalance"}
)

const (
	// Controllers is the KRaft quorum's size. It's fixed: three survive
	// one loss, and changing a quorum's membership is a separate,
	// rarer operation than scaling brokers.
	Controllers = 3
	// controllerDisk holds only cluster metadata.
	controllerDisk = "10Gi"

	clusterLabel = "strimzi.io/cluster"
	poolLabel    = "strimzi.io/pool-name"
	managedBy    = "app.kubernetes.io/managed-by"
	// rollAnnotation on a pod makes Strimzi restart it, in its own safe
	// order: controllers first, then brokers, each once its partitions
	// are back in sync.
	rollAnnotation = "strimzi.io/manual-rolling-update"
)

// Names of the Strimzi resources for a service. The Kafka shares the
// service's name; pools are prefixed with it because KafkaNodePool names
// are unique per namespace, not per cluster.
func brokerPool(ks *KafkaService) string     { return ks.Name + "-brokers" }
func controllerPool(ks *KafkaService) string { return ks.Name + "-controllers" }
func rebalanceName(ks *KafkaService) string  { return ks.Name + "-rebalance" }

// Bootstrap is the in-cluster address clients connect to.
func Bootstrap(ks *KafkaService) string {
	return ks.Name + "-kafka-bootstrap." + ks.Namespace + ".svc:9092"
}

// render is every Strimzi object for ks, running brokers brokers.
func render(ks *KafkaService, brokers int32) []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		kafka(ks, brokers),
		pool(ks, controllerPool(ks), Controllers, []any{"controller"}, controllerDisk),
		pool(ks, brokerPool(ks), brokers, []any{"broker"}, ks.Spec.Storage),
		rebalance(ks),
	}
}

func object(gvk schema.GroupVersionKind, ks *KafkaService, name string, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	u.SetGroupVersionKind(gvk)
	u.SetName(name)
	u.SetNamespace(ks.Namespace)
	u.SetLabels(map[string]string{managedBy: "kaas", "kaas.io/service": ks.Name})
	return u
}

func kafka(ks *KafkaService, brokers int32) *unstructured.Unstructured {
	config := map[string]any{}
	for k, v := range defaults(brokers) {
		config[k] = v
	}
	for k, v := range ks.Spec.Config {
		config[k] = v
	}
	// Cruise Control moves partitions when the broker pool changes size:
	// on to brokers just added, and off brokers before they're removed
	// (Strimzi won't remove a broker that still holds partitions).
	auto := []any{
		map[string]any{"mode": "add-brokers", "template": map[string]any{"name": rebalanceName(ks)}},
		map[string]any{"mode": "remove-brokers", "template": map[string]any{"name": rebalanceName(ks)}},
	}
	return object(KafkaGVK, ks, ks.Name, map[string]any{
		"kafka": map[string]any{
			"version": ks.Spec.Version,
			"listeners": []any{
				map[string]any{"name": "plain", "port": int64(9092), "type": "internal", "tls": false},
			},
			"config": config,
		},
		"entityOperator": map[string]any{"topicOperator": map[string]any{}, "userOperator": map[string]any{}},
		"cruiseControl":  map[string]any{"autoRebalance": auto},
	})
}

// defaults are durable settings for the brokers there are: three copies
// of everything, two of which must have a write, once there are three
// brokers to hold them. They're set when internal topics are created, so
// a cluster started small keeps fewer copies of those.
func defaults(brokers int32) map[string]string {
	rf := min(brokers, 3)
	isr := max(rf-1, 1)
	return map[string]string{
		"default.replication.factor":               strconv.Itoa(int(rf)),
		"min.insync.replicas":                      strconv.Itoa(int(isr)),
		"offsets.topic.replication.factor":         strconv.Itoa(int(rf)),
		"transaction.state.log.replication.factor": strconv.Itoa(int(rf)),
		"transaction.state.log.min.isr":            strconv.Itoa(int(isr)),
	}
}

func pool(ks *KafkaService, name string, replicas int32, roles []any, size string) *unstructured.Unstructured {
	u := object(NodePoolGVK, ks, name, map[string]any{
		"replicas": int64(replicas),
		"roles":    roles,
		"storage": map[string]any{
			"type": "jbod",
			"volumes": []any{map[string]any{
				"id": int64(0), "type": "persistent-claim", "size": size,
				"kraftMetadata": "shared", "deleteClaim": false,
			}},
		},
	})
	addLabel(u, clusterLabel, ks.Name)
	return u
}

// rebalance is the template Strimzi copies for each automatic rebalance.
func rebalance(ks *KafkaService) *unstructured.Unstructured {
	u := object(RebalanceGVK, ks, rebalanceName(ks), map[string]any{})
	addLabel(u, clusterLabel, ks.Name)
	u.SetAnnotations(map[string]string{"strimzi.io/rebalance-template": "true"})
	return u
}

func addLabel(u *unstructured.Unstructured, k, v string) {
	l := u.GetLabels()
	l[k] = v
	u.SetLabels(l)
}
