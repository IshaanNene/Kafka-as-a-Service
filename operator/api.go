// Package operator runs managed Kafka clusters on Kubernetes.
//
// A KafkaService (kaas.io/v1alpha1) says what a team wants: a Kafka
// version, a broker count or an autoscaling policy, and the disk each
// broker gets. The operator turns it into Strimzi resources (a Kafka, a
// controller pool, a broker pool and a rebalance template) and leaves
// running the brokers to Strimzi. On top it adds what Strimzi leaves to
// people: rolling every broker on request, measuring load and sizing the
// broker pool to it, with Strimzi's Cruise Control moving partitions on
// to new brokers and off ones about to go.
//
// It also implements lifecycle.Manager, which is how the console lists,
// creates, rolls and resizes these clusters.
package operator

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the KafkaService API's group and version.
var GroupVersion = schema.GroupVersion{Group: "kaas.io", Version: "v1alpha1"}

// AddToScheme registers KafkaService with a scheme.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &KafkaService{}, &KafkaServiceList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

// KafkaService is a managed Kafka cluster.
type KafkaService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaServiceSpec   `json:"spec"`
	Status KafkaServiceStatus `json:"status,omitempty"`
}

// KafkaServiceSpec is what's wanted.
type KafkaServiceSpec struct {
	Version string `json:"version"`
	// Brokers is the broker count when Autoscale is unset.
	Brokers int32 `json:"brokers"`
	// Storage is each broker's disk, e.g. 100Gi.
	Storage string `json:"storage"`
	// Config is broker configuration over the operator's defaults.
	Config    map[string]string `json:"config,omitempty"`
	Autoscale *AutoscaleSpec    `json:"autoscale,omitempty"`
	// RestartAt rolls every broker when set to a time after the last
	// roll. `kaas` and the console set it to now.
	RestartAt *metav1.Time `json:"restartAt,omitempty"`
}

// AutoscaleSpec sizes the broker pool to load.
type AutoscaleSpec struct {
	Min int32 `json:"min"`
	Max int32 `json:"max"`
	// Target is records a second per broker.
	Target         int32           `json:"target"`
	ScaleDownAfter metav1.Duration `json:"scaleDownAfter"`
}

// KafkaServiceStatus is what the operator last saw and did.
type KafkaServiceStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Phase              string `json:"phase,omitempty"`
	Message            string `json:"message,omitempty"`
	Bootstrap          string `json:"bootstrap,omitempty"`
	// Brokers is the broker count the operator is running towards: the
	// spec's, or autoscaling's decision.
	Brokers      int32        `json:"brokers,omitempty"`
	ReadyBrokers int32        `json:"readyBrokers,omitempty"`
	Nodes        []NodeStatus `json:"nodes,omitempty"`
	// Ready is set the first time every broker is up; until then a
	// missing broker means provisioning, not degraded.
	Ready bool `json:"ready,omitempty"`
	// ScaledAt is when Brokers last changed, and RebalancedBefore the
	// Kafka's auto-rebalance transition time as it stood then. Both are
	// cleared once Strimzi has acted on the change: its auto-rebalance is
	// idle again after a transition of its own since. Until then the pods
	// alone don't say the change is done, since Strimzi moves partitions
	// on to new brokers after they start and off old ones before they
	// stop. Strimzi's own timestamps are compared with each other, never
	// with ours: a resize in the same second as Strimzi's first idle
	// state otherwise reads as already rebalanced.
	ScaledAt         *metav1.Time `json:"scaledAt,omitempty"`
	RebalancedBefore string       `json:"rebalancedBefore,omitempty"`

	// RolledAt is the RestartAt last acted on. Roll is set while one runs.
	RolledAt *metav1.Time `json:"rolledAt,omitempty"`
	Roll     *RollStatus  `json:"roll,omitempty"`

	// Offsets and MeasuredAt are the last load sample's high-water-mark
	// total, so the next one can turn the difference into a rate.
	Offsets     int64        `json:"offsets,omitempty"`
	MeasuredAt  *metav1.Time `json:"measuredAt,omitempty"`
	Load        []LoadSample `json:"load,omitempty"`
	Marks       []Mark       `json:"marks,omitempty"`
	Recommended int32        `json:"recommended,omitempty"`
	Reason      string       `json:"reason,omitempty"`

	History    []HistoryEvent     `json:"history,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NodeStatus is one broker.
type NodeStatus struct {
	ID    int32  `json:"id"`
	Ready bool   `json:"ready"`
	State string `json:"state"`
}

// RollStatus counts brokers through a roll.
type RollStatus struct {
	Done  int32 `json:"done"`
	Total int32 `json:"total"`
}

// LoadSample is records a second at a time (unix milliseconds); kept
// whole, since CRDs discourage floats.
type LoadSample struct {
	T int64 `json:"t"`
	V int64 `json:"v"`
}

// Mark is an autoscaling recommendation, kept for the scale-down window
// so a restarted operator doesn't forget recent peaks.
type Mark struct {
	At      metav1.Time `json:"at"`
	Brokers int32       `json:"brokers"`
}

// HistoryEvent is something the operator did.
type HistoryEvent struct {
	At     metav1.Time `json:"at"`
	Kind   string      `json:"kind"`
	Detail string      `json:"detail"`
}

// KafkaServiceList is a list of KafkaServices.
type KafkaServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaService `json:"items"`
}

// Deep copies go through JSON: the types are small and change rarely,
// and this can't fall out of step with them the way hand-written copies
// (or generated ones nobody regenerated) can.

func (in *KafkaService) DeepCopy() *KafkaService {
	out := new(KafkaService)
	jsonCopy(in, out)
	return out
}

func (in *KafkaService) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *KafkaServiceList) DeepCopyObject() runtime.Object {
	out := new(KafkaServiceList)
	jsonCopy(in, out)
	return out
}

func jsonCopy(in, out any) {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err) // every field is plain data
	}
	if err := json.Unmarshal(b, out); err != nil {
		panic(err)
	}
}
