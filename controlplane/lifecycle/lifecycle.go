// Package lifecycle is the contract between the console and whatever runs
// Kafka clusters for it: create a cluster, roll its brokers, resize it by
// hand or let it size itself to its load.
//
// The Kubernetes operator (package operator) implements it over
// KafkaService resources; Demo implements it in memory so the console's
// pages can be tried, and tested, without a Kubernetes cluster.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Manager creates, rolls and resizes managed clusters.
type Manager interface {
	// Services lists every managed cluster, by name.
	Services(ctx context.Context) ([]Service, error)
	// Create asks for a new cluster. It returns once the request is
	// recorded; provisioning shows up in the service's phase.
	Create(ctx context.Context, s Spec) error
	// Roll restarts every broker, one at a time, keeping partitions
	// available throughout.
	Roll(ctx context.Context, name string) error
	// Scale sets the broker count and turns autoscaling off.
	Scale(ctx context.Context, name string, brokers int) error
	// SetAutoscale turns autoscaling on with p, or off when p is nil.
	SetAutoscale(ctx context.Context, name string, p *Autoscale) error
	// Where says where the clusters run, for the page header: a
	// Kubernetes namespace and context, or "demo".
	Where() string
}

// Phase is where a cluster is in its life.
type Phase string

const (
	Provisioning Phase = "provisioning" // being created; not serving yet
	Ready        Phase = "ready"        // every broker up and serving
	Rolling      Phase = "rolling"      // brokers restarting one at a time
	Scaling      Phase = "scaling"      // brokers being added or removed, partitions moving
	Degraded     Phase = "degraded"     // serving, but brokers are missing
	Failed       Phase = "failed"       // can't be made to match its spec; Message says why
)

// Spec is what someone asks for when they create a cluster.
type Spec struct {
	Name    string `json:"name"`
	Version string `json:"version"` // Kafka version, e.g. 4.2.0
	Brokers int    `json:"brokers"`
	// Storage is each broker's disk, as a Kubernetes quantity (100Gi).
	Storage   string     `json:"storage"`
	Autoscale *Autoscale `json:"autoscale,omitempty"`
}

// Autoscale sizes a cluster to its load: enough brokers that each takes
// at most Target records a second, between Min and Max.
type Autoscale struct {
	Min    int `json:"min"`
	Max    int `json:"max"`
	Target int `json:"target"` // records a second per broker
	// ScaleDownAfter is how long load must stay low before brokers are
	// removed. Adding brokers doesn't wait.
	ScaleDownAfter Duration `json:"scaleDownAfter"`
}

// Service is a managed cluster as the console shows it.
type Service struct {
	Name      string    `json:"name"`
	Namespace string    `json:"namespace,omitempty"`
	Phase     Phase     `json:"phase"`
	Message   string    `json:"message,omitempty"` // why it's degraded or failed, or what it's doing
	Version   string    `json:"version"`
	Storage   string    `json:"storage"`
	Bootstrap string    `json:"bootstrap,omitempty"`
	Created   time.Time `json:"created"`

	Brokers Brokers    `json:"brokers"`
	Auto    *Autoscale `json:"autoscale,omitempty"`
	// Load is the measured records a second, oldest first; empty until
	// the cluster has been measured.
	Load []Sample `json:"load"`
	// Recommended is the broker count autoscaling would pick for the
	// current load, and why; set even when autoscaling is off.
	Recommended int    `json:"recommended,omitempty"`
	Reason      string `json:"reason,omitempty"`
	// Roll is the restart in progress, if any.
	Roll    *Progress `json:"roll,omitempty"`
	History []Event   `json:"history"` // newest first
}

// Brokers counts brokers.
type Brokers struct {
	Desired int `json:"desired"`
	Ready   int `json:"ready"`
	// Nodes is each broker, in node ID order.
	Nodes []Node `json:"nodes"`
}

// Node is one broker.
type Node struct {
	ID    int    `json:"id"`
	Ready bool   `json:"ready"`
	State string `json:"state"` // running, starting, restarting, stopping, pending
}

// Progress counts through a roll.
type Progress struct {
	Done  int `json:"done"`
	Total int `json:"total"`
}

// Sample is one load measurement.
type Sample struct {
	T int64   `json:"t"` // unix milliseconds
	V float64 `json:"v"` // records a second
}

// Event is something that happened to a cluster.
type Event struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"` // created, rolled, scaled, autoscaled, failed
	Detail string    `json:"detail"`
}

// Duration is a time.Duration that reads and writes as "10m".
type Duration struct{ time.Duration }

func (d Duration) MarshalJSON() ([]byte, error) { return []byte(`"` + Short(d.Duration) + `"`), nil }

// Short is a duration the way people write it: 10m, 1h30m, 45s.
func Short(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) < 2 || b[0] != '"' {
		return errors.New("a duration is a string such as \"10m\"")
	}
	v, err := time.ParseDuration(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// Limits on what can be asked for.
const (
	MinBrokers = 1
	MaxBrokers = 30
)

// name is a DNS label short enough that Strimzi's pod names, which
// repeat it (orders-orders-controllers-12), stay valid 63-character
// hostnames.
var name = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,18}[a-z0-9])?$`)

var version = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

var quantity = regexp.MustCompile(`^\d+(Mi|Gi|Ti)$`)

// Validate says what's wrong with s, or nil.
func (s Spec) Validate() error {
	switch {
	case !name.MatchString(s.Name):
		return invalid("name %q: use lowercase letters, digits and dashes, starting with a letter, at most 20", s.Name)
	case !version.MatchString(s.Version):
		return invalid("version %q: give a Kafka version such as 4.2.0", s.Version)
	case !quantity.MatchString(s.Storage):
		return invalid("storage %q: give a size such as 100Gi", s.Storage)
	case s.Brokers < MinBrokers || s.Brokers > MaxBrokers:
		return invalid("brokers: between %d and %d", MinBrokers, MaxBrokers)
	}
	if s.Autoscale != nil {
		return s.Autoscale.Validate()
	}
	return nil
}

// Validate says what's wrong with a, or nil.
func (a Autoscale) Validate() error {
	switch {
	case a.Min < MinBrokers || a.Max > MaxBrokers:
		return invalid("autoscale: between %d and %d brokers", MinBrokers, MaxBrokers)
	case a.Min > a.Max:
		return invalid("autoscale: min %d is above max %d", a.Min, a.Max)
	case a.Target <= 0:
		return invalid("autoscale: target records a second per broker must be above zero")
	case a.ScaleDownAfter.Duration < 0:
		return invalid("autoscale: scale-down delay can't be negative")
	}
	return nil
}

// ValidateScale says what's wrong with a manual broker count, or nil.
func ValidateScale(brokers int) error {
	if brokers < MinBrokers || brokers > MaxBrokers {
		return invalid("brokers: between %d and %d", MinBrokers, MaxBrokers)
	}
	return nil
}

// Invalid is a request that fails validation; nothing was changed.
type Invalid struct{ msg string }

func (e Invalid) Error() string { return e.msg }

func invalid(format string, a ...any) error { return Invalid{fmt.Sprintf(format, a...)} }

// ErrNotFound is returned for a cluster that doesn't exist.
var ErrNotFound = errors.New("no such managed cluster")

// ErrExists is returned when creating a cluster whose name is taken.
var ErrExists = errors.New("a managed cluster by that name already exists")

// ErrBusy is returned when a change would collide with one in progress,
// such as rolling a cluster that is already rolling.
var ErrBusy = errors.New("the cluster is busy with another change")
