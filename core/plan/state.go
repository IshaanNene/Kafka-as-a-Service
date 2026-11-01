// Package plan turns a cluster's desired state, kept in git, into the
// ordered changes that would make the live cluster match it, checks them
// against policies and the controller, and applies them.
//
// A desired-state file declares topics, ACLs and quotas:
//
//	cluster: prod-eu
//	topics:
//	  orders:
//	    partitions: 12
//	    replication: 3
//	    configs: {retention.ms: 7d, cleanup.policy: delete}
//	acls:
//	  - principal: User:billing
//	    resource: topic
//	    name: orders
//	    operations: [read, describe]
//	quotas:
//	  - user: billing
//	    values: {consumer_byte_rate: 10MiB}
//	policies:
//	  - name: replicated
//	    rule: topic.replication >= 3
//	prune: {acls: true}
//
// Nothing is deleted unless prune says so for that kind. A topic that
// declares configs has every topic-level config managed: one set on the
// cluster but missing from the file is planned for removal.
package plan

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/quotas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
)

// State is a cluster's desired state.
type State struct {
	Cluster  string               `json:"cluster" yaml:"cluster"`
	Topics   map[string]TopicSpec `json:"topics" yaml:"topics"`
	ACLs     []acls.ACL           `json:"acls" yaml:"-"`
	Quotas   []quotas.Quota       `json:"quotas" yaml:"-"`
	Policies []Policy             `json:"policies" yaml:"policies"`
	Prune    Prune                `json:"prune" yaml:"prune"`

	// Manages is which kinds the file declares at all. A file without an
	// acls key leaves the cluster's ACLs alone even with prune set.
	Manages Kinds `json:"manages" yaml:"-"`
}

// TopicSpec is one topic's declared shape. Configs nil means its configs
// aren't managed; an empty map means none may be set on the topic.
type TopicSpec struct {
	Partitions  int32             `json:"partitions" yaml:"partitions"`
	Replication int16             `json:"replication" yaml:"replication"`
	Configs     map[string]string `json:"configs,omitempty" yaml:"configs"`
}

// Policy is a CEL rule every declared topic must pass, such as
// "topic.replication >= 3". It sees topic.name, topic.partitions,
// topic.replication and topic.configs.
type Policy struct {
	Name    string `json:"name" yaml:"name"`
	Rule    string `json:"rule" yaml:"rule"`
	Message string `json:"message,omitempty" yaml:"message"`
}

// Prune says which kinds may be deleted when the cluster has one the file
// doesn't declare.
type Prune struct {
	Topics bool `json:"topics" yaml:"topics"`
	ACLs   bool `json:"acls" yaml:"acls"`
	Quotas bool `json:"quotas" yaml:"quotas"`
}

// Kinds is a set of the kinds a file manages.
type Kinds struct {
	Topics bool `json:"topics"`
	ACLs   bool `json:"acls"`
	Quotas bool `json:"quotas"`
}

// The file's shape for ACLs and quotas is friendlier than the types: an ACL
// lists its operations, and quota values take sizes like 10MiB.
type file struct {
	Cluster  string               `yaml:"cluster"`
	Topics   map[string]TopicSpec `yaml:"topics"`
	ACLs     *[]aclSpec           `yaml:"acls"`
	Quotas   *[]quotaSpec         `yaml:"quotas"`
	Policies []Policy             `yaml:"policies"`
	Prune    Prune                `yaml:"prune"`
}

type aclSpec struct {
	Principal  string   `yaml:"principal"`
	Host       string   `yaml:"host"`
	Resource   string   `yaml:"resource"`
	Name       string   `yaml:"name"`
	Pattern    string   `yaml:"pattern"`
	Operation  string   `yaml:"operation"`
	Operations []string `yaml:"operations"`
	Permission string   `yaml:"permission"`
}

type quotaSpec struct {
	User            string            `yaml:"user"`
	ClientID        string            `yaml:"client_id"`
	IP              string            `yaml:"ip"`
	DefaultUser     bool              `yaml:"default_user"`
	DefaultClientID bool              `yaml:"default_client_id"`
	Values          map[string]string `yaml:"values"`
}

// Load reads a desired-state file.
func Load(path string) (State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return State{}, err
	}
	s, err := Parse(b)
	if err != nil {
		return State{}, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// Parse reads a desired-state document, rejecting unknown keys and
// checking every name, ACL and quota.
func Parse(b []byte) (State, error) {
	var f file
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	// An empty document is io.EOF; it declares nothing, which is valid.
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return State{}, err
	}
	s := State{
		Cluster: f.Cluster, Topics: f.Topics, Policies: f.Policies, Prune: f.Prune,
		Manages: Kinds{Topics: f.Topics != nil, ACLs: f.ACLs != nil, Quotas: f.Quotas != nil},
	}
	if s.Topics == nil {
		s.Topics = map[string]TopicSpec{}
	}
	for name, t := range s.Topics {
		if err := topics.ValidName(name); err != nil {
			return State{}, fmt.Errorf("topic %q: %w", name, err)
		}
		if t.Partitions < 0 || t.Replication < 0 {
			return State{}, fmt.Errorf("topic %s: partitions and replication can't be negative", name)
		}
	}
	if f.ACLs != nil {
		seen := map[string]bool{}
		for i, a := range *f.ACLs {
			ops := a.Operations
			if a.Operation != "" {
				ops = append(ops, a.Operation)
			}
			if len(ops) == 0 {
				return State{}, fmt.Errorf("acls[%d]: give operation or operations", i)
			}
			for _, op := range ops {
				n, err := acls.Normalize(acls.ACL{
					Principal: a.Principal, Host: a.Host, Resource: a.Resource, Name: a.Name,
					Pattern: a.Pattern, Operation: op, Permission: a.Permission,
				})
				if err != nil {
					return State{}, fmt.Errorf("acls[%d]: %w", i, err)
				}
				if !seen[n.Key()] {
					seen[n.Key()] = true
					s.ACLs = append(s.ACLs, n)
				}
			}
		}
		acls.Sort(s.ACLs)
	}
	if f.Quotas != nil {
		seen := map[string]bool{}
		for i, q := range *f.Quotas {
			out := quotas.Quota{
				User: q.User, ClientID: q.ClientID, IP: q.IP,
				DefaultUser: q.DefaultUser, DefaultClientID: q.DefaultClientID,
				Values: map[string]float64{},
			}
			for k, v := range q.Values {
				n, err := quotaValue(k, v)
				if err != nil {
					return State{}, fmt.Errorf("quotas[%d]: %w", i, err)
				}
				out.Values[k] = n
			}
			if err := quotas.Check(out); err != nil {
				return State{}, fmt.Errorf("quotas[%d]: %w", i, err)
			}
			if seen[out.Entity()] {
				return State{}, fmt.Errorf("quotas[%d]: %s is declared twice", i, out.Entity())
			}
			seen[out.Entity()] = true
			s.Quotas = append(s.Quotas, out)
		}
		sort.Slice(s.Quotas, func(i, j int) bool { return s.Quotas[i].Entity() < s.Quotas[j].Entity() })
	}
	for i, p := range s.Policies {
		if p.Name == "" || p.Rule == "" {
			return State{}, fmt.Errorf("policies[%d]: a policy needs a name and a rule", i)
		}
		if _, err := compile(p.Rule); err != nil {
			return State{}, fmt.Errorf("policy %s: %w", p.Name, err)
		}
	}
	return s, nil
}

// quotaValue reads a number, or a byte rate like 10MiB for the byte-rate
// keys.
func quotaValue(key, v string) (float64, error) {
	v = strings.TrimSpace(v)
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("%s can't be negative", key)
		}
		return n, nil
	}
	if !strings.HasSuffix(key, "_byte_rate") {
		return 0, fmt.Errorf("%s takes a number, not %q", key, v)
	}
	units := []struct {
		suffix string
		mul    float64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(v, u.suffix); ok {
			n, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("%s takes a byte rate like 10MiB, not %q", key, v)
			}
			return n * u.mul, nil
		}
	}
	return 0, fmt.Errorf("%s takes a byte rate like 10MiB, not %q", key, v)
}
