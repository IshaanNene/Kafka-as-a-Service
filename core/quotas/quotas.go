// Package quotas lists and changes client quotas: the byte rates and
// request share Kafka allows a user, a client ID, or a pair of them.
package quotas

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Quota is the limits on one entity. An empty User or ClientID with the
// matching Default flag set means the default for every user or client.
type Quota struct {
	User            string             `json:"user,omitempty" yaml:"user,omitempty"`
	ClientID        string             `json:"client_id,omitempty" yaml:"client_id,omitempty"`
	IP              string             `json:"ip,omitempty" yaml:"ip,omitempty"`
	DefaultUser     bool               `json:"default_user,omitempty" yaml:"default_user,omitempty"`
	DefaultClientID bool               `json:"default_client_id,omitempty" yaml:"default_client_id,omitempty"`
	Values          map[string]float64 `json:"values" yaml:"values"`
}

// Keys Kafka accepts as quota values.
var Keys = []string{
	"producer_byte_rate",
	"consumer_byte_rate",
	"request_percentage",
	"controller_mutation_rate",
	"connection_creation_rate",
}

// Entity is a stable name for the quota's entity, such as
// "user=billing,client-id=<default>".
func (q Quota) Entity() string {
	var parts []string
	switch {
	case q.DefaultUser:
		parts = append(parts, "user=<default>")
	case q.User != "":
		parts = append(parts, "user="+q.User)
	}
	switch {
	case q.DefaultClientID:
		parts = append(parts, "client-id=<default>")
	case q.ClientID != "":
		parts = append(parts, "client-id="+q.ClientID)
	}
	if q.IP != "" {
		parts = append(parts, "ip="+q.IP)
	}
	return strings.Join(parts, ",")
}

// Check reports an entity Kafka would refuse, or an unknown key.
func Check(q Quota) error {
	if q.Entity() == "" {
		return fmt.Errorf("a quota needs a user, client_id or ip")
	}
	if q.IP != "" && (q.User != "" || q.ClientID != "" || q.DefaultUser || q.DefaultClientID) {
		return fmt.Errorf("%s: an ip quota can't be combined with a user or client", q.Entity())
	}
	for k := range q.Values {
		if !known(k) {
			return fmt.Errorf("%s: unknown quota %q; use one of %s", q.Entity(), k, strings.Join(Keys, ", "))
		}
	}
	return nil
}

func known(k string) bool {
	for _, x := range Keys {
		if x == k {
			return true
		}
	}
	return false
}

func (q Quota) entity() kadm.ClientQuotaEntity {
	var e kadm.ClientQuotaEntity
	add := func(typ, name string, def bool) {
		c := kadm.ClientQuotaEntityComponent{Type: typ}
		if !def {
			c.Name = &name
		}
		e = append(e, c)
	}
	if q.User != "" || q.DefaultUser {
		add("user", q.User, q.DefaultUser)
	}
	if q.ClientID != "" || q.DefaultClientID {
		add("client-id", q.ClientID, q.DefaultClientID)
	}
	if q.IP != "" {
		add("ip", q.IP, false)
	}
	return e
}

// List returns every quota on the cluster, sorted by entity.
func List(ctx context.Context, adm *kadm.Client) ([]Quota, error) {
	described, err := adm.DescribeClientQuotas(ctx, false, nil)
	if err != nil {
		return nil, err
	}
	out := make([]Quota, 0, len(described))
	for _, d := range described {
		q := Quota{Values: map[string]float64{}}
		for _, c := range d.Entity {
			name, def := "", c.Name == nil
			if !def {
				name = *c.Name
			}
			switch c.Type {
			case "user":
				q.User, q.DefaultUser = name, def
			case "client-id":
				q.ClientID, q.DefaultClientID = name, def
			case "ip":
				q.IP = name
			}
		}
		for _, v := range d.Values {
			q.Values[v.Key] = v.Value
		}
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Entity() < out[j].Entity() })
	return out, nil
}

// Change is what to set and remove on one entity.
type Change struct {
	Quota  Quota    // the entity, and the values to set
	Remove []string // keys to remove
}

// Apply sends the changes; with validate the broker checks them and changes
// nothing.
func Apply(ctx context.Context, adm *kadm.Client, cs []Change, validate bool) error {
	if len(cs) == 0 {
		return nil
	}
	entries := make([]kadm.AlterClientQuotaEntry, 0, len(cs))
	for _, c := range cs {
		e := kadm.AlterClientQuotaEntry{Entity: c.Quota.entity()}
		keys := make([]string, 0, len(c.Quota.Values))
		for k := range c.Quota.Values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			e.Ops = append(e.Ops, kadm.AlterClientQuotaOp{Key: k, Value: c.Quota.Values[k]})
		}
		for _, k := range c.Remove {
			e.Ops = append(e.Ops, kadm.AlterClientQuotaOp{Key: k, Remove: true})
		}
		entries = append(entries, e)
	}
	alter := adm.AlterClientQuotas
	if validate {
		alter = adm.ValidateAlterClientQuotas
	}
	rs, err := alter(ctx, entries)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("%s: %w: %s", r.Entity, r.Err, r.ErrMessage)
			}
			return fmt.Errorf("%s: %w", r.Entity, r.Err)
		}
	}
	return nil
}
