package topics

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Spec is a topic to create. Zero Partitions or ReplicationFactor takes
// the broker's num.partitions or default.replication.factor (KIP-464).
type Spec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// Created is what the controller made, or would make for a dry run, with
// the broker defaults filled in.
type Created struct {
	ID                string // as kafka-topics prints it; empty for a dry run, or before Kafka 2.8
	Partitions        int32
	ReplicationFactor int16
}

// Create asks the controller to create spec. With dry, it only checks it.
func Create(ctx context.Context, adm *kadm.Client, spec Spec, dry bool) (Created, error) {
	p, rf := spec.Partitions, spec.ReplicationFactor
	if p == 0 {
		p = -1
	}
	if rf == 0 {
		rf = -1
	}
	configs := map[string]*string{}
	for k, v := range spec.Configs {
		configs[k] = &v
	}
	create := adm.CreateTopics
	if dry {
		create = adm.ValidateCreateTopics
	}
	rs, err := create(ctx, p, rf, configs, spec.Name)
	if err != nil {
		return Created{}, err
	}
	r, ok := rs[spec.Name]
	switch {
	case !ok:
		return Created{}, fmt.Errorf("the controller did not answer for %s", spec.Name)
	case r.Err != nil && r.ErrMessage != "":
		return Created{}, fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
	case r.Err != nil:
		return Created{}, r.Err
	}
	c := Created{Partitions: r.NumPartitions, ReplicationFactor: r.ReplicationFactor}
	if !dry && r.ID != (kadm.TopicID{}) {
		c.ID = base64.RawURLEncoding.EncodeToString(r.ID[:]) // as kafka-topics prints it
	}
	return c, nil
}

// MaxNameLen is the longest topic name Kafka accepts.
const MaxNameLen = 249

// ValidName applies Kafka's topic name rules: 1 to 249 of a-z, A-Z, 0-9,
// '.', '_' and '-', and not "." or "..".
func ValidName(name string) error {
	switch {
	case name == "":
		return errors.New("a topic needs a name")
	case name == "." || name == "..":
		return fmt.Errorf("%q is reserved", name)
	case len(name) > MaxNameLen:
		return fmt.Errorf("%d characters; at most %d", len(name), MaxNameLen)
	}
	for _, r := range name {
		if !legal(r) {
			return fmt.Errorf("%q is not allowed: use letters, digits, '.', '_' and '-'", r)
		}
	}
	return nil
}

func legal(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-", r)
}

// Collides is an existing topic name that Kafka's metrics can't tell from
// name, because '.' and '_' both become '_' there; "" if none. Kafka
// refuses such a topic.
func Collides(name string, existing []string) string {
	flat := strings.ReplaceAll(name, ".", "_")
	for _, e := range existing {
		if e != name && strings.ReplaceAll(e, ".", "_") == flat {
			return e
		}
	}
	return ""
}
