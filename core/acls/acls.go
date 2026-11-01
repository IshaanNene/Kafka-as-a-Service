// Package acls lists, creates and deletes a cluster's access-control
// entries one by one. kadm's builder sends the cross product of what it is
// given, which is right for "allow these users these operations" but not
// for applying an exact diff, so this package speaks the raw requests.
package acls

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ACL is one access-control entry, in the lower-case words people write in
// files: resource "topic", operation "read", permission "allow".
type ACL struct {
	Principal  string `json:"principal" yaml:"principal"`             // User:alice
	Host       string `json:"host" yaml:"host,omitempty"`             // "*" for any
	Resource   string `json:"resource" yaml:"resource"`               // topic, group, cluster, transactional-id, delegation-token, user
	Name       string `json:"name" yaml:"name"`                       // kafka-cluster for the cluster resource
	Pattern    string `json:"pattern" yaml:"pattern,omitempty"`       // literal or prefixed
	Operation  string `json:"operation" yaml:"operation"`             // read, write, create, delete, alter, describe, all, ...
	Permission string `json:"permission" yaml:"permission,omitempty"` // allow or deny
}

// Key identifies an entry: two ACLs with the same key are the same grant.
func (a ACL) Key() string {
	return strings.Join([]string{a.Principal, a.Host, a.Resource, a.Name, a.Pattern, a.Operation, a.Permission}, "\x00")
}

func (a ACL) String() string {
	name := a.Name
	if a.Pattern == "prefixed" {
		name += "*"
	}
	s := fmt.Sprintf("%s %s %s on %s %s", a.Permission, a.Principal, a.Operation, a.Resource, name)
	if a.Host != "*" {
		s += " from " + a.Host
	}
	return s
}

// Normalize fills defaults and checks every word, so two spellings of the
// same grant compare equal.
func Normalize(a ACL) (ACL, error) {
	if !strings.Contains(a.Principal, ":") {
		return a, fmt.Errorf("principal %q needs a type, like User:%s", a.Principal, a.Principal)
	}
	if a.Host == "" {
		a.Host = "*"
	}
	if a.Pattern == "" {
		a.Pattern = "literal"
	}
	if a.Permission == "" {
		a.Permission = "allow"
	}
	if a.Resource == "cluster" && a.Name == "" {
		a.Name = "kafka-cluster"
	}
	if a.Name == "" {
		return a, fmt.Errorf("%s ACL for %s needs a name", a.Resource, a.Principal)
	}
	if _, _, _, _, err := enums(a); err != nil {
		return a, err
	}
	rt, _ := kmsg.ParseACLResourceType(a.Resource)
	op, _ := kmsg.ParseACLOperation(a.Operation)
	a.Resource, a.Operation = word(rt.String()), word(op.String())
	a.Pattern, a.Permission = strings.ToLower(a.Pattern), strings.ToLower(a.Permission)
	return a, nil
}

// word turns kmsg's TRANSACTIONAL_ID or DESCRIBE_CONFIGS into
// transactional-id or describe-configs.
func word(s string) string {
	switch s {
	case "TRANSACTIONAL_ID":
		return "transactional-id"
	case "DELEGATION_TOKEN":
		return "delegation-token"
	}
	return strings.ReplaceAll(strings.ToLower(s), "_", "-")
}

func enums(a ACL) (kmsg.ACLResourceType, kmsg.ACLResourcePatternType, kmsg.ACLOperation, kmsg.ACLPermissionType, error) {
	rt, err := kmsg.ParseACLResourceType(a.Resource)
	if err != nil || rt <= kmsg.ACLResourceTypeAny {
		return 0, 0, 0, 0, fmt.Errorf("unknown resource %q: use topic, group, cluster, transactional-id, delegation-token or user", a.Resource)
	}
	pt, err := kmsg.ParseACLResourcePatternType(a.Pattern)
	if err != nil || (pt != kmsg.ACLResourcePatternTypeLiteral && pt != kmsg.ACLResourcePatternTypePrefixed) {
		return 0, 0, 0, 0, fmt.Errorf("unknown pattern %q: use literal or prefixed", a.Pattern)
	}
	op, err := kmsg.ParseACLOperation(a.Operation)
	if err != nil || op <= kmsg.ACLOperationAny {
		return 0, 0, 0, 0, fmt.Errorf("unknown operation %q", a.Operation)
	}
	perm, err := kmsg.ParseACLPermissionType(a.Permission)
	if err != nil || perm <= kmsg.ACLPermissionTypeAny {
		return 0, 0, 0, 0, fmt.Errorf("unknown permission %q: use allow or deny", a.Permission)
	}
	return rt, pt, op, perm, nil
}

// ErrNoAuthorizer is returned by List when the cluster has no authorizer
// configured, so it has no ACLs to list.
var ErrNoAuthorizer = errors.New("the cluster has no authorizer configured")

// List returns every ACL on the cluster, sorted by principal, resource and
// operation.
func List(ctx context.Context, cl kmsg.Requestor) ([]ACL, error) {
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if err := responseErr(resp.ErrorCode, resp.ErrorMessage); err != nil {
		return nil, err
	}
	var out []ACL
	for _, r := range resp.Resources {
		for _, e := range r.ACLs {
			out = append(out, ACL{
				Principal:  e.Principal,
				Host:       e.Host,
				Resource:   word(r.ResourceType.String()),
				Name:       r.ResourceName,
				Pattern:    strings.ToLower(r.ResourcePatternType.String()),
				Operation:  word(e.Operation.String()),
				Permission: strings.ToLower(e.PermissionType.String()),
			})
		}
	}
	Sort(out)
	return out, nil
}

// Sort orders ACLs by principal, then resource, name and operation.
func Sort(as []ACL) {
	sort.Slice(as, func(i, j int) bool {
		a, b := as[i], as[j]
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}
		if a.Resource != b.Resource {
			return a.Resource < b.Resource
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Key() < b.Key()
	})
}

// Create adds the given entries. ACLs must be normalized.
func Create(ctx context.Context, cl kmsg.Requestor, as []ACL) error {
	if len(as) == 0 {
		return nil
	}
	req := kmsg.NewPtrCreateACLsRequest()
	for _, a := range as {
		rt, pt, op, perm, err := enums(a)
		if err != nil {
			return err
		}
		c := kmsg.NewCreateACLsRequestCreation()
		c.ResourceType, c.ResourceName, c.ResourcePatternType = rt, a.Name, pt
		c.Principal, c.Host, c.Operation, c.PermissionType = a.Principal, a.Host, op, perm
		req.Creations = append(req.Creations, c)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for i, r := range resp.Results {
		if err := responseErr(r.ErrorCode, r.ErrorMessage); err != nil {
			return fmt.Errorf("%s: %w", as[i], err)
		}
	}
	return nil
}

// Delete removes exactly the given entries: each filter names every field,
// so nothing broader can match. ACLs must be normalized.
func Delete(ctx context.Context, cl kmsg.Requestor, as []ACL) error {
	if len(as) == 0 {
		return nil
	}
	req := kmsg.NewPtrDeleteACLsRequest()
	for _, a := range as {
		rt, pt, op, perm, err := enums(a)
		if err != nil {
			return err
		}
		f := kmsg.NewDeleteACLsRequestFilter()
		f.ResourceType, f.ResourceName, f.ResourcePatternType = rt, &a.Name, pt
		f.Principal, f.Host, f.Operation, f.PermissionType = &a.Principal, &a.Host, op, perm
		req.Filters = append(req.Filters, f)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for i, r := range resp.Results {
		if err := responseErr(r.ErrorCode, r.ErrorMessage); err != nil {
			return fmt.Errorf("%s: %w", as[i], err)
		}
	}
	return nil
}

func responseErr(code int16, msg *string) error {
	err := kerr.ErrorForCode(code)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.SecurityDisabled):
		return ErrNoAuthorizer
	case msg != nil && *msg != "":
		return fmt.Errorf("%w: %s", err, *msg)
	}
	return err
}
