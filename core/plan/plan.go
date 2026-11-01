package plan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/configs"
	"github.com/IshaanNene/Kafka-as-a-Service/core/quotas"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
)

// Kind is what an operation does.
type Kind string

// Operations, in the order a plan applies them: additions first, then
// removals, so a client never loses access while its replacement is added.
const (
	CreateTopic   Kind = "create-topic"
	AddPartitions Kind = "add-partitions"
	SetConfig     Kind = "set-config"
	UnsetConfig   Kind = "unset-config"
	CreateACL     Kind = "create-acl"
	SetQuota      Kind = "set-quota"
	RemoveQuota   Kind = "remove-quota"
	DeleteACL     Kind = "delete-acl"
	DeleteTopic   Kind = "delete-topic"
)

var order = map[Kind]int{
	CreateTopic: 0, AddPartitions: 1, SetConfig: 2, UnsetConfig: 3, CreateACL: 4,
	SetQuota: 5, RemoveQuota: 6, DeleteACL: 7, DeleteTopic: 8,
}

// Risk is how carefully a reviewer should read an operation.
type Risk string

// Risks.
const (
	Safe        Risk = "safe"        // adds something; nothing that works now stops working
	Caution     Risk = "caution"     // changes behaviour: keyed ordering, retention, a limit
	Destructive Risk = "destructive" // removes data or access
)

// Op is one change.
type Op struct {
	Kind    Kind   `json:"kind"`
	Target  string `json:"target"` // topic, ACL or quota entity
	Key     string `json:"key,omitempty"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Detail  string `json:"detail"`
	Risk    Risk   `json:"risk"`
	Problem string `json:"problem,omitempty"` // what the controller said when asked to validate it

	spec  topics.Spec
	grow  int // the partition count to grow to
	acl   acls.ACL
	quota quotas.Change
	conf  configs.Change
}

// Violation is a declared topic a policy refuses.
type Violation struct {
	Policy  string `json:"policy"`
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

// Plan is the changes that would make a cluster match its desired state.
type Plan struct {
	ID         string      `json:"id"` // a hash of the operations; apply refuses a plan that changed since review
	Cluster    string      `json:"cluster"`
	At         time.Time   `json:"at"`
	Ops        []Op        `json:"ops"`
	Violations []Violation `json:"violations"`
	Warnings   []string    `json:"warnings"`
	Errors     []string    `json:"errors"` // the plan can't be made whole: an unknown config, say
	Unmanaged  Unmanaged   `json:"unmanaged"`
	Checked    bool        `json:"checked"` // the controller validated each operation
}

// Unmanaged counts what the cluster has that the file doesn't declare and
// won't prune.
type Unmanaged struct {
	Topics int `json:"topics"`
	ACLs   int `json:"acls"`
	Quotas int `json:"quotas"`
}

// Blocked is why the plan can't be applied, or "".
func (p *Plan) Blocked() string {
	switch {
	case len(p.Errors) > 0:
		return p.Errors[0]
	case len(p.Violations) > 0:
		v := p.Violations[0]
		return fmt.Sprintf("policy %s refuses topic %s", v.Policy, v.Topic)
	}
	for _, o := range p.Ops {
		if o.Problem != "" {
			return fmt.Sprintf("%s %s: %s", o.Kind, o.Target, o.Problem)
		}
	}
	return ""
}

// Counts is how many operations of each risk the plan has.
func (p *Plan) Counts() map[Risk]int {
	n := map[Risk]int{}
	for _, o := range p.Ops {
		n[o.Risk]++
	}
	return n
}

// Live is what the cluster has now, for the parts a State can declare.
type Live struct {
	Topics  map[string]LiveTopic
	ACLs    []acls.ACL
	ACLErr  error // listing failed; ErrNoAuthorizer when there are no ACLs to manage
	Quotas  []quotas.Quota
	Configs map[string][]configs.Entry // for declared topics that manage configs
}

// LiveTopic is a topic's shape on the cluster.
type LiveTopic struct {
	Partitions  int32
	Replication int16
	Internal    bool
}

// Read fetches the live state a plan for s needs.
func Read(ctx context.Context, cl *kgo.Client, s State) (Live, error) {
	snap, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		return Live{}, err
	}
	l := Live{Topics: map[string]LiveTopic{}, Configs: map[string][]configs.Entry{}}
	for _, t := range snap.Topics {
		lt := LiveTopic{Partitions: int32(len(t.Partitions)), Internal: t.Internal}
		if len(t.Partitions) > 0 {
			lt.Replication = int16(len(t.Partitions[0].Replicas))
		}
		l.Topics[t.Name] = lt
	}
	for name, t := range s.Topics {
		if _, ok := l.Topics[name]; !ok || t.Configs == nil {
			continue
		}
		es, err := configs.Describe(ctx, cl, name, nil)
		if err != nil {
			return Live{}, fmt.Errorf("configs of %s: %w", name, err)
		}
		l.Configs[name] = es
	}
	if s.Manages.ACLs {
		l.ACLs, l.ACLErr = acls.List(ctx, cl)
	}
	if s.Manages.Quotas {
		if l.Quotas, err = quotas.List(ctx, kadm.NewClient(cl)); err != nil {
			return Live{}, fmt.Errorf("quotas: %w", err)
		}
	}
	return l, nil
}

// Diff is the plan that turns live into s. It is pure: the same inputs give
// the same plan and ID.
func Diff(s State, live Live) *Plan {
	p := &Plan{Cluster: s.Cluster, At: time.Now()}
	diffTopics(p, s, live)
	diffACLs(p, s, live)
	diffQuotas(p, s, live)
	p.Violations = checkPolicies(s)
	// Empty lists, not null, in JSON: readers iterate without checking.
	p.Ops = nonNil(p.Ops)
	p.Violations = nonNil(p.Violations)
	p.Warnings = nonNil(p.Warnings)
	p.Errors = nonNil(p.Errors)
	sort.SliceStable(p.Ops, func(i, j int) bool { return order[p.Ops[i].Kind] < order[p.Ops[j].Kind] })
	p.ID = id(p.Ops)
	return p
}

func diffTopics(p *Plan, s State, live Live) {
	names := sortedKeys(s.Topics)
	for _, name := range names {
		want := s.Topics[name]
		have, exists := live.Topics[name]
		if !exists {
			spec := topics.Spec{Name: name, Partitions: want.Partitions, ReplicationFactor: want.Replication, Configs: map[string]string{}}
			for k, v := range want.Configs {
				n, err := configs.Normalize(k, v)
				if err != nil {
					p.Errors = append(p.Errors, fmt.Sprintf("topic %s: %v", name, err))
					continue
				}
				spec.Configs[k] = n
			}
			p.Ops = append(p.Ops, Op{
				Kind: CreateTopic, Target: name, Risk: Safe, spec: spec,
				Detail: fmt.Sprintf("%s partitions × %s replicas%s", orDefault(int(want.Partitions)), orDefault(int(want.Replication)), configNote(len(spec.Configs))),
			})
			continue
		}
		switch {
		case want.Partitions > have.Partitions:
			p.Ops = append(p.Ops, Op{
				Kind: AddPartitions, Target: name, Risk: Caution, grow: int(want.Partitions),
				From: strconv.Itoa(int(have.Partitions)), To: strconv.Itoa(int(want.Partitions)),
				Detail: "keyed records move to new partitions from now on; consumer groups rebalance",
			})
		case want.Partitions > 0 && want.Partitions < have.Partitions:
			p.Warnings = append(p.Warnings, fmt.Sprintf("topic %s declares %d partitions but has %d; Kafka can't remove partitions", name, want.Partitions, have.Partitions))
		}
		if want.Replication > 0 && want.Replication != have.Replication {
			p.Warnings = append(p.Warnings, fmt.Sprintf("topic %s declares replication %d but has %d; changing it needs a partition reassignment, which plans don't make yet", name, want.Replication, have.Replication))
		}
		if want.Configs == nil {
			continue
		}
		cs, err := configs.Diff(live.Configs[name], configs.LevelTopic, want.Configs)
		if err != nil {
			p.Errors = append(p.Errors, fmt.Sprintf("topic %s: %v", name, err))
			continue
		}
		for _, c := range cs {
			o := Op{Kind: SetConfig, Target: name, Key: c.Key, From: c.From, To: c.To, Risk: Safe, conf: c}
			if c.Op == configs.OpUnset {
				o.Kind, o.Risk = UnsetConfig, Caution
				o.Detail = "not in the file; falls back to " + fallback(c.To)
			} else {
				o.Detail = "set on the topic"
				if c.From != "" {
					o.Detail = "changed on the topic"
				}
				if c.From != "" && shrinks(c.Key, c.From, c.To) {
					o.Risk = Caution
					o.Detail += "; less data is kept"
				}
			}
			p.Ops = append(p.Ops, o)
		}
	}
	for _, name := range sortedKeys(live.Topics) {
		t := live.Topics[name]
		if _, declared := s.Topics[name]; declared || t.Internal || strings.HasPrefix(name, "__") {
			continue
		}
		if s.Prune.Topics && s.Manages.Topics {
			p.Ops = append(p.Ops, Op{
				Kind: DeleteTopic, Target: name, Risk: Destructive,
				Detail: fmt.Sprintf("not in the file; its %d partitions and every record are deleted", t.Partitions),
			})
			continue
		}
		p.Unmanaged.Topics++
	}
}

func diffACLs(p *Plan, s State, live Live) {
	if !s.Manages.ACLs {
		return
	}
	if live.ACLErr != nil {
		if errors.Is(live.ACLErr, acls.ErrNoAuthorizer) && len(s.ACLs) == 0 {
			return
		}
		p.Errors = append(p.Errors, "ACLs: "+live.ACLErr.Error())
		return
	}
	have := map[string]bool{}
	for _, a := range live.ACLs {
		have[a.Key()] = true
	}
	want := map[string]bool{}
	for _, a := range s.ACLs {
		want[a.Key()] = true
		if !have[a.Key()] {
			p.Ops = append(p.Ops, Op{Kind: CreateACL, Target: a.String(), Risk: Safe, acl: a, Detail: "grants " + a.Principal + " " + a.Operation})
		}
	}
	for _, a := range live.ACLs {
		if want[a.Key()] {
			continue
		}
		if !s.Prune.ACLs {
			p.Unmanaged.ACLs++
			continue
		}
		detail := a.Principal + " loses " + a.Operation + " on " + a.Name
		if a.Permission == "deny" {
			detail = "lifts a deny: " + a.Principal + " may " + a.Operation + " on " + a.Name + " again"
		}
		p.Ops = append(p.Ops, Op{Kind: DeleteACL, Target: a.String(), Risk: Destructive, acl: a, Detail: detail})
	}
}

func diffQuotas(p *Plan, s State, live Live) {
	if !s.Manages.Quotas {
		return
	}
	have := map[string]quotas.Quota{}
	for _, q := range live.Quotas {
		have[q.Entity()] = q
	}
	want := map[string]bool{}
	for _, q := range s.Quotas {
		want[q.Entity()] = true
		cur := have[q.Entity()]
		for _, k := range sortedKeys(q.Values) {
			v := q.Values[k]
			if old, ok := cur.Values[k]; ok && old == v {
				continue
			}
			o := Op{Kind: SetQuota, Target: q.Entity(), Key: k, To: rate(k, v), Risk: Safe, quota: quotas.Change{Quota: quotaOf(q, k, v)}}
			if old, ok := cur.Values[k]; ok {
				o.From = rate(k, old)
				if v < old {
					o.Risk = Caution
					o.Detail = "a lower limit: clients over it are throttled"
				}
			}
			if o.Detail == "" {
				o.Detail = "clients over it are throttled"
			}
			p.Ops = append(p.Ops, o)
		}
		for _, k := range sortedKeys(cur.Values) {
			if _, kept := q.Values[k]; kept {
				continue
			}
			p.Ops = append(p.Ops, Op{
				Kind: RemoveQuota, Target: q.Entity(), Key: k, From: rate(k, cur.Values[k]), Risk: Caution,
				quota: quotas.Change{Quota: quotaOf(q, "", 0), Remove: []string{k}}, Detail: "not in the file; the limit is lifted",
			})
		}
	}
	for _, q := range live.Quotas {
		if want[q.Entity()] {
			continue
		}
		if !s.Prune.Quotas {
			p.Unmanaged.Quotas++
			continue
		}
		for _, k := range sortedKeys(q.Values) {
			p.Ops = append(p.Ops, Op{
				Kind: RemoveQuota, Target: q.Entity(), Key: k, From: rate(k, q.Values[k]), Risk: Caution,
				quota: quotas.Change{Quota: quotaOf(q, "", 0), Remove: []string{k}}, Detail: "entity not in the file; the limit is lifted",
			})
		}
	}
}

// quotaOf is q's entity with one value to set, or none.
func quotaOf(q quotas.Quota, k string, v float64) quotas.Quota {
	out := q
	out.Values = map[string]float64{}
	if k != "" {
		out.Values[k] = v
	}
	return out
}

// Check asks the controller to validate every operation it can without
// changing anything, recording each refusal on its operation.
func Check(ctx context.Context, cl *kgo.Client, p *Plan) {
	adm := kadm.NewClient(cl)
	byTopic := map[string][]int{}
	for i := range p.Ops {
		o := &p.Ops[i]
		var err error
		switch o.Kind {
		case CreateTopic:
			_, err = topics.Create(ctx, adm, o.spec, true)
		case AddPartitions:
			err = topics.Grow(ctx, adm, o.Target, o.grow, true)
		case SetConfig, UnsetConfig:
			byTopic[o.Target] = append(byTopic[o.Target], i)
		case SetQuota, RemoveQuota:
			err = quotas.Apply(ctx, adm, []quotas.Change{o.quota}, true)
		}
		if err != nil {
			o.Problem = err.Error()
		}
	}
	for topic, idx := range byTopic {
		cs := make([]configs.Change, len(idx))
		for j, i := range idx {
			cs[j] = p.Ops[i].conf
		}
		if err := configs.Apply(ctx, adm, topic, nil, cs, true); err != nil {
			for _, i := range idx {
				p.Ops[i].Problem = err.Error()
			}
		}
	}
	p.Checked = true
}

// Result is what happened to one operation.
type Result struct {
	Op   Op     `json:"op"`
	Done bool   `json:"done"`
	Err  string `json:"error,omitempty"`
}

// ErrBlocked is returned by Apply for a plan with errors, violations or
// operations the controller refused.
var ErrBlocked = errors.New("the plan is blocked")

// Apply makes the plan's changes in order and stops at the first failure.
// Every operation gets a result; those after a failure are not done.
func Apply(ctx context.Context, cl *kgo.Client, p *Plan) ([]Result, error) {
	if why := p.Blocked(); why != "" {
		return nil, fmt.Errorf("%w: %s", ErrBlocked, why)
	}
	adm := kadm.NewClient(cl)
	out := make([]Result, len(p.Ops))
	for i, o := range p.Ops {
		out[i].Op = o
	}
	var failed error
	for i := 0; i < len(p.Ops) && failed == nil; i++ {
		o := p.Ops[i]
		// A topic's config changes go in one request, as Kafka applies
		// them together.
		if o.Kind == SetConfig || o.Kind == UnsetConfig {
			j := i
			var cs []configs.Change
			for ; j < len(p.Ops) && (p.Ops[j].Kind == SetConfig || p.Ops[j].Kind == UnsetConfig) && p.Ops[j].Target == o.Target; j++ {
				cs = append(cs, p.Ops[j].conf)
			}
			failed = configs.Apply(ctx, adm, o.Target, nil, cs, false)
			for k := i; k < j; k++ {
				out[k].Done = failed == nil
				if failed != nil {
					out[k].Err = failed.Error()
				}
			}
			i = j - 1
			continue
		}
		switch o.Kind {
		case CreateTopic:
			_, failed = topics.Create(ctx, adm, o.spec, false)
		case AddPartitions:
			failed = topics.Grow(ctx, adm, o.Target, o.grow, false)
		case CreateACL:
			failed = acls.Create(ctx, cl, []acls.ACL{o.acl})
		case DeleteACL:
			failed = acls.Delete(ctx, cl, []acls.ACL{o.acl})
		case SetQuota, RemoveQuota:
			failed = quotas.Apply(ctx, adm, []quotas.Change{o.quota}, false)
		case DeleteTopic:
			var errs map[string]error
			if errs, failed = topics.Delete(ctx, adm, []string{o.Target}); failed == nil {
				failed = errs[o.Target]
			}
		}
		out[i].Done = failed == nil
		if failed != nil {
			out[i].Err = failed.Error()
		}
	}
	return out, failed
}

// The ID covers what each operation does, so a plan reviewed in the console
// is applied only if a fresh diff gives the same operations.
func id(ops []Op) string {
	h := sha256.New()
	for _, o := range ops {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\n", o.Kind, o.Target, o.Key, o.From, o.To)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

var policyEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(cel.Variable("topic", cel.MapType(cel.StringType, cel.DynType)), cel.OptionalTypes())
})

// costLimit bounds one policy evaluation, as filters are bounded (ADR-0005).
const costLimit = 100_000

func compile(rule string) (cel.Program, error) {
	env, err := policyEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(rule)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("%q is a %s; want true or false", rule, t)
	}
	return env.Program(ast, cel.CostLimit(costLimit))
}

func checkPolicies(s State) []Violation {
	var out []Violation
	for _, pol := range s.Policies {
		prg, err := compile(pol.Rule)
		if err != nil {
			continue // Parse already refused it
		}
		for _, name := range sortedKeys(s.Topics) {
			t := s.Topics[name]
			cfg := map[string]string{}
			for k, v := range t.Configs {
				if n, err := configs.Normalize(k, v); err == nil {
					v = n
				}
				cfg[k] = v
			}
			vars := map[string]any{"topic": map[string]any{
				"name": name, "partitions": int64(t.Partitions), "replication": int64(t.Replication), "configs": cfg,
			}}
			res, _, err := prg.Eval(vars)
			msg := pol.Message
			switch {
			case err != nil:
				msg = "the rule failed: " + err.Error()
			case res.Value() == true:
				continue
			case msg == "":
				msg = "fails " + pol.Rule
			}
			out = append(out, Violation{Policy: pol.Name, Topic: name, Message: msg})
		}
	}
	return out
}

func nonNil[T any](xs []T) []T {
	if xs == nil {
		return []T{}
	}
	return xs
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func orDefault(n int) string {
	if n <= 0 {
		return "default"
	}
	return strconv.Itoa(n)
}

func configNote(n int) string {
	switch n {
	case 0:
		return ""
	case 1:
		return ", 1 config"
	}
	return fmt.Sprintf(", %d configs", n)
}

func fallback(to string) string {
	if to == "" {
		return "the broker default"
	}
	return to
}

// shrinks is whether a retention change keeps less: a smaller positive
// number, or a limit where there was none (-1).
func shrinks(key, from, to string) bool {
	if !strings.HasPrefix(key, "retention.") {
		return false
	}
	f, err1 := strconv.ParseInt(from, 10, 64)
	t, err2 := strconv.ParseInt(to, 10, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	return (f < 0 && t >= 0) || (t >= 0 && t < f)
}

// rate shows a byte-rate quota in MiB/s or KiB/s when it is a whole number
// of them.
func rate(key string, v float64) string {
	if strings.HasSuffix(key, "_byte_rate") && v == float64(int64(v)) {
		switch n := int64(v); {
		case n >= 1<<20 && n%(1<<20) == 0:
			return fmt.Sprintf("%d MiB/s", n>>20)
		case n >= 1<<10 && n%(1<<10) == 0:
			return fmt.Sprintf("%d KiB/s", n>>10)
		}
		return strconv.FormatInt(int64(v), 10) + " B/s"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
