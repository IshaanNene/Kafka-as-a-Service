package plan

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/acls"
	"github.com/IshaanNene/Kafka-as-a-Service/core/quotas"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

const doc = `
cluster: test
topics:
  orders:
    partitions: 6
    replication: 1
    configs:
      retention.ms: 2d
      cleanup.policy: delete
  payments:
    partitions: 3
    replication: 1
acls:
  - principal: User:billing
    resource: topic
    name: orders
    operations: [read, describe]
  - principal: User:billing
    resource: group
    name: billing
    operation: read
quotas:
  - user: billing
    values: {consumer_byte_rate: 10MiB, request_percentage: "50"}
policies:
  - name: enough-partitions
    rule: topic.partitions >= 3
    message: every topic needs at least 3 partitions
prune: {acls: true}
`

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestParse(t *testing.T) {
	s, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Topics) != 2 || s.Topics["orders"].Partitions != 6 {
		t.Fatalf("topics: %+v", s.Topics)
	}
	if len(s.ACLs) != 3 {
		t.Fatalf("operations expand to one ACL each: %+v", s.ACLs)
	}
	for _, a := range s.ACLs {
		if a.Host != "*" || a.Pattern != "literal" || a.Permission != "allow" {
			t.Errorf("defaults not filled: %+v", a)
		}
	}
	if got := s.Quotas[0].Values["consumer_byte_rate"]; got != 10<<20 {
		t.Errorf("10MiB = %v", got)
	}
	if !s.Manages.ACLs || !s.Manages.Quotas || !s.Manages.Topics {
		t.Errorf("manages: %+v", s.Manages)
	}
}

func TestParseRefuses(t *testing.T) {
	for name, in := range map[string]string{
		"unknown key":      "cluster: x\ntopix: {}",
		"bad topic":        "topics: {'a b': {partitions: 1}}",
		"principal type":   "acls: [{principal: billing, resource: topic, name: x, operation: read}]",
		"no operation":     "acls: [{principal: User:a, resource: topic, name: x}]",
		"bad operation":    "acls: [{principal: User:a, resource: topic, name: x, operation: fly}]",
		"bad resource":     "acls: [{principal: User:a, resource: table, name: x, operation: read}]",
		"quota entity":     "quotas: [{values: {producer_byte_rate: '1'}}]",
		"quota key":        "quotas: [{user: a, values: {speed: '1'}}]",
		"quota size":       "quotas: [{user: a, values: {request_percentage: 1MiB}}]",
		"quota twice":      "quotas: [{user: a, values: {}}, {user: a, values: {}}]",
		"policy syntax":    "policies: [{name: p, rule: 'topic.partitions >='}]",
		"policy not bool":  "policies: [{name: p, rule: '1 + 1'}]",
		"policy unnamed":   "policies: [{rule: 'true'}]",
		"negative":         "topics: {a: {partitions: -1}}",
		"ip with a user":   "quotas: [{user: a, ip: 10.0.0.1, values: {}}]",
		"cluster acl name": "acls: [{principal: User:a, resource: topic, operation: read}]",
	} {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("%s: accepted %q", name, in)
		}
	}
	if s, err := Parse(nil); err != nil || s.Manages.ACLs || len(s.Topics) != 0 {
		t.Errorf("an empty file declares nothing: %+v, %v", s, err)
	}
}

func TestPlanApplyConverges(t *testing.T) {
	_, cl := kafkatest.Fake(t)
	adm := kadm.NewClient(cl)
	c := ctx(t)

	// The cluster starts with orders too small and a config the file
	// doesn't declare, an ACL the file doesn't declare, and a stray topic.
	if _, err := adm.CreateTopics(c, 3, 1, map[string]*string{"segment.ms": ptr("3600000")}, "orders", "stray"); err != nil {
		t.Fatal(err)
	}
	stale := acls.ACL{Principal: "User:old", Host: "*", Resource: "topic", Name: "orders", Pattern: "literal", Operation: "write", Permission: "allow"}
	if err := acls.Create(c, cl, []acls.ACL{stale}); err != nil {
		t.Fatal(err)
	}

	s, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	live, err := Read(c, cl, s)
	if err != nil {
		t.Fatal(err)
	}
	p := Diff(s, live)
	got := map[Kind]int{}
	for _, o := range p.Ops {
		got[o.Kind]++
	}
	want := map[Kind]int{CreateTopic: 1, AddPartitions: 1, SetConfig: 2, UnsetConfig: 1, CreateACL: 3, SetQuota: 2, DeleteACL: 1}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s: %d ops, want %d\n%s", k, got[k], n, dump(p))
		}
	}
	if p.Unmanaged.Topics != 1 {
		t.Errorf("stray isn't pruned without prune.topics: unmanaged %+v", p.Unmanaged)
	}
	for i := 1; i < len(p.Ops); i++ {
		if order[p.Ops[i].Kind] < order[p.Ops[i-1].Kind] {
			t.Fatalf("ops out of order:\n%s", dump(p))
		}
	}
	if p.Blocked() != "" {
		t.Fatalf("blocked: %s", p.Blocked())
	}

	Check(c, cl, p)
	if why := p.Blocked(); why != "" {
		t.Fatalf("the controller refused a valid plan: %s", why)
	}
	res, err := Apply(c, cl, p)
	if err != nil {
		t.Fatalf("apply: %v\n%+v", err, res)
	}
	for _, r := range res {
		if !r.Done {
			t.Errorf("not done: %+v", r)
		}
	}

	// A second plan has nothing left to do.
	live, err = Read(c, cl, s)
	if err != nil {
		t.Fatal(err)
	}
	if again := Diff(s, live); len(again.Ops) != 0 {
		t.Fatalf("not converged:\n%s", dump(again))
	}
	qs, err := quotas.List(c, adm)
	if err != nil || len(qs) != 1 || qs[0].Values["request_percentage"] != 50 {
		t.Fatalf("quotas %+v, %v", qs, err)
	}
}

func TestPlanIDIsStable(t *testing.T) {
	s, _ := Parse([]byte(doc))
	live := Live{Topics: map[string]LiveTopic{}}
	a, b := Diff(s, live), Diff(s, live)
	if a.ID != b.ID || a.ID == "" {
		t.Fatalf("same inputs, different IDs: %s %s", a.ID, b.ID)
	}
	live.Topics["payments"] = LiveTopic{Partitions: 3, Replication: 1}
	if c := Diff(s, live); c.ID == a.ID {
		t.Fatal("different operations, same ID")
	}
}

func TestPolicies(t *testing.T) {
	s, err := Parse([]byte(`
topics:
  small: {partitions: 1}
  big: {partitions: 12, configs: {retention.ms: 30d}}
policies:
  - {name: partitions, rule: topic.partitions >= 3}
  - {name: retention, rule: 'int(topic.configs[?"retention.ms"].orValue("0")) <= 604800000', message: keep a week at most}
`))
	if err != nil {
		t.Fatal(err)
	}
	p := Diff(s, Live{Topics: map[string]LiveTopic{}})
	if len(p.Violations) != 2 {
		t.Fatalf("violations: %+v", p.Violations)
	}
	if v := p.Violations[1]; v.Topic != "big" || v.Message != "keep a week at most" {
		t.Errorf("retention violation: %+v", v)
	}
	if !strings.Contains(p.Blocked(), "policy partitions refuses topic small") {
		t.Errorf("blocked: %q", p.Blocked())
	}
	if _, err := Apply(context.Background(), nil, p); err == nil {
		t.Fatal("applied a plan that breaks a policy")
	}
}

func TestPruneTopicsIsDestructive(t *testing.T) {
	s, _ := Parse([]byte("topics: {keep: {partitions: 1}}\nprune: {topics: true}"))
	p := Diff(s, Live{Topics: map[string]LiveTopic{
		"keep": {Partitions: 1, Replication: 1}, "gone": {Partitions: 4}, "__consumer_offsets": {Partitions: 50, Internal: true},
	}})
	if len(p.Ops) != 1 || p.Ops[0].Kind != DeleteTopic || p.Ops[0].Target != "gone" || p.Ops[0].Risk != Destructive {
		t.Fatalf("ops: %s", dump(p))
	}
}

func TestWarnings(t *testing.T) {
	s, _ := Parse([]byte("topics: {t: {partitions: 2, replication: 3}}"))
	p := Diff(s, Live{Topics: map[string]LiveTopic{"t": {Partitions: 4, Replication: 1}}})
	if len(p.Ops) != 0 || len(p.Warnings) != 2 {
		t.Fatalf("ops %s warnings %q", dump(p), p.Warnings)
	}
}

func TestRetentionDecreaseIsCaution(t *testing.T) {
	if !shrinks("retention.ms", "604800000", "86400000") || !shrinks("retention.bytes", "-1", "1000") {
		t.Error("smaller retention not flagged")
	}
	if shrinks("retention.ms", "1", "2") || shrinks("segment.ms", "2", "1") {
		t.Error("growth or non-retention flagged")
	}
}

func TestRate(t *testing.T) {
	for v, want := range map[float64]string{10 << 20: "10 MiB/s", 512 << 10: "512 KiB/s", 1000: "1000 B/s"} {
		if got := rate("producer_byte_rate", v); got != want {
			t.Errorf("rate(%v) = %q, want %q", v, got, want)
		}
	}
	if got := rate("request_percentage", 12.5); got != "12.5" {
		t.Errorf("percentage: %q", got)
	}
}

func dump(p *Plan) string {
	var b strings.Builder
	for _, o := range p.Ops {
		b.WriteString(string(o.Kind) + " " + o.Target + " " + o.Key + " " + o.From + "→" + o.To + "\n")
	}
	return b.String()
}

func ptr(s string) *string { return &s }
