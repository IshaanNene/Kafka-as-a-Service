// Package probe checks that a cluster is reachable, one layer at a time, so a
// failure says which layer broke: name resolution, TCP, the Kafka protocol,
// or cluster metadata.
//
// It is what kaas runs before opening a session, and what its connection
// errors are built from.
package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// Stage is one layer of the check.
type Stage int

const (
	DNS Stage = iota
	TCP
	API
	Metadata
)

// Stages lists every stage in the order they run.
var Stages = []Stage{DNS, TCP, API, Metadata}

func (s Stage) String() string {
	return [...]string{"dns", "tcp", "api", "metadata"}[s]
}

// Step is the outcome of one stage against one seed. Metadata steps are
// cluster-wide and carry the seed that answered.
type Step struct {
	Seed   string
	Stage  Stage
	Err    error
	Detail string        // what was learned: addresses, version, cluster shape
	Took   time.Duration // wall time of this stage
	At     time.Duration // since the probe started
}

// OK reports whether the step passed.
func (s Step) OK() bool { return s.Err == nil }

// Result is the whole probe.
type Result struct {
	Seeds   []string
	Steps   []Step
	Took    time.Duration
	Cluster string // cluster ID, when metadata answered
	Brokers int
	Topics  int
	Version string // best guess, e.g. "v4.0"
}

// OK reports whether metadata came back, i.e. the cluster is usable.
func (r Result) OK() bool {
	for _, s := range r.Steps {
		if s.Stage == Metadata && s.OK() {
			return true
		}
	}
	return false
}

// Err is the first failure that explains why the probe did not succeed: the
// deepest stage any seed reached is where it broke.
func (r Result) Err() error {
	if r.OK() {
		return nil
	}
	var worst *Step
	for i := range r.Steps {
		s := &r.Steps[i]
		if s.Err != nil && (worst == nil || s.Stage > worst.Stage) {
			worst = s
		}
	}
	if worst == nil {
		return errors.New("probe did not run")
	}
	return fmt.Errorf("%s %s: %w", worst.Stage, worst.Seed, worst.Err)
}

// Seed returns the steps for one seed, by stage.
func (r Result) Seed(seed string) map[Stage]Step {
	m := map[Stage]Step{}
	for _, s := range r.Steps {
		if s.Seed == seed {
			m[s.Stage] = s
		}
	}
	return m
}

// Options tunes a probe.
type Options struct {
	Timeout time.Duration // per stage; default 3s
	// Report, if set, is called as each step finishes, from the probing
	// goroutine; renderers use it to draw progress live.
	Report func(Step)
	// Client options for the Kafka stages (TLS, SASL later).
	Client []kgo.Opt
}

// DefaultPort is used for seeds without one.
const DefaultPort = "9092"

// Run probes seeds. It never returns an error itself: every failure is a step
// in the result, so callers can render exactly what happened.
func Run(ctx context.Context, seeds []string, o Options) Result {
	if o.Timeout == 0 {
		o.Timeout = 3 * time.Second
	}
	start := time.Now()
	r := Result{Seeds: seeds}
	add := func(s Step) {
		s.At = time.Since(start)
		r.Steps = append(r.Steps, s)
		if o.Report != nil {
			o.Report(s)
		}
	}

	// DNS and TCP per seed; only seeds that accept a connection go on to
	// speak Kafka.
	var live, liveSeeds []string
	for _, seed := range seeds {
		host, port, err := net.SplitHostPort(seed)
		if err != nil {
			host, port = seed, DefaultPort
		}
		t := time.Now()
		sctx, cancel := context.WithTimeout(ctx, o.Timeout)
		addrs, err := net.DefaultResolver.LookupHost(sctx, host)
		cancel()
		detail := ""
		if err == nil {
			detail = host + " → " + strings.Join(first(addrs, 3), ", ")
		}
		add(Step{Seed: seed, Stage: DNS, Err: err, Detail: detail, Took: time.Since(t)})
		if err != nil {
			continue
		}
		t = time.Now()
		sctx, cancel = context.WithTimeout(ctx, o.Timeout)
		var d net.Dialer
		conn, err := d.DialContext(sctx, "tcp", net.JoinHostPort(host, port))
		cancel()
		detail = ""
		if err == nil {
			detail = "connected " + conn.RemoteAddr().String()
			_ = conn.Close()
		}
		add(Step{Seed: seed, Stage: TCP, Err: err, Detail: detail, Took: time.Since(t)})
		if err == nil {
			live = append(live, net.JoinHostPort(host, port))
			liveSeeds = append(liveSeeds, seed)
		}
	}
	if len(live) == 0 {
		r.Took = time.Since(start)
		return r
	}

	opts := append([]kgo.Opt{kgo.SeedBrokers(live...), kgo.RequestRetries(0)}, o.Client...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		add(Step{Seed: liveSeeds[0], Stage: API, Err: err})
		r.Took = time.Since(start)
		return r
	}
	defer cl.Close()

	// ApiVersions against each live seed proves it speaks Kafka and dates it.
	var answered []string
	for i, b := range cl.SeedBrokers() {
		if i >= len(live) {
			break
		}
		seed := liveSeeds[i]
		t := time.Now()
		sctx, cancel := context.WithTimeout(ctx, o.Timeout)
		req := kmsg.NewPtrApiVersionsRequest()
		// v3+ brokers reject an empty client name (KIP-511).
		req.ClientSoftwareName, req.ClientSoftwareVersion = "kaas-probe", "1.0"
		resp, err := b.Request(sctx, req)
		cancel()
		s := Step{Seed: seed, Stage: API, Err: err, Took: time.Since(t)}
		if err == nil {
			av := resp.(*kmsg.ApiVersionsResponse)
			if err = kerr.ErrorForCode(av.ErrorCode); err == nil {
				v := kversion.FromApiVersionsResponse(av).VersionGuess()
				if !strings.HasPrefix(v, "v") {
					// Kafka-compatible brokers (Redpanda, kfake) don't match an
					// Apache release; franz-go then says "between v1.0 and …".
					v = "compatible"
				}
				if r.Version == "" {
					r.Version = v
				}
				s.Detail = fmt.Sprintf("kafka %s · %d APIs", v, len(av.ApiKeys))
				answered = append(answered, seed)
			}
			s.Err = err
		}
		add(s)
	}
	if len(answered) == 0 {
		r.Took = time.Since(start)
		return r
	}

	t := time.Now()
	sctx, cancel := context.WithTimeout(ctx, o.Timeout)
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(sctx, cl)
	cancel()
	s := Step{Seed: answered[0], Stage: Metadata, Err: err, Took: time.Since(t)}
	if err == nil {
		if resp.ClusterID != nil {
			r.Cluster = *resp.ClusterID
		}
		r.Brokers, r.Topics = len(resp.Brokers), len(resp.Topics)
		s.Detail = fmt.Sprintf("%d brokers · %d topics · controller %d", r.Brokers, r.Topics, resp.ControllerID)
	}
	add(s)
	r.Took = time.Since(start)
	return r
}

func first(s []string, n int) []string {
	if len(s) > n {
		return append(s[:n:n], fmt.Sprintf("+%d", len(s)-n))
	}
	return s
}
