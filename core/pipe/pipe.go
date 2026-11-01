// Package pipe moves records from sources to sinks: Kafka topics, files,
// stdin and stdout, HTTP, a data generator, and plugins, which are any
// executable that speaks JSON lines. A pipe reads a batch from each
// source, keeps the records its filter passes, writes them to every sink,
// and only then acknowledges the batch to its source, so a source that
// checkpoints (a Kafka consumer group, an HTTP request waiting for its
// answer, a plugin) redelivers what a failed or stopped pipe didn't
// write: delivery is at least once.
package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header is one record header.
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Record is what flows through a pipe. Topic, Partition and Offset say
// where it came from, when its source knows.
type Record struct {
	Key       []byte // nil for no key
	Value     []byte // nil for a tombstone
	Headers   []Header
	Time      time.Time
	Topic     string
	Partition int32
	Offset    int64
}

// Batch is records read together, and how to tell their source they are
// written. Ack is nil for a source that doesn't checkpoint.
type Batch struct {
	Records []Record
	Ack     func(context.Context) error
}

// Source reads batches. Next returns io.EOF when there is nothing more to
// read, and blocks until there is a batch or ctx ends.
type Source interface {
	Next(ctx context.Context) (Batch, error)
	Close() error
}

// Sink writes records. A Write that returns nil has them durably, as far
// as the sink can tell: the records are acknowledged to their source.
type Sink interface {
	Write(ctx context.Context, rs []Record) error
	Close() error
}

// Env is what endpoints open with: the cluster for kafka: endpoints, the
// process's stdin and stdout, and where plugins live.
type Env struct {
	Brokers    []string
	Stdin      io.Reader
	Stdout     io.Writer
	PluginPath []string  // directories searched before PATH
	KafkaOpts  []kgo.Opt // added to every kafka: endpoint's client
	Stderr     io.Writer // where plugins' stderr goes; nil keeps only its tail
}

// Endpoint is an opened source or sink and the spec it came from.
type Endpoint struct {
	Spec   string
	Scheme string
	Source Source
	Sink   Sink
}

// Kind is what an endpoint scheme can be.
type Kind struct {
	Scheme  string
	Summary string
	Example string
	Source  func(u *url.URL, env Env) (Source, error)
	Sink    func(u *url.URL, env Env) (Sink, error)
}

var (
	kindsMu sync.RWMutex
	kinds   = map[string]Kind{}
)

// Register adds an endpoint kind; built-ins register themselves, and a Go
// program embedding kaas can add its own.
func Register(k Kind) {
	kindsMu.Lock()
	defer kindsMu.Unlock()
	kinds[k.Scheme] = k
}

// Kinds is every registered kind, by scheme.
func Kinds() []Kind {
	kindsMu.RLock()
	defer kindsMu.RUnlock()
	out := make([]Kind, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Scheme < out[j].Scheme })
	return out
}

// parse reads an endpoint spec: a URL, or stdin, stdout and - for the
// process's own streams.
func parse(spec string) (*url.URL, error) {
	base, q, _ := strings.Cut(spec, "?")
	switch base {
	case "-", "stdin", "stdout":
		return &url.URL{Scheme: "std", Opaque: base, RawQuery: q}, nil
	}
	u, err := url.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("%q isn't an endpoint: %w", spec, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("%q has no scheme: want kafka:topic, file:path, http://…, gen:orders or a plugin's scheme", spec)
	}
	return u, nil
}

// OpenSource opens spec as a source: a built-in scheme, or a plugin
// named kaas-source-<scheme>.
func OpenSource(spec string, env Env) (Endpoint, error) {
	u, err := parse(spec)
	if err != nil {
		return Endpoint{}, err
	}
	kindsMu.RLock()
	k, ok := kinds[u.Scheme]
	kindsMu.RUnlock()
	var s Source
	switch {
	case ok && k.Source != nil:
		s, err = k.Source(u, env)
	case ok:
		return Endpoint{}, fmt.Errorf("%s: can only be written to", u.Scheme)
	default:
		s, err = openPluginSource(spec, u.Scheme, env)
	}
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{Spec: Redact(spec), Scheme: u.Scheme, Source: s}, nil
}

// OpenSink opens spec as a sink: a built-in scheme, or a plugin named
// kaas-sink-<scheme>.
func OpenSink(spec string, env Env) (Endpoint, error) {
	u, err := parse(spec)
	if err != nil {
		return Endpoint{}, err
	}
	kindsMu.RLock()
	k, ok := kinds[u.Scheme]
	kindsMu.RUnlock()
	var s Sink
	switch {
	case ok && k.Sink != nil:
		s, err = k.Sink(u, env)
	case ok:
		return Endpoint{}, fmt.Errorf("%s: can only be read from", u.Scheme)
	default:
		s, err = openPluginSink(spec, u.Scheme, env)
	}
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{Spec: Redact(spec), Scheme: u.Scheme, Sink: s}, nil
}

// secretParams are query parameters whose values Redact hides.
var secretParams = []string{"header", "token", "password", "secret", "apikey", "api_key", "sasl_password"}

// Redact is spec fit to show and log: a URL's password and the values
// of parameters that carry credentials are replaced with ***.
func Redact(spec string) string {
	u, err := url.Parse(spec)
	if err != nil || u.Scheme == "" {
		return spec
	}
	changed := false
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), "***")
		changed = true
	}
	q := u.Query()
	for k := range q {
		for _, s := range secretParams {
			if strings.EqualFold(k, s) {
				q[k] = []string{"***"}
				changed = true
			}
		}
	}
	if !changed {
		return spec
	}
	u.RawQuery = q.Encode()
	return strings.ReplaceAll(u.String(), "%2A%2A%2A", "***")
}

// Options shapes a run.
type Options struct {
	Where *Filter // nil keeps every record
	Max   int64   // stop after writing this many; 0 for no limit
}

// SourceStats and SinkStats count one endpoint's records.
type SourceStats struct {
	Spec     string
	Read     int64
	Batches  int64
	Done     bool // read to its end
	Err      string
	LastRead time.Time
}

type SinkStats struct {
	Spec    string
	Written int64
	Bytes   int64
	Writes  int64
	Err     string
}

// Stats is a run so far.
type Stats struct {
	Started  time.Time
	Read     int64 // from every source
	Filtered int64 // dropped by the filter
	Rejected int64 // the filter couldn't evaluate
	Written  int64 // to every sink (a record written to two sinks counts once)
	Acked    int64
	Sources  []SourceStats
	Sinks    []SinkStats
	InFlight int // records read but not yet written
	Done     bool
	Err      string
}

// Pipe is a run of sources into sinks.
type Pipe struct {
	srcs  []Endpoint
	sinks []Endpoint
	o     Options

	mu    sync.Mutex
	st    Stats
	sinkM []sync.Mutex // one write at a time per sink

	written atomic.Int64
}

// New is a pipe from srcs to sinks; Run starts it.
func New(srcs, sinks []Endpoint, o Options) *Pipe {
	p := &Pipe{srcs: srcs, sinks: sinks, o: o, sinkM: make([]sync.Mutex, len(sinks))}
	for _, s := range srcs {
		p.st.Sources = append(p.st.Sources, SourceStats{Spec: s.Spec})
	}
	for _, s := range sinks {
		p.st.Sinks = append(p.st.Sinks, SinkStats{Spec: s.Spec})
	}
	return p
}

// Stats is a copy of the run's counts.
func (p *Pipe) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.st
	st.Sources = append([]SourceStats(nil), p.st.Sources...)
	st.Sinks = append([]SinkStats(nil), p.st.Sinks...)
	return st
}

// errMax ends a run that wrote Options.Max records.
var errMax = errors.New("wrote the most asked for")

// Run reads every source until each ends, ctx ends, Max records are
// written, or something fails; it closes every endpoint before returning.
// A source that ends with an error ends the run.
func (p *Pipe) Run(ctx context.Context) error {
	p.mu.Lock()
	p.st.Started = time.Now()
	p.mu.Unlock()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for i := range p.srcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.drain(ctx, i); err != nil {
				cancel(err)
			}
		}()
	}
	wg.Wait()
	var errs []error
	for _, e := range p.srcs {
		if err := e.Source.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s: %w", e.Spec, err))
		}
	}
	for _, e := range p.sinks {
		if err := e.Sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s: %w", e.Spec, err))
		}
	}
	err := context.Cause(ctx)
	switch {
	case errors.Is(err, errMax),
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && !errors.Is(err, errSource):
		err = nil
	}
	err = errors.Join(append([]error{err}, errs...)...)
	p.mu.Lock()
	p.st.Done = true
	if err != nil {
		p.st.Err = err.Error()
	}
	p.mu.Unlock()
	return err
}

// errSource marks an error that ended a source, as opposed to the run
// being stopped.
var errSource = errors.New("source failed")

// drain moves source i's batches to the sinks until it ends.
func (p *Pipe) drain(ctx context.Context, i int) error {
	src := p.srcs[i]
	for {
		b, err := src.Source.Next(ctx)
		switch {
		case errors.Is(err, io.EOF):
			p.update(func(st *Stats) { st.Sources[i].Done = true })
			return nil
		case ctx.Err() != nil:
			return nil
		case err != nil:
			p.update(func(st *Stats) { st.Sources[i].Err = err.Error() })
			return fmt.Errorf("%w: %s: %w", errSource, src.Spec, err)
		}
		keep := b.Records
		var filtered, rejected int64
		if p.o.Where != nil {
			keep = keep[:0:0]
			for _, r := range b.Records {
				ok, err := p.o.Where.Match(r)
				switch {
				case err != nil:
					rejected++
				case ok:
					keep = append(keep, r)
				default:
					filtered++
				}
			}
		}
		if p.o.Max > 0 {
			left := p.o.Max - p.written.Load()
			if left <= 0 {
				return errMax
			}
			keep = keep[:min(int64(len(keep)), left)]
		}
		p.update(func(st *Stats) {
			s := &st.Sources[i]
			s.Read += int64(len(b.Records))
			s.Batches++
			s.LastRead = time.Now()
			st.Read += int64(len(b.Records))
			st.Filtered += filtered
			st.Rejected += rejected
			st.InFlight += len(keep)
		})
		if len(keep) > 0 {
			if err := p.write(ctx, keep); err != nil {
				p.update(func(st *Stats) { st.InFlight -= len(keep) })
				return fmt.Errorf("%w: %w", errSource, err)
			}
		}
		if b.Ack != nil {
			if err := b.Ack(ctx); err != nil && ctx.Err() == nil {
				return fmt.Errorf("%w: acknowledging to %s: %w", errSource, src.Spec, err)
			}
		}
		n := p.written.Add(int64(len(keep)))
		p.update(func(st *Stats) {
			st.InFlight -= len(keep)
			st.Written += int64(len(keep))
			st.Acked += int64(len(b.Records))
		})
		if p.o.Max > 0 && n >= p.o.Max {
			return errMax
		}
	}
}

// write gives rs to every sink, each in turn.
func (p *Pipe) write(ctx context.Context, rs []Record) error {
	var bytes int64
	for _, r := range rs {
		bytes += int64(len(r.Key) + len(r.Value))
	}
	for j, s := range p.sinks {
		p.sinkM[j].Lock()
		err := s.Sink.Write(ctx, rs)
		p.sinkM[j].Unlock()
		p.update(func(st *Stats) {
			k := &st.Sinks[j]
			if err != nil {
				k.Err = err.Error()
				return
			}
			k.Written += int64(len(rs))
			k.Bytes += bytes
			k.Writes++
		})
		if err != nil {
			return fmt.Errorf("writing to %s: %w", s.Spec, err)
		}
	}
	return nil
}

func (p *Pipe) update(f func(*Stats)) {
	p.mu.Lock()
	f(&p.st)
	p.mu.Unlock()
}

// query reads a URL's query parameter, or def.
func query(u *url.URL, key, def string) string {
	if v := u.Query().Get(key); v != "" {
		return v
	}
	return def
}

// pathOf is a URL's path, for file:path and file:///path alike.
func pathOf(u *url.URL) string {
	if u.Opaque != "" {
		return u.Opaque
	}
	return u.Host + u.Path
}
