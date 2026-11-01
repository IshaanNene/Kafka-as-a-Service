package pipe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Plugins are executables named kaas-source-<scheme> and
// kaas-sink-<scheme>: a spec whose scheme isn't built in runs the one
// that matches, with the spec as its only argument. They speak JSON
// lines on stdin and stdout, so a plugin is a few lines in any language.
//
// A source plugin writes a record a line, in the shape kaas consume -o
// json writes, with an "id" if it wants to know when the record is
// written: kaas then writes {"ack": id} to its stdin. It ends by
// exiting; {"error": "…"} on a line of its own fails the pipe.
//
// A sink plugin reads a record a line, each with a "seq" that counts up
// from 1, and writes {"ack": seq} once everything up to seq is durable,
// or {"error": "…", "seq": seq} to fail the write. kaas waits for the
// ack of a batch's last record before acknowledging the batch to its
// source.
const (
	SourcePrefix = "kaas-source-"
	SinkPrefix   = "kaas-sink-"
)

// Plugin is an executable found on the plugin path.
type Plugin struct {
	Scheme string
	Role   string // source or sink
	Path   string
}

// pluginDirs are where plugins are looked for, before PATH: the Env's
// directories, $KAAS_PLUGIN_PATH, then ~/.config/kaas/plugins.
func pluginDirs(env Env) []string {
	dirs := append([]string(nil), env.PluginPath...)
	if p := os.Getenv("KAAS_PLUGIN_PATH"); p != "" {
		dirs = append(dirs, filepath.SplitList(p)...)
	}
	if cfg, err := os.UserConfigDir(); err == nil {
		dirs = append(dirs, filepath.Join(cfg, "kaas", "plugins"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".config", "kaas", "plugins"))
	}
	return dirs
}

// findPlugin is the path of the executable named name.
func findPlugin(name string, env Env) (string, error) {
	for _, d := range pluginDirs(env) {
		// Absolute, or exec would look a bare "kaas-sink-x" up on PATH.
		p, err := filepath.Abs(filepath.Join(d, name))
		if err != nil {
			continue
		}
		if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Mode()&0o111 != 0 {
			return p, nil
		}
	}
	return exec.LookPath(name)
}

// Discover is every plugin on the plugin path and PATH, by scheme; one
// earlier on the path hides a later one of the same name.
func Discover(env Env) []Plugin {
	dirs := pluginDirs(env)
	dirs = append(dirs, filepath.SplitList(os.Getenv("PATH"))...)
	seen := map[string]bool{}
	var out []Plugin
	for _, d := range dirs {
		ents, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range ents {
			name := e.Name()
			role, scheme := "", ""
			switch {
			case strings.HasPrefix(name, SourcePrefix):
				role, scheme = "source", strings.TrimPrefix(name, SourcePrefix)
			case strings.HasPrefix(name, SinkPrefix):
				role, scheme = "sink", strings.TrimPrefix(name, SinkPrefix)
			default:
				continue
			}
			info, err := e.Info()
			if scheme == "" || seen[name] || err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
				continue
			}
			seen[name] = true
			path, err := filepath.Abs(filepath.Join(d, name))
			if err != nil {
				path = filepath.Join(d, name)
			}
			out = append(out, Plugin{Scheme: scheme, Role: role, Path: path})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scheme != out[j].Scheme {
			return out[i].Scheme < out[j].Scheme
		}
		return out[i].Role > out[j].Role
	})
	return out
}

// proc is a running plugin: its stdin, its stdout a line at a time, and
// the tail of its stderr for errors.
type proc struct {
	name  string
	cmd   *exec.Cmd
	in    io.WriteCloser
	out   *bufio.Scanner
	tail  *tailWriter
	waitC chan error
}

func startPlugin(prefix, spec, scheme string, env Env) (*proc, error) {
	name := prefix + scheme
	path, err := findPlugin(name, env)
	if err != nil {
		return nil, fmt.Errorf("no %q scheme: built in are %s, and no %s plugin is on KAAS_PLUGIN_PATH, ~/.config/kaas/plugins or PATH", scheme, builtinNames(), name)
	}
	cmd := exec.Command(path, spec)
	cmd.Env = append(os.Environ(), "KAAS_SPEC="+spec, "KAAS_BROKERS="+strings.Join(env.Brokers, ","))
	p := &proc{name: name, cmd: cmd, tail: &tailWriter{w: env.Stderr}, waitC: make(chan error, 1)}
	cmd.Stderr = p.tail
	if p.in, err = cmd.StdinPipe(); err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	p.out = bufio.NewScanner(stdout)
	p.out.Buffer(make([]byte, 64<<10), MaxLine)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting %s: %w", name, err)
	}
	return p, nil
}

func builtinNames() string {
	var names []string
	for _, k := range Kinds() {
		names = append(names, k.Scheme)
	}
	return strings.Join(names, ", ")
}

// failed is err with what the plugin last said on stderr.
func (p *proc) failed(err error) error {
	if t := p.tail.String(); t != "" {
		return fmt.Errorf("%s: %w: %s", p.name, err, t)
	}
	return fmt.Errorf("%s: %w", p.name, err)
}

// stop closes the plugin's stdin and waits for it to exit, killing it
// after a grace period.
func (p *proc) stop() error {
	_ = p.in.Close()
	go func() { p.waitC <- p.cmd.Wait() }()
	select {
	case err := <-p.waitC:
		if err != nil {
			return p.failed(err)
		}
		return nil
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		return fmt.Errorf("%s didn't exit within 5s of its input closing, so was killed: %w", p.name, <-p.waitC)
	}
}

// tailWriter keeps the last line or so a plugin wrote to stderr, and
// passes all of it on.
type tailWriter struct {
	mu  sync.Mutex
	w   io.Writer
	buf []byte
}

func (t *tailWriter) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.w != nil {
		_, _ = t.w.Write(b) // passing it on is best effort
	}
	t.buf = append(t.buf, b...)
	if len(t.buf) > 2048 {
		t.buf = t.buf[len(t.buf)-2048:]
	}
	return len(b), nil
}

func (t *tailWriter) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := strings.TrimSpace(string(t.buf))
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// pluginLine is a line a plugin writes: a record, an ack, or an error.
type pluginLine struct {
	RecordJSON
	ID    json.RawMessage `json:"id,omitempty"`
	Ack   *int64          `json:"ack,omitempty"`
	Seq   int64           `json:"seq,omitempty"`
	Error string          `json:"error,omitempty"`
}

// pluginSource reads records from a source plugin as they come, in
// batches of up to batch, waiting no more than linger for one to fill.
type pluginSource struct {
	p      *proc
	lines  chan pluginLine
	errC   chan error
	mu     sync.Mutex // one ack write at a time
	batch  int
	linger time.Duration
}

func openPluginSource(spec, scheme string, env Env) (Source, error) {
	p, err := startPlugin(SourcePrefix, spec, scheme, env)
	if err != nil {
		return nil, err
	}
	s := &pluginSource{p: p, lines: make(chan pluginLine, 1024), errC: make(chan error, 1), batch: 500, linger: 50 * time.Millisecond}
	go s.read()
	return s, nil
}

func (s *pluginSource) read() {
	defer close(s.lines)
	n := 0
	for s.p.out.Scan() {
		n++
		raw := bytes.TrimSpace(s.p.out.Bytes())
		if len(raw) == 0 {
			continue
		}
		var l pluginLine
		if err := json.Unmarshal(raw, &l); err != nil {
			s.errC <- s.p.failed(fmt.Errorf("line %d isn't a JSON record: %w", n, err))
			return
		}
		s.lines <- l
	}
	if err := s.p.out.Err(); err != nil {
		s.errC <- s.p.failed(err)
	}
}

func (s *pluginSource) Next(ctx context.Context) (Batch, error) {
	var b Batch
	var ids []json.RawMessage
	var timer <-chan time.Time
	for len(b.Records) < s.batch {
		select {
		case <-ctx.Done():
			return Batch{}, ctx.Err()
		case err := <-s.errC:
			return Batch{}, err
		case <-timer:
			return s.ack(b, ids), nil
		case l, ok := <-s.lines:
			if !ok {
				select {
				case err := <-s.errC:
					return Batch{}, err
				default:
				}
				if len(b.Records) > 0 {
					return s.ack(b, ids), nil
				}
				return Batch{}, io.EOF
			}
			if l.Error != "" {
				return Batch{}, s.p.failed(errors.New(l.Error))
			}
			r, err := FromJSON(l.RecordJSON)
			if err != nil {
				return Batch{}, s.p.failed(err)
			}
			b.Records = append(b.Records, r)
			if len(l.ID) > 0 {
				ids = append(ids, l.ID)
			}
			if timer == nil {
				timer = time.After(s.linger)
			}
		}
	}
	return s.ack(b, ids), nil
}

// ack has b, once written, tell the plugin each of ids.
func (s *pluginSource) ack(b Batch, ids []json.RawMessage) Batch {
	if len(ids) == 0 {
		return b
	}
	b.Ack = func(context.Context) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		var buf bytes.Buffer
		for _, id := range ids {
			buf.WriteString(`{"ack":`)
			buf.Write(id)
			buf.WriteString("}\n")
		}
		// A plugin that has exited, or closed its stdin, isn't listening
		// for acks: that's its choice, not a failure.
		if _, err := s.p.in.Write(buf.Bytes()); err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
			return s.p.failed(err)
		}
		return nil
	}
	return b
}

func (s *pluginSource) Close() error {
	go func() {
		for range s.lines { // keep its stdout moving so it can exit
		}
	}()
	err := s.p.stop()
	if s.p.cmd.ProcessState != nil && !s.p.cmd.ProcessState.Success() {
		return err
	}
	return nil
}

// pluginSink writes records to a sink plugin and waits for its acks.
type pluginSink struct {
	p    *proc
	seq  int64
	acks chan pluginLine
	dead chan struct{}
	err  error
}

func openPluginSink(spec, scheme string, env Env) (Sink, error) {
	p, err := startPlugin(SinkPrefix, spec, scheme, env)
	if err != nil {
		return nil, err
	}
	s := &pluginSink{p: p, acks: make(chan pluginLine, 64), dead: make(chan struct{})}
	go s.read()
	return s, nil
}

func (s *pluginSink) read() {
	defer close(s.dead)
	for s.p.out.Scan() {
		raw := bytes.TrimSpace(s.p.out.Bytes())
		if len(raw) == 0 {
			continue
		}
		var l pluginLine
		if err := json.Unmarshal(raw, &l); err != nil || (l.Ack == nil && l.Error == "") {
			s.err = s.p.failed(fmt.Errorf("wrote %q; want {\"ack\": seq} or {\"error\": …}", kitClip(string(raw), 80)))
			return
		}
		s.acks <- l
	}
	s.err = s.p.failed(errors.New("exited"))
	if err := s.p.out.Err(); err != nil {
		s.err = s.p.failed(err)
	}
}

func (s *pluginSink) Write(ctx context.Context, rs []Record) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rs {
		s.seq++
		if err := enc.Encode(pluginLine{RecordJSON: ToJSON(r), Seq: s.seq}); err != nil {
			return err
		}
	}
	if _, err := s.p.in.Write(buf.Bytes()); err != nil {
		select {
		case <-s.dead:
			return s.err
		default:
		}
		return s.p.failed(err)
	}
	want := s.seq
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.dead:
			return s.err
		case l := <-s.acks:
			if l.Error != "" {
				return s.p.failed(fmt.Errorf("record %d: %s", l.Seq, l.Error))
			}
			if *l.Ack >= want {
				return nil
			}
		}
	}
}

func (s *pluginSink) Close() error { return s.p.stop() }

func kitClip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
