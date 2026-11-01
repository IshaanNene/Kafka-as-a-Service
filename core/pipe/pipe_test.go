package pipe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memSource is a source of fixed batches that counts acks, like a
// checkpointing source would.
type memSource struct {
	batches [][]Record
	acked   atomic.Int64
	closed  atomic.Bool
	err     error // returned after the batches, instead of io.EOF
}

func (s *memSource) Next(ctx context.Context) (Batch, error) {
	if len(s.batches) == 0 {
		if s.err != nil {
			return Batch{}, s.err
		}
		return Batch{}, io.EOF
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return Batch{Records: b, Ack: func(context.Context) error { s.acked.Add(int64(len(b))); return nil }}, nil
}

func (s *memSource) Close() error { s.closed.Store(true); return nil }

// memSink keeps what it's given; fail makes the n-th write fail.
type memSink struct {
	mu     sync.Mutex
	got    []Record
	writes int
	fail   int
	closed bool
}

func (s *memSink) Write(_ context.Context, rs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.writes == s.fail {
		return errors.New("disk full")
	}
	s.got = append(s.got, rs...)
	return nil
}

func (s *memSink) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }

func (s *memSink) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.got {
		out = append(out, string(r.Value))
	}
	return out
}

func recs(vals ...string) []Record {
	out := make([]Record, len(vals))
	for i, v := range vals {
		out[i] = Record{Key: []byte(fmt.Sprint("k", i)), Value: []byte(v)}
	}
	return out
}

func src(s Source) Endpoint { return Endpoint{Spec: "mem", Source: s} }
func snk(s Sink) Endpoint   { return Endpoint{Spec: "mem", Sink: s} }

func TestFanOutWritesEverySinkThenAcks(t *testing.T) {
	s := &memSource{batches: [][]Record{recs("a", "b"), recs("c")}}
	k1, k2 := &memSink{}, &memSink{}
	p := New([]Endpoint{src(s)}, []Endpoint{snk(k1), snk(k2)}, Options{})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, k := range []*memSink{k1, k2} {
		if got := strings.Join(k.values(), ""); got != "abc" {
			t.Errorf("sink got %q, want abc", got)
		}
		if !k.closed {
			t.Error("sink not closed")
		}
	}
	st := p.Stats()
	if st.Read != 3 || st.Written != 3 || st.Acked != 3 || s.acked.Load() != 3 || !st.Done || st.InFlight != 0 {
		t.Errorf("stats %+v, acked %d", st, s.acked.Load())
	}
	if !st.Sources[0].Done || st.Sinks[1].Writes != 2 || st.Sinks[1].Bytes != 3+6 {
		t.Errorf("endpoint stats %+v %+v", st.Sources, st.Sinks)
	}
	if !s.closed.Load() {
		t.Error("source not closed")
	}
}

func TestFanIn(t *testing.T) {
	a := &memSource{batches: [][]Record{recs("1", "2"), recs("3")}}
	b := &memSource{batches: [][]Record{recs("x"), recs("y", "z")}}
	k := &memSink{}
	if err := New([]Endpoint{src(a), src(b)}, []Endpoint{snk(k)}, Options{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(k.values()); got != 6 {
		t.Errorf("wrote %d, want 6", got)
	}
}

// A sink that fails stops the pipe without acknowledging the batch, so a
// checkpointing source sends it again: at least once, never lost.
func TestFailedWriteIsNotAcked(t *testing.T) {
	s := &memSource{batches: [][]Record{recs("a"), recs("b"), recs("c")}}
	good, bad := &memSink{}, &memSink{fail: 2}
	p := New([]Endpoint{src(s)}, []Endpoint{snk(good), snk(bad)}, Options{})
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err %v, want disk full", err)
	}
	if s.acked.Load() != 1 {
		t.Errorf("acked %d, want only the first batch", s.acked.Load())
	}
	if st := p.Stats(); st.Sinks[1].Err == "" || st.Err == "" || st.Written != 1 {
		t.Errorf("stats %+v", st)
	}
}

func TestSourceErrorEndsRun(t *testing.T) {
	s := &memSource{batches: [][]Record{recs("a")}, err: errors.New("broker gone")}
	p := New([]Endpoint{src(s)}, []Endpoint{snk(&memSink{})}, Options{})
	if err := p.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "broker gone") {
		t.Fatalf("err %v", err)
	}
	if p.Stats().Sources[0].Err == "" {
		t.Error("source error not in stats")
	}
}

func TestMaxStopsCleanly(t *testing.T) {
	s := &memSource{batches: [][]Record{recs("a", "b", "c"), recs("d", "e"), recs("f")}}
	k := &memSink{}
	p := New([]Endpoint{src(s)}, []Endpoint{snk(k)}, Options{Max: 4})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(k.values(), ""); got != "abcd" {
		t.Errorf("wrote %q, want abcd", got)
	}
}

func TestWhere(t *testing.T) {
	f, err := NewFilter(`value.amount > 100 && headers["region"] == "eu"`)
	if err != nil {
		t.Fatal(err)
	}
	r := func(v, region string) Record {
		return Record{Value: []byte(v), Headers: []Header{{Key: "region", Value: region}}}
	}
	s := &memSource{batches: [][]Record{{
		r(`{"amount": 150}`, "eu"),
		r(`{"amount": 50}`, "eu"),
		r(`{"amount": 500}`, "us"),
		r(`{"total": 9}`, "eu"), // no amount: rejected, not a crash
		r(`not json`, "eu"),     // a string has no fields
	}}}
	k := &memSink{}
	p := New([]Endpoint{src(s)}, []Endpoint{snk(k)}, Options{Where: f})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := p.Stats()
	if len(k.values()) != 1 || st.Filtered != 2 || st.Rejected != 2 || st.Acked != 5 {
		t.Errorf("wrote %v, stats %+v", k.values(), st)
	}
}

func TestWhereVars(t *testing.T) {
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	r := Record{Key: []byte("c-1"), Value: []byte(`{"tags":["vip"]}`), Topic: "orders", Partition: 3, Offset: 41, Time: at}
	for expr, want := range map[string]bool{
		`key.startsWith("c-")`:                     true,
		`"vip" in value.tags`:                      true,
		`topic == "orders" && partition == 3`:      true,
		`offset > 40`:                              true,
		`time > timestamp("2026-01-01T00:00:00Z")`: true,
		`raw.contains("vip")`:                      true,
		`size(headers) == 0`:                       true,
		`key == "c-2"`:                             false,
	} {
		f, err := NewFilter(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		if got, err := f.Match(r); err != nil || got != want {
			t.Errorf("%s = %v, %v; want %v", expr, got, err, want)
		}
	}
	for _, bad := range []string{`value.amount >`, `key + 1`, `"text"`, `nosuchvar == 1`} {
		if _, err := NewFilter(bad); err == nil {
			t.Errorf("%s compiled", bad)
		}
	}
}

func TestCancelIsNotAnError(t *testing.T) {
	g, err := OpenSource("gen:orders?rate=50&seed=1", Env{})
	if err != nil {
		t.Fatal(err)
	}
	k := &memSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := New([]Endpoint{g}, []Endpoint{snk(k)}, Options{}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(k.values()); n == 0 || n > 20 {
		t.Errorf("wrote %d at 50/s in 150ms", n)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	in := []Record{
		{Key: []byte("k"), Value: []byte(`{"a":1}`), Headers: []Header{{Key: "h", Value: "v"}}, Time: time.Unix(1700000000, 0).UTC(), Topic: "t", Partition: 2, Offset: 9},
		{Key: nil, Value: nil},                               // a tombstone with no key
		{Key: []byte{0xff, 0x00}, Value: []byte{0x01, 0xfe}}, // binary: base64
		{Value: []byte("line with \"quotes\", commas")},
	}
	for _, format := range []string{FormatJSONL, FormatCSV, FormatText} {
		var buf bytes.Buffer
		e, _ := newEncoder(&buf, format)
		if err := e.write(in); err != nil {
			t.Fatal(err)
		}
		d, _ := newDecoder(&buf, format)
		var out []Record
		for {
			r, err := d.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("%s: %v", format, err)
			}
			out = append(out, r)
		}
		if len(out) != len(in) {
			t.Fatalf("%s: %d records back, want %d", format, len(out), len(in))
		}
		for i := range in {
			if !bytes.Equal(out[i].Value, in[i].Value) {
				t.Errorf("%s %d: value %q, want %q", format, i, out[i].Value, in[i].Value)
			}
			if format == FormatJSONL {
				if !bytes.Equal(out[i].Key, in[i].Key) || (out[i].Key == nil) != (in[i].Key == nil) || (out[i].Value == nil) != (in[i].Value == nil) {
					t.Errorf("jsonl %d: key %q nil=%v", i, out[i].Key, out[i].Key == nil)
				}
				if !out[i].Time.Equal(in[i].Time) || out[i].Offset != in[i].Offset || len(out[i].Headers) != len(in[i].Headers) {
					t.Errorf("jsonl %d: %+v", i, out[i])
				}
			}
		}
	}
}

// What kaas consume -o json writes reads back as records: a sample of a
// production topic replays through a pipe.
func TestReadsConsumeOutput(t *testing.T) {
	line := `{"partition":0,"offset":12,"timestamp":"2026-01-02T03:04:05Z","key":"c-1","value":"{\"amount\":3}","headers":[{"key":"trace","value":"ab"}]}` + "\n" +
		`{"partition":1,"offset":4,"timestamp":"2026-01-02T03:04:06Z","key":null,"value":"/w==","encoding":"base64"}` + "\n"
	d, _ := newDecoder(strings.NewReader(line), FormatJSONL)
	r1, err := d.next()
	if err != nil || string(r1.Key) != "c-1" || r1.Offset != 12 || r1.Headers[0].Value != "ab" {
		t.Fatalf("%+v %v", r1, err)
	}
	r2, err := d.next()
	if err != nil || r2.Key != nil || !bytes.Equal(r2.Value, []byte{0xff}) || r2.Partition != 1 {
		t.Fatalf("%+v %v", r2, err)
	}
}

func TestStdinToFile(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.jsonl")
	env := Env{Stdin: strings.NewReader("one\ntwo\nthree\n")}
	in, err := OpenSource("stdin?format=text", env)
	if err != nil {
		t.Fatal(err)
	}
	f, err := OpenSink("file:"+out, env)
	if err != nil {
		t.Fatal(err)
	}
	if err := New([]Endpoint{in}, []Endpoint{f}, Options{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	if n := strings.Count(string(b), "\n"); n != 3 || !strings.Contains(string(b), `"value":"two"`) {
		t.Errorf("file:\n%s", b)
	}

	// And back out to stdout as CSV.
	var stdout bytes.Buffer
	env = Env{Stdout: &stdout}
	in, _ = OpenSource("file:"+out, env)
	o, _ := OpenSink("stdout?format=csv", env)
	if err := New([]Endpoint{in}, []Endpoint{o}, Options{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stdout.String(), "key,value,timestamp\n,one,\n") {
		t.Errorf("csv:\n%s", stdout.String())
	}
}

func TestSpecs(t *testing.T) {
	for spec, want := range map[string]string{
		"nope":               "no scheme",
		"gen:nothing":        "want one of",
		"gen:orders?rate=-1": "rate=-1",
		"file:x.txt?batch=0": "",
		"stdin?format=xml":   "format",
		"zzz:thing":          "kaas-source-zzz",
		"kafka:":             "name a topic",
	} {
		_, err := OpenSource(spec, Env{Brokers: []string{"localhost:1"}, PluginPath: []string{t.TempDir()}})
		if spec == "file:x.txt?batch=0" {
			continue // opens the file first; covered by batchSize
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", spec, err, want)
		}
	}
	if _, err := OpenSink("gen:orders", Env{}); err == nil || !strings.Contains(err.Error(), "only be read") {
		t.Errorf("gen as a sink: %v", err)
	}
}

func TestGenIsDeterministic(t *testing.T) {
	read := func() []string {
		g, err := OpenSource("gen:payments?count=250&seed=42&batch=100", Env{})
		if err != nil {
			t.Fatal(err)
		}
		k := &memSink{}
		if err := New([]Endpoint{g}, []Endpoint{snk(k)}, Options{}).Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k.values()
	}
	a, b := read(), read()
	if len(a) != 250 || strings.Join(a, "") != strings.Join(b, "") {
		t.Fatalf("%d records, same: %v", len(a), strings.Join(a, "") == strings.Join(b, ""))
	}
	for name := range generators {
		g, _ := OpenSource("gen:"+name+"?count=5", Env{})
		bt, err := g.Source.Next(context.Background())
		if err != nil || len(bt.Records) != 5 {
			t.Fatalf("%s: %v", name, err)
		}
		for _, r := range bt.Records {
			if !json.Valid(r.Value) || len(r.Key) == 0 {
				t.Errorf("%s: %s", name, r.Value)
			}
		}
	}
}

func TestGenRate(t *testing.T) {
	g, _ := OpenSource("gen:clicks?rate=200&count=60", Env{})
	k := &memSink{}
	start := time.Now()
	if err := New([]Endpoint{g}, []Endpoint{snk(k)}, Options{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 250*time.Millisecond || took > 2*time.Second {
		t.Errorf("60 records at 200/s took %s", took)
	}
}

// A plugin is any executable that speaks JSON lines: these are shell
// scripts, as a user's might be.
func TestExecPlugins(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("plugins here are shell scripts")
	}
	dir := t.TempDir()
	acks := filepath.Join(dir, "acks")
	written := filepath.Join(dir, "written")
	script := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script("kaas-source-tickets", `
echo '{"id":1,"key":"t-1","value":"{\"sev\":1}"}'
echo '{"id":2,"key":"t-2","value":"{\"sev\":3}"}'
echo '{"key":"t-3","value":"{\"sev\":2}"}'
exec 1>&-
cat > `+acks+`
`)
	script("kaas-sink-ledger", `
while IFS= read -r line; do
  echo "$line" >> `+written+`
  seq=$(echo "$line" | sed 's/.*"seq":\([0-9]*\).*/\1/')
  echo "{\"ack\":$seq}"
done
`)
	env := Env{PluginPath: []string{dir}}
	got := Discover(env)
	if len(got) != 2 || got[0].Scheme != "ledger" || got[1].Role != "source" {
		t.Fatalf("discovered %+v", got)
	}
	in, err := OpenSource("tickets://prod?project=ops", env)
	if err != nil {
		t.Fatal(err)
	}
	out, err := OpenSink("ledger:main", env)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := NewFilter("value.sev < 3")
	p := New([]Endpoint{in}, []Endpoint{out}, Options{Where: f})
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	w, _ := os.ReadFile(written)
	if n := strings.Count(string(w), "\n"); n != 2 || !strings.Contains(string(w), `"key":"t-3"`) || !strings.Contains(string(w), `"seq":2`) {
		t.Errorf("sink plugin wrote:\n%s", w)
	}
	a, _ := os.ReadFile(acks)
	if string(a) != "{\"ack\":1}\n{\"ack\":2}\n" {
		t.Errorf("source plugin was told %q", a)
	}
}

func TestExecPluginFailures(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("plugins here are shell scripts")
	}
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("kaas-source-quick", `printf '{"id":1,"value":"a"}\n{"id":2,"value":"b"}\n'`+"\n")
	write("kaas-source-broken", `echo 'oops not json'`+"\n")
	write("kaas-source-says", `echo '{"error":"token expired"}'; echo "auth failed for project x" >&2; sleep 1`+"\n")
	write("kaas-sink-refuses", `read line; echo '{"error":"quota","seq":1}'; cat >/dev/null`+"\n")
	write("kaas-sink-dies", `echo "panic: nil map" >&2; exit 3`+"\n")
	env := Env{PluginPath: []string{dir}}
	run := func(from, to string) error {
		in, err := OpenSource(from, env)
		if err != nil {
			return err
		}
		out, err := OpenSink(to, env)
		if err != nil {
			return err
		}
		return New([]Endpoint{in}, []Endpoint{out}, Options{}).Run(context.Background())
	}
	// A source that exits without reading its acks has simply declined them.
	if err := run("quick:x", "file:"+filepath.Join(dir, "q")); err != nil {
		t.Errorf("a source that exits after writing: %v", err)
	}
	for _, c := range []struct{ from, to, want string }{
		{"broken:x", "file:" + filepath.Join(dir, "o"), "isn't a JSON record"},
		{"says:x", "file:" + filepath.Join(dir, "o"), "token expired"},
		{"gen:orders?count=3", "refuses:x", "quota"},
		{"gen:orders?count=3", "dies:x", "panic: nil map"},
	} {
		if err := run(c.from, c.to); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s → %s: %v, want %q", c.from, c.to, err, c.want)
		}
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"kafka:orders?group=etl": "kafka:orders?group=etl",
		"https://u:pw@hooks.example.com/x?header=Authorization:Bearer%20abc&team=a": "https://u:***@hooks.example.com/x?header=***&team=a",
		"tickets://prod?token=s3cret": "tickets://prod?token=***",
		"stdin":                       "stdin",
	} {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%s) = %s, want %s", in, got, want)
		}
	}
}
