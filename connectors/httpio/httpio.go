// Package httpio is the pipe's HTTP endpoints: a webhook sink and a
// listening source. They live outside core/ because core imports no
// transport (ADR-0003); a program gets them by importing this package.
package httpio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/pipe"
)

func init() {
	sink := pipe.Kind{
		Summary: "a webhook: each batch POSTed as a JSON array, retried on 429 and 5xx with Retry-After honoured",
		Example: "https://hooks.example.com/orders · http://localhost:8080/ingest?format=jsonl",
		Sink:    openHTTPSink,
	}
	sink.Scheme = "http"
	pipe.Register(sink)
	sink.Scheme = "https"
	pipe.Register(sink)
	pipe.Register(pipe.Kind{
		Scheme:  "listen",
		Summary: "an HTTP endpoint that takes POSTs of JSON lines, a JSON array or text, answering 200 once written",
		Example: "listen://:8080/ingest · listen://127.0.0.1:9000/",
		Source:  openListenSource,
	})
}

// httpSink POSTs batches. A 2xx is written; 429 and 5xx, or a request
// that didn't get an answer, are retried with backoff up to retries=
// times; any other answer fails the write.
type httpSink struct {
	url     string
	format  string
	retries int
	headers http.Header
	client  *http.Client
}

func openHTTPSink(u *url.URL, _ pipe.Env) (pipe.Sink, error) {
	q := u.Query()
	format := query(u, "format", "json")
	if format != "json" && format != pipe.FormatJSONL {
		return nil, fmt.Errorf("format=%s: want json or jsonl", format)
	}
	retries, err := strconv.Atoi(query(u, "retries", "5"))
	if err != nil || retries < 0 {
		return nil, fmt.Errorf("retries=%s: want a number", q.Get("retries"))
	}
	timeout, err := time.ParseDuration(query(u, "timeout", "30s"))
	if err != nil {
		return nil, fmt.Errorf("timeout=%s: %w", q.Get("timeout"), err)
	}
	hs := http.Header{}
	for _, h := range q["header"] { // header=Authorization:Bearer%20…
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			return nil, fmt.Errorf("header=%s: want Name:value", h)
		}
		hs.Add(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	// The pipe's own parameters don't go to the server.
	for _, k := range []string{"format", "retries", "timeout", "header"} {
		q.Del(k)
	}
	target := *u
	target.RawQuery = q.Encode()
	return &httpSink{url: target.String(), format: format, retries: retries, headers: hs, client: &http.Client{Timeout: timeout}}, nil
}

func (s *httpSink) Write(ctx context.Context, rs []pipe.Record) error {
	var body bytes.Buffer
	ctype := "application/json"
	if s.format == pipe.FormatJSONL {
		ctype = "application/x-ndjson"
		enc := json.NewEncoder(&body)
		for _, r := range rs {
			if err := enc.Encode(pipe.ToJSON(r)); err != nil {
				return err
			}
		}
	} else {
		js := make([]pipe.RecordJSON, len(rs))
		for i, r := range rs {
			js[i] = pipe.ToJSON(r)
		}
		if err := json.NewEncoder(&body).Encode(js); err != nil {
			return err
		}
	}
	wait := 200 * time.Millisecond
	for try := 0; ; try++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body.Bytes()))
		if err != nil {
			return err
		}
		req.Header = s.headers.Clone()
		req.Header.Set("Content-Type", ctype)
		req.Header.Set("User-Agent", "kaas-pipe")
		resp, err := s.client.Do(req)
		var retry time.Duration
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			err = fmt.Errorf("POST %s: %w", s.url, err)
		default:
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return nil
			}
			err = fmt.Errorf("POST %s: %s %s", s.url, resp.Status, strings.TrimSpace(string(msg)))
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode/100 != 5 {
				return err // the server won't take it however often it's asked
			}
			retry = retryAfter(resp.Header.Get("Retry-After"))
		}
		if try >= s.retries {
			return fmt.Errorf("%w (gave up after %d tries)", err, try+1)
		}
		if retry == 0 {
			retry, wait = wait, min(wait*2, 30*time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// retryAfter reads a Retry-After header in seconds or as a date, capped
// at a minute; 0 for none.
func retryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil && n >= 0 {
		return min(time.Duration(n)*time.Second, time.Minute)
	}
	if t, err := http.ParseTime(h); err == nil {
		return min(max(time.Until(t), 0), time.Minute)
	}
	return 0
}

func (s *httpSink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// ListenSource serves POSTs: each request's records are one batch, and
// the request is answered 200 only after every sink wrote them, or 503
// when the pipe stops first, so the sender knows to send again.
type ListenSource struct {
	ln     net.Listener
	srv    *http.Server
	path   string
	reqs   chan listenReq
	closed chan struct{}
	once   sync.Once
}

type listenReq struct {
	recs []pipe.Record
	done chan struct{}
}

func openListenSource(u *url.URL, _ pipe.Env) (pipe.Source, error) {
	addr := u.Host
	if addr == "" {
		return nil, fmt.Errorf("%s: name an address, as listen://:8080/ingest", u)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &ListenSource{ln: ln, path: u.Path, reqs: make(chan listenReq), closed: make(chan struct{})}
	if s.path == "" {
		s.path = "/"
	}
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.serve)
	s.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go s.srv.Serve(ln) //nolint:errcheck // ends with ErrServerClosed on Close
	return s, nil
}

// Addr is where the source listens.
func (s *ListenSource) Addr() net.Addr { return s.ln.Addr() }

// MaxBody is the largest request a listen: source reads.
const MaxBody = 32 << 20

func (s *ListenSource) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		http.Error(w, "POST records here", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	recs, err := decodeBody(body, r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(recs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	req := listenReq{recs: recs, done: make(chan struct{})}
	select {
	case s.reqs <- req:
	case <-s.closed:
		http.Error(w, "the pipe stopped", http.StatusServiceUnavailable)
		return
	case <-r.Context().Done():
		return
	}
	select {
	case <-req.done:
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"written\":%d}\n", len(recs))
	case <-s.closed:
		http.Error(w, "the pipe stopped before writing these", http.StatusServiceUnavailable)
	case <-r.Context().Done():
	}
}

// decodeBody reads a request: a JSON array of records, JSON lines, or
// with a text/plain type a value a line.
func decodeBody(body []byte, ctype string) ([]pipe.Record, error) {
	if strings.HasPrefix(ctype, "text/plain") {
		var out []pipe.Record
		for _, l := range bytes.Split(bytes.TrimRight(body, "\r\n"), []byte("\n")) {
			out = append(out, pipe.Record{Value: append([]byte(nil), bytes.TrimRight(l, "\r")...)})
		}
		return out, nil
	}
	if t := bytes.TrimSpace(body); len(t) > 0 && t[0] == '[' {
		var js []pipe.RecordJSON
		if err := json.Unmarshal(t, &js); err != nil {
			return nil, fmt.Errorf("not a JSON array of records: %w", err)
		}
		out := make([]pipe.Record, len(js))
		for i, j := range js {
			r, err := pipe.FromJSON(j)
			if err != nil {
				return nil, fmt.Errorf("record %d: %w", i, err)
			}
			out[i] = r
		}
		return out, nil
	}
	return pipe.Decode(bytes.NewReader(body), pipe.FormatJSONL)
}

func (s *ListenSource) Next(ctx context.Context) (pipe.Batch, error) {
	select {
	case <-ctx.Done():
		return pipe.Batch{}, ctx.Err()
	case <-s.closed:
		return pipe.Batch{}, io.EOF
	case req := <-s.reqs:
		return pipe.Batch{Records: req.recs, Ack: func(context.Context) error {
			close(req.done)
			return nil
		}}, nil
	}
}

func (s *ListenSource) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = s.srv.Shutdown(ctx)
	})
	return err
}

func query(u *url.URL, key, def string) string {
	if v := u.Query().Get(key); v != "" {
		return v
	}
	return def
}
