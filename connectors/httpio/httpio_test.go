package httpio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/pipe"
)

type memSink struct {
	mu  sync.Mutex
	got []pipe.Record
}

func (s *memSink) Write(_ context.Context, rs []pipe.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, rs...)
	return nil
}

func (s *memSink) Close() error { return nil }

func (s *memSink) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.got {
		out = append(out, string(r.Value))
	}
	return out
}

func recs(vals ...string) []pipe.Record {
	out := make([]pipe.Record, len(vals))
	for i, v := range vals {
		out[i] = pipe.Record{Key: []byte(fmt.Sprint("k", i)), Value: []byte(v)}
	}
	return out
}

func snk(s pipe.Sink) pipe.Endpoint { return pipe.Endpoint{Spec: "mem", Sink: s} }

func TestHTTPSinkRetries(t *testing.T) {
	var calls atomic.Int32
	var got []pipe.RecordJSON
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			if r.Header.Get("Authorization") != "Bearer t0k" || r.URL.Query().Get("retries") != "" || r.URL.Query().Get("team") != "x" {
				t.Errorf("request %s %v", r.URL, r.Header)
			}
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Error(err)
			}
		}
	}))
	defer srv.Close()
	sink, err := pipe.OpenSink(srv.URL+"/hook?retries=3&team=x&header=Authorization:Bearer%20t0k", pipe.Env{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Sink.Write(context.Background(), recs("a", "b")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || len(got) != 2 || *got[1].Value != "b" {
		t.Errorf("%d calls, got %+v", calls.Load(), got)
	}

	// A 400 is final: retrying won't change the server's mind.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "schema mismatch", http.StatusBadRequest)
	}))
	defer bad.Close()
	calls.Store(0)
	sink, _ = pipe.OpenSink(bad.URL, pipe.Env{})
	if err := sink.Sink.Write(context.Background(), recs("a")); err == nil || !strings.Contains(err.Error(), "schema mismatch") || calls.Load() != 1 {
		t.Errorf("%v after %d calls", err, calls.Load())
	}
	if retryAfter("2") != 2*time.Second || retryAfter("9999") != time.Minute || retryAfter("soon") != 0 {
		t.Error("retryAfter")
	}
}

func TestListenAnswersOnceWritten(t *testing.T) {
	ep, err := pipe.OpenSource("listen://127.0.0.1:0/ingest", pipe.Env{})
	if err != nil {
		t.Fatal(err)
	}
	addr := ep.Source.(*ListenSource).Addr().String()
	k := &memSink{}
	p := pipe.New([]pipe.Endpoint{ep}, []pipe.Endpoint{snk(k)}, pipe.Options{Max: 4})
	done := make(chan error)
	go func() { done <- p.Run(context.Background()) }()

	post := func(ctype, body string) (int, string) {
		resp, err := http.Post("http://"+addr+"/ingest", ctype, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := post("application/json", `[{"key":"a","value":"1"},{"key":"b","value":"2"}]`); code != 200 || !strings.Contains(body, `"written":2`) {
		t.Errorf("array: %d %s", code, body)
	}
	if code, _ := post("application/json", `{"value":`); code != 400 {
		t.Errorf("bad JSON: %d", code)
	}
	if code, _ := post("text/plain", "x\ny\n"); code != 200 {
		t.Errorf("text: %d", code)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(k.values(), ","); got != "1,2,x,y" {
		t.Errorf("wrote %s", got)
	}
}

func TestSpecs(t *testing.T) {
	if _, err := pipe.OpenSink("https://x?retries=lots", pipe.Env{}); err == nil || !strings.Contains(err.Error(), "retries") {
		t.Errorf("retries=lots: %v", err)
	}
	if _, err := pipe.OpenSource("https://example.com", pipe.Env{}); err == nil || !strings.Contains(err.Error(), "only be written") {
		t.Errorf("http as a source: %v", err)
	}
	if _, err := pipe.OpenSink("listen://:0/", pipe.Env{}); err == nil || !strings.Contains(err.Error(), "only be read") {
		t.Errorf("listen as a sink: %v", err)
	}
}
