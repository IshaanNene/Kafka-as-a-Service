package console

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

const desired = `cluster: dev
topics:
  orders: {partitions: 6}
  refunds: {partitions: 2}
policies:
  - {name: two, rule: topic.partitions >= 2}
`

// fleet starts a console over one kfake cluster with orders at 3
// partitions, so the desired file has two changes to make.
func fleet(t *testing.T, opts Options) (*httptest.Server, *Fleet, *kfake.Cluster) {
	t.Helper()
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(2))
	if _, err := kadm.NewClient(cl).CreateTopics(context.Background(), 3, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "dev.yaml")
	if err := os.WriteFile(path, []byte(desired), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := NewFleet(Config{Clusters: []ClusterConfig{
		{Name: "dev", Brokers: c.ListenAddrs(), Env: "development", Desired: path},
		{Name: "gone", Brokers: []string{"127.0.0.1:1"}},
	}}, Intervals{Groups: 200 * time.Millisecond, Access: 300 * time.Millisecond, Plan: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	srv, err := NewServer(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return hs, f, c
}

func get[T any](t *testing.T, url string) T {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	if r.StatusCode != 200 {
		t.Fatalf("GET %s: %s", url, r.Status)
	}
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestFleetAPI(t *testing.T) {
	hs, _, _ := fleet(t, Options{Version: "test"})

	m := get[Meta](t, hs.URL+"/api/meta")
	if m.AllowApply || len(m.Clusters) != 2 || m.Version != "test" {
		t.Fatalf("meta: %+v", m)
	}
	var v FleetView
	eventually(t, "dev to be healthy with a plan and gone to be unreachable", func() bool {
		v = get[FleetView](t, hs.URL+"/api/fleet")
		dev, gone := v.Clusters[0], v.Clusters[1]
		return dev.Status == StatusHealthy && dev.Drift == 2 && gone.Status == StatusUnreachable
	})
	dev := v.Clusters[0]
	if dev.Brokers != 2 || dev.Topics != 1 || dev.Partitions != 3 || !dev.GitOps || dev.Env != "development" {
		t.Errorf("dev card: %+v", dev)
	}
	if v.Totals.Clusters != 2 || v.Totals.Healthy != 1 || v.Totals.Drift != 2 {
		t.Errorf("totals: %+v", v.Totals)
	}

	cv := get[ClusterView](t, hs.URL+"/api/clusters/dev")
	if len(cv.Brokers) != 2 || len(cv.Topics) != 1 || cv.Topics[0].Name != "orders" {
		t.Errorf("cluster view: %+v", cv)
	}
	tv := get[TopicView](t, hs.URL+"/api/clusters/dev/topics/orders")
	if len(tv.Partitions) != 3 || tv.Row.Partitions != 3 {
		t.Errorf("topic view: %+v", tv)
	}
	pv := get[PlanView](t, hs.URL+"/api/clusters/dev/plan")
	if !pv.Configured || pv.CanApply || len(pv.Plan.Ops) != 2 || !pv.Plan.Checked {
		t.Errorf("plan view (read-only console): %+v", pv)
	}
	// Lists are arrays, never null, even for a cluster that never answered.
	r, _ := http.Get(hs.URL + "/api/clusters/gone")
	var raw map[string]any
	_ = json.NewDecoder(r.Body).Decode(&raw)
	_ = r.Body.Close()
	for _, k := range []string{"brokers", "topics", "groups", "events"} {
		if _, ok := raw[k].([]any); !ok {
			t.Errorf("%s is %v, want an array", k, raw[k])
		}
	}

	for path, code := range map[string]int{
		"/api/clusters/nope":                   404,
		"/api/clusters/dev/topics/nope":        404,
		"/api/nothing":                         404,
		"/clusters/dev/gitops":                 200, // the app's own route
		"/fonts/martian-mono.woff2":            200,
		"/api/clusters/dev/schemas":            200,
		"/api/clusters/dev/access":             200,
		"/api/clusters/dev/topics/orders/../x": 404,
	} {
		r, err := http.Get(hs.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != code {
			t.Errorf("GET %s: %d, want %d", path, r.StatusCode, code)
		}
		if csp := r.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
			t.Errorf("GET %s: CSP %q", path, csp)
		}
	}
}

func post(t *testing.T, url, body string, h map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range h {
		req.Header.Set(k, v)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(r.Body).Decode(&out)
	return r, out
}

func TestApplyIsGuarded(t *testing.T) {
	ro, _, _ := fleet(t, Options{})
	if r, _ := post(t, ro.URL+"/api/clusters/dev/apply", `{"plan":"x","confirm":"dev"}`, nil); r.StatusCode != http.StatusForbidden {
		t.Errorf("read-only console applied: %d", r.StatusCode)
	}

	hs, f, _ := fleet(t, Options{AllowApply: true})
	dev, _ := f.Cluster("dev")
	var p *plan.Plan
	eventually(t, "a plan", func() bool {
		dev.mu.RLock()
		defer dev.mu.RUnlock()
		p = dev.plan
		return p != nil && len(p.Ops) == 2
	})
	url := hs.URL + "/api/clusters/dev/apply"
	ok := `{"plan":"` + p.ID + `","confirm":"dev"}`
	for name, tc := range map[string]struct {
		body    string
		headers map[string]string
		code    int
	}{
		"cross-origin":    {ok, map[string]string{"Origin": "https://evil.example"}, http.StatusForbidden},
		"cross-site":      {ok, map[string]string{"Sec-Fetch-Site": "cross-site"}, http.StatusForbidden},
		"form post":       {ok, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, http.StatusUnsupportedMediaType},
		"unconfirmed":     {`{"plan":"` + p.ID + `","confirm":"prod"}`, nil, http.StatusBadRequest},
		"stale plan":      {`{"plan":"0000","confirm":"dev"}`, nil, http.StatusConflict},
		"bad json":        {`{`, nil, http.StatusBadRequest},
		"unknown cluster": {ok, nil, 0},
	} {
		u := url
		if name == "unknown cluster" {
			u, tc.code = hs.URL+"/api/clusters/nope/apply", http.StatusNotFound
		}
		if r, body := post(t, u, tc.body, tc.headers); r.StatusCode != tc.code {
			t.Errorf("%s: %d %v, want %d", name, r.StatusCode, body, tc.code)
		}
	}

	r, body := post(t, url, ok, map[string]string{"Origin": hs.URL})
	if r.StatusCode != 200 {
		t.Fatalf("apply: %d %v", r.StatusCode, body)
	}
	if res, _ := body["results"].([]any); len(res) != 2 {
		t.Fatalf("results: %v", body)
	}
	pv := get[PlanView](t, hs.URL+"/api/clusters/dev/plan")
	if len(pv.Plan.Ops) != 0 {
		t.Fatalf("not converged after apply: %+v", pv.Plan.Ops)
	}
	applied := false
	for _, e := range f.Events(maxEvents) {
		applied = applied || (e.Kind == "PlanApplied" && e.Cluster == "dev")
	}
	if !applied {
		t.Errorf("no PlanApplied event: %+v", f.Events(maxEvents))
	}
}

func TestEventStream(t *testing.T) {
	hs, f, _ := fleet(t, Options{})
	req, _ := http.NewRequest(http.MethodGet, hs.URL+"/api/events", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	if ct := r.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	f.publish(Event{Cluster: "dev", Kind: "Test", Severity: SevInfo, Text: "hello"})
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e); err != nil {
			t.Fatal(err)
		}
		if e.Kind == "Test" {
			if e.Seq == 0 || e.At.IsZero() {
				t.Errorf("event not stamped: %+v", e)
			}
			return
		}
	}
	t.Fatalf("stream ended without the event: %v", sc.Err())
}

func TestEventsReplayAfterReconnect(t *testing.T) {
	f := &Fleet{subs: map[chan Event]struct{}{}}
	for i := range 3 {
		f.publish(Event{Kind: "E", Text: string(rune('a' + i))})
	}
	srv, err := NewServer(f, Options{})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+"/api/events", nil)
	req.Header.Set("Last-Event-ID", "1")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	sc := bufio.NewScanner(r.Body)
	var ids []string
	for sc.Scan() && len(ids) < 2 {
		if id, ok := strings.CutPrefix(sc.Text(), "id: "); ok {
			ids = append(ids, id)
		}
	}
	if strings.Join(ids, ",") != "2,3" {
		t.Fatalf("replayed %v, want 2,3", ids)
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "fleet.yaml")
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c, err := LoadConfig(write("clusters:\n  - {name: a, brokers: [x:9092], desired: gitops/a.yaml}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Clusters[0].Desired != filepath.Join(dir, "gitops", "a.yaml") {
		t.Errorf("desired path not made relative to the fleet file: %s", c.Clusters[0].Desired)
	}
	for _, bad := range []string{
		"clusters: []",
		"clusters:\n  - {brokers: [x:1]}",
		"clusters:\n  - {name: a}",
		"clusters:\n  - {name: a, brokers: [x:1]}\n  - {name: a, brokers: [y:1]}",
		"clusters:\n  - {name: a, brokers: [x:1], tls: true}",
	} {
		if _, err := LoadConfig(write(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestDemoStarts(t *testing.T) {
	if testing.Short() {
		t.Skip("starts four in-memory clusters")
	}
	d, err := StartDemo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if len(d.Config.Clusters) != 5 {
		t.Fatalf("demo clusters: %d", len(d.Config.Clusters))
	}
	for _, c := range d.Config.Clusters[:4] {
		if _, err := plan.Load(c.Desired); err != nil {
			t.Errorf("%s's desired file doesn't parse: %v", c.Name, err)
		}
	}
}
