package console

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
)

//go:embed web
var webFS embed.FS

// Options configure the HTTP side.
type Options struct {
	// AllowApply lets the console apply GitOps plans. Off by default: the
	// usual path is a reviewed pull request and `kaas apply` in CI.
	AllowApply bool
	Version    string
	Demo       bool // the fleet is the built-in demo
	// WebDir serves the web app from this directory instead of the copy
	// built into the binary, so UI changes show on reload.
	WebDir string
}

// Server serves the console: the API under /api and the web app.
type Server struct {
	fleet  *Fleet
	opts   Options
	mux    *http.ServeMux
	assets map[string]asset
}

type asset struct {
	body []byte
	etag string
	typ  string
}

// NewServer builds the handler for f.
func NewServer(f *Fleet, opts Options) (*Server, error) {
	s := &Server{fleet: f, opts: opts, mux: http.NewServeMux(), assets: map[string]asset{}}
	err := fs.WalkDir(webFS, "web", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := webFS.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		typ := mime.TypeByExtension(path.Ext(p))
		if typ == "" {
			typ = "application/octet-stream"
		}
		s.assets[strings.TrimPrefix(p, "web")] = asset{body: b, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, typ: typ}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.mux.HandleFunc("GET /api/meta", s.meta)
	s.mux.HandleFunc("GET /api/fleet", s.fleetView)
	s.mux.HandleFunc("GET /api/events", s.events)
	s.mux.HandleFunc("GET /api/clusters/{name}", s.cluster(func(c *Cluster, _ *http.Request) (any, int) { return c.view(), 200 }))
	s.mux.HandleFunc("GET /api/clusters/{name}/topics/{topic}", s.cluster(func(c *Cluster, r *http.Request) (any, int) {
		v, ok := c.topicView(r.PathValue("topic"))
		if !ok {
			return apiError{"no topic " + r.PathValue("topic") + " on " + c.Config.Name}, http.StatusNotFound
		}
		return v, 200
	}))
	s.mux.HandleFunc("GET /api/clusters/{name}/access", s.cluster(func(c *Cluster, _ *http.Request) (any, int) { return c.access(), 200 }))
	s.mux.HandleFunc("GET /api/clusters/{name}/schemas", s.cluster(func(c *Cluster, _ *http.Request) (any, int) { return c.schemas(), 200 }))
	s.mux.HandleFunc("GET /api/clusters/{name}/plan", s.cluster(func(c *Cluster, _ *http.Request) (any, int) {
		return c.planView(s.opts.AllowApply), 200
	}))
	s.mux.HandleFunc("POST /api/clusters/{name}/apply", s.apply)
	s.mux.HandleFunc("/api/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, apiError{"no such API"})
	})
	s.mux.HandleFunc("/", s.static)
	return s, nil
}

type apiError struct {
	Error string `json:"error"`
}

// ServeHTTP adds the security headers every answer carries.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cross-Origin-Opener-Policy", "same-origin")
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(buf.Bytes())
}

// Meta is what the browser needs before anything else.
type Meta struct {
	Version    string   `json:"version"`
	AllowApply bool     `json:"allowApply"`
	Demo       bool     `json:"demo"`
	Clusters   []string `json:"clusters"`
}

func (s *Server) meta(w http.ResponseWriter, _ *http.Request) {
	m := Meta{Version: s.opts.Version, AllowApply: s.opts.AllowApply, Demo: s.opts.Demo}
	for _, c := range s.fleet.Clusters {
		m.Clusters = append(m.Clusters, c.Config.Name)
	}
	writeJSON(w, 200, m)
}

func (s *Server) fleetView(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.fleet.view())
}

func (s *Server) cluster(f func(*Cluster, *http.Request) (any, int)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, ok := s.fleet.Cluster(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{"no cluster named " + r.PathValue("name")})
			return
		}
		v, code := f(c, r)
		writeJSON(w, code, v)
	}
}

// ApplyRequest names the plan the user reviewed.
type ApplyRequest struct {
	Plan    string `json:"plan"`
	Confirm string `json:"confirm"` // the cluster's name, typed
}

// ApplyResponse is what each operation did.
type ApplyResponse struct {
	Results []plan.Result `json:"results"`
	Error   string        `json:"error,omitempty"`
}

func (s *Server) apply(w http.ResponseWriter, r *http.Request) {
	c, ok := s.fleet.Cluster(r.PathValue("name"))
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{"no cluster named " + r.PathValue("name")})
		return
	}
	if !s.opts.AllowApply {
		writeJSON(w, http.StatusForbidden, apiError{"this console is read-only; start it with --allow-apply, or apply from CI with kaas apply"})
		return
	}
	// A cross-site form can't send JSON, and a cross-site fetch carries an
	// Origin that isn't ours.
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, apiError{"send application/json"})
		return
	}
	if !sameOrigin(r) {
		writeJSON(w, http.StatusForbidden, apiError{"cross-origin request refused"})
		return
	}
	var req ApplyRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{"bad request: " + err.Error()})
		return
	}
	if req.Confirm != c.Config.Name {
		writeJSON(w, http.StatusBadRequest, apiError{"type the cluster's name to confirm"})
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Minute)
	defer cancel()
	res, err := c.Apply(ctx, req.Plan)
	switch {
	case errors.Is(err, ErrPlanChanged):
		writeJSON(w, http.StatusConflict, apiError{"the plan changed since you reviewed it; review the new plan"})
	case errors.Is(err, ErrApplying):
		writeJSON(w, http.StatusConflict, apiError{err.Error()})
	case errors.Is(err, plan.ErrBlocked):
		writeJSON(w, http.StatusUnprocessableEntity, apiError{err.Error()})
	case err != nil && res == nil:
		writeJSON(w, http.StatusBadGateway, apiError{err.Error()})
	case err != nil:
		writeJSON(w, http.StatusBadGateway, ApplyResponse{Results: res, Error: err.Error()})
	default:
		writeJSON(w, 200, ApplyResponse{Results: res})
	}
}

func sameOrigin(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return r.Header.Get("Sec-Fetch-Site") == "" || r.Header.Get("Sec-Fetch-Site") == "same-origin"
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

// events streams the fleet's events as server-sent events. A reconnecting
// browser sends Last-Event-ID and gets what it missed from the ring.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, apiError{"streaming unsupported"})
		return
	}
	ch, cancel := s.fleet.subscribe()
	defer cancel()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprint(w, "retry: 3000\n\n")
	last, _ := strconv.ParseUint(r.Header.Get("Last-Event-ID"), 10, 64)
	send := func(e Event) {
		if e.Seq <= last {
			return
		}
		b, _ := json.Marshal(e)
		fmt.Fprintf(w, "id: %d\ndata: %s\n\n", e.Seq, b)
		last = e.Seq
	}
	if last > 0 {
		for _, e := range s.fleet.Events(maxEvents) {
			send(e)
		}
	}
	fl.Flush()
	beat := time.NewTicker(15 * time.Second)
	defer beat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-ch:
			send(e)
			fl.Flush()
		case <-beat.C:
			fmt.Fprint(w, ": beat\n\n")
			fl.Flush()
		}
	}
}

// static serves the web app; any path that isn't a file is the app's own
// route, so it gets index.html.
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a, ok := s.assets[r.URL.Path]
	if !ok || r.URL.Path == "/" {
		a = s.assets["/index.html"]
	}
	if s.opts.WebDir != "" {
		p := path.Clean(r.URL.Path)
		if !ok || p == "/" {
			p = "/index.html"
		}
		if b, err := os.ReadFile(filepath.Join(s.opts.WebDir, filepath.FromSlash(p))); err == nil {
			sum := sha256.Sum256(b)
			a = asset{body: b, typ: a.typ, etag: `"` + hex.EncodeToString(sum[:8]) + `"`}
		}
	}
	w.Header().Set("Content-Type", a.typ)
	w.Header().Set("ETag", a.etag)
	w.Header().Set("Cache-Control", "no-cache")
	if r.Header.Get("If-None-Match") == a.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = w.Write(a.body)
}
