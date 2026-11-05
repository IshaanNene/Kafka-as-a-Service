package console

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
)

// ServicesView is every managed cluster and where they run.
type ServicesView struct {
	Where    string              `json:"where"`
	Services []lifecycle.Service `json:"services"`
	// Monitored names the services that are also in the fleet, so their
	// pages can link across.
	Monitored []string `json:"monitored"`
}

// ScaleRequest sets a fixed broker count.
type ScaleRequest struct {
	Brokers int `json:"brokers"`
}

// AutoscaleRequest turns autoscaling on, or off with a null policy.
type AutoscaleRequest struct {
	Autoscale *lifecycle.Autoscale `json:"autoscale"`
}

// RollRequest confirms a roll.
type RollRequest struct {
	Confirm string `json:"confirm"` // the cluster's name, typed
}

func (s *Server) routeServices() {
	s.mux.HandleFunc("GET /api/services", s.services)
	s.mux.HandleFunc("POST /api/services", s.change(func(ctx context.Context, m lifecycle.Manager, r *http.Request) error {
		var spec lifecycle.Spec
		if err := decode(r, &spec); err != nil {
			return err
		}
		return m.Create(ctx, spec)
	}))
	s.mux.HandleFunc("POST /api/services/{name}/roll", s.change(func(ctx context.Context, m lifecycle.Manager, r *http.Request) error {
		var req RollRequest
		if err := decode(r, &req); err != nil {
			return err
		}
		if req.Confirm != r.PathValue("name") {
			return badRequest{"type the cluster's name to confirm"}
		}
		return m.Roll(ctx, r.PathValue("name"))
	}))
	s.mux.HandleFunc("POST /api/services/{name}/scale", s.change(func(ctx context.Context, m lifecycle.Manager, r *http.Request) error {
		var req ScaleRequest
		if err := decode(r, &req); err != nil {
			return err
		}
		return m.Scale(ctx, r.PathValue("name"), req.Brokers)
	}))
	s.mux.HandleFunc("POST /api/services/{name}/autoscale", s.change(func(ctx context.Context, m lifecycle.Manager, r *http.Request) error {
		var req AutoscaleRequest
		if err := decode(r, &req); err != nil {
			return err
		}
		return m.SetAutoscale(ctx, r.PathValue("name"), req.Autoscale)
	}))
}

func (s *Server) services(w http.ResponseWriter, r *http.Request) {
	m := s.opts.Lifecycle
	if m == nil {
		writeJSON(w, http.StatusNotFound, apiError{"this console doesn't manage clusters; start it with --kube"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	list, err := m.Services(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, apiError{err.Error()})
		return
	}
	v := ServicesView{Where: m.Where(), Services: list, Monitored: []string{}}
	for _, svc := range list {
		if _, ok := s.fleet.Cluster(svc.Name); ok {
			v.Monitored = append(v.Monitored, svc.Name)
		}
	}
	writeJSON(w, 200, v)
}

type badRequest struct{ msg string }

func (e badRequest) Error() string { return e.msg }

func decode(r *http.Request, v any) error {
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(v); err != nil {
		return badRequest{"bad request: " + err.Error()}
	}
	return nil
}

// change is a write to the lifecycle manager, behind the same guards as
// applying a plan: writes allowed, JSON only, from this origin.
func (s *Server) change(f func(context.Context, lifecycle.Manager, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := s.opts.Lifecycle
		switch {
		case m == nil:
			writeJSON(w, http.StatusNotFound, apiError{"this console doesn't manage clusters; start it with --kube"})
			return
		case !s.opts.AllowApply:
			writeJSON(w, http.StatusForbidden, apiError{"this console is read-only; start it with --allow-apply, or change the KafkaService with kubectl"})
			return
		}
		if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, apiError{"send application/json"})
			return
		}
		if !sameOrigin(r) {
			writeJSON(w, http.StatusForbidden, apiError{"cross-origin request refused"})
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
		defer cancel()
		err := f(ctx, m, r)
		var bad badRequest
		switch {
		case err == nil:
			writeJSON(w, 200, struct {
				OK bool `json:"ok"`
			}{true})
		case errors.Is(err, lifecycle.ErrNotFound):
			writeJSON(w, http.StatusNotFound, apiError{err.Error()})
		case errors.Is(err, lifecycle.ErrExists), errors.Is(err, lifecycle.ErrBusy):
			writeJSON(w, http.StatusConflict, apiError{err.Error()})
		case errors.As(err, &bad), errors.As(err, new(lifecycle.Invalid)):
			writeJSON(w, http.StatusBadRequest, apiError{err.Error()})
		default:
			writeJSON(w, http.StatusBadGateway, apiError{err.Error()})
		}
	}
}
