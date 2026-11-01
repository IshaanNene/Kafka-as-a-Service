package schemas

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Memory is an in-memory registry serving the read side of the REST API.
// The console's demo fleet and the tests use it.
type Memory struct {
	mu       sync.Mutex
	global   string
	subjects map[string]*memSubject
	nextID   int
}

type memSubject struct {
	compat   string
	versions []memVersion
}

type memVersion struct {
	ID     int
	Type   string
	Schema string
}

// NewMemory returns an empty registry with BACKWARD compatibility.
func NewMemory() *Memory {
	return &Memory{global: "BACKWARD", subjects: map[string]*memSubject{}, nextID: 1}
}

// Register adds a version of subject. typ is AVRO, PROTOBUF or JSON;
// compat, when not empty, sets the subject's compatibility level.
func (m *Memory) Register(subject, typ, schema, compat string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.subjects[subject]
	if s == nil {
		s = &memSubject{}
		m.subjects[subject] = s
	}
	if compat != "" {
		s.compat = compat
	}
	if typ == "AVRO" {
		typ = "" // as a real registry answers
	}
	s.versions = append(s.versions, memVersion{ID: m.nextID, Type: typ, Schema: schema})
	m.nextID++
}

func (m *Memory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	notFound := func(code int, msg string) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": code, "message": msg})
	}
	reply := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	switch {
	case len(parts) == 1 && parts[0] == "subjects":
		names := make([]string, 0, len(m.subjects))
		for n := range m.subjects {
			names = append(names, n)
		}
		sort.Strings(names)
		reply(names)
	case len(parts) == 1 && parts[0] == "config":
		reply(map[string]string{"compatibilityLevel": m.global})
	case len(parts) == 2 && parts[0] == "config":
		s := m.subjects[parts[1]]
		if s == nil || s.compat == "" {
			notFound(40408, "Subject '"+parts[1]+"' does not have subject-level compatibility configured")
			return
		}
		reply(map[string]string{"compatibilityLevel": s.compat})
	case len(parts) >= 3 && parts[0] == "subjects" && parts[2] == "versions":
		s := m.subjects[parts[1]]
		if s == nil {
			notFound(40401, "Subject '"+parts[1]+"' not found.")
			return
		}
		if len(parts) == 3 {
			vs := make([]int, len(s.versions))
			for i := range vs {
				vs[i] = i + 1
			}
			reply(vs)
			return
		}
		if len(parts) == 4 && parts[3] == "latest" {
			v := s.versions[len(s.versions)-1]
			out := map[string]any{"subject": parts[1], "version": len(s.versions), "id": v.ID, "schema": v.Schema}
			if v.Type != "" {
				out["schemaType"] = v.Type
			}
			reply(out)
			return
		}
		notFound(40402, "Version not found.")
	default:
		notFound(404, "Not found")
	}
}
