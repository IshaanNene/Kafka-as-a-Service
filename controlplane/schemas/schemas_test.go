package schemas

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestSubjects(t *testing.T) {
	m := NewMemory()
	m.Register("orders-value", "AVRO", `{"type":"record","name":"Order","fields":[]}`, "")
	m.Register("orders-value", "AVRO", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`, "")
	m.Register("payments-value", "PROTOBUF", `syntax = "proto3"; message Payment {}`, "FULL_TRANSITIVE")
	var (
		mu         sync.Mutex
		user, pass string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		user, pass, _ = r.BasicAuth()
		mu.Unlock()
		m.ServeHTTP(w, r)
	}))
	defer srv.Close()

	c, err := New("http://reader:s3cret@" + srv.Listener.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	ss, err := c.Subjects(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if user != "reader" || pass != "s3cret" {
		t.Errorf("basic auth: %q %q", user, pass)
	}
	if len(ss) != 2 {
		t.Fatalf("subjects: %+v", ss)
	}
	o, p := ss[0], ss[1]
	if o.Name != "orders-value" || o.Version != 2 || o.Versions != 2 || o.Type != "AVRO" || o.Compatibility != "BACKWARD" || o.ID != 2 {
		t.Errorf("orders: %+v", o)
	}
	if p.Type != "PROTOBUF" || p.Compatibility != "FULL_TRANSITIVE" {
		t.Errorf("payments: %+v", p)
	}
}

func TestErrors(t *testing.T) {
	if _, err := New("ftp://x"); err == nil {
		t.Error("accepted ftp")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error_code":40101,"message":"Unauthorized"}`))
	}))
	defer srv.Close()
	c, _ := New(srv.URL)
	_, err := c.Subjects(context.Background())
	var se *Error
	if !errors.As(err, &se) || se.Status != 401 || se.Message != "Unauthorized" {
		t.Fatalf("err: %v", err)
	}
}
