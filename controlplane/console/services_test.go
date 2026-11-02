package console

import (
	"net/http"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/lifecycle"
)

func TestServicesAPI(t *testing.T) {
	// Without a manager the pages are off.
	none, _, _ := fleet(t, Options{})
	if m := get[Meta](t, none.URL+"/api/meta"); m.Manage {
		t.Error("meta says manage with no lifecycle manager")
	}
	if r, _ := http.Get(none.URL + "/api/services"); r.StatusCode != http.StatusNotFound {
		t.Errorf("services with no manager: %d", r.StatusCode)
	}

	demo := lifecycle.NewDemo(time.Now())
	ro, _, _ := fleet(t, Options{Lifecycle: demo})
	v := get[ServicesView](t, ro.URL+"/api/services")
	if len(v.Services) != 4 || v.Where == "" {
		t.Fatalf("services: %+v", v)
	}
	if r, _ := post(t, ro.URL+"/api/services/search/scale", `{"brokers":4}`, nil); r.StatusCode != http.StatusForbidden {
		t.Errorf("read-only console scaled: %d", r.StatusCode)
	}

	hs, _, _ := fleet(t, Options{Lifecycle: demo, AllowApply: true})
	for name, tc := range map[string]struct {
		path, body string
		headers    map[string]string
		code       int
	}{
		"cross-origin":    {"/api/services/search/scale", `{"brokers":4}`, map[string]string{"Origin": "https://evil.example"}, http.StatusForbidden},
		"form post":       {"/api/services/search/scale", `{"brokers":4}`, map[string]string{"Content-Type": "text/plain"}, http.StatusUnsupportedMediaType},
		"too many":        {"/api/services/search/scale", `{"brokers":99}`, nil, http.StatusBadRequest},
		"unknown":         {"/api/services/nope/scale", `{"brokers":4}`, nil, http.StatusNotFound},
		"bad json":        {"/api/services/search/scale", `{`, nil, http.StatusBadRequest},
		"unconfirmed":     {"/api/services/search/roll", `{"confirm":"other"}`, nil, http.StatusBadRequest},
		"invalid create":  {"/api/services", `{"name":"Bad Name","version":"4.2.0","brokers":3,"storage":"10Gi"}`, nil, http.StatusBadRequest},
		"duplicate":       {"/api/services", `{"name":"search","version":"4.2.0","brokers":3,"storage":"10Gi"}`, nil, http.StatusConflict},
		"busy":            {"/api/services/analytics/roll", `{"confirm":"analytics"}`, nil, http.StatusConflict},
		"autoscale bound": {"/api/services/search/autoscale", `{"autoscale":{"min":5,"max":3,"target":1000,"scaleDownAfter":"5m"}}`, nil, http.StatusBadRequest},
	} {
		if r, body := post(t, hs.URL+tc.path, tc.body, tc.headers); r.StatusCode != tc.code {
			t.Errorf("%s: %d %v, want %d", name, r.StatusCode, body, tc.code)
		}
	}

	for _, ok := range []struct{ path, body string }{
		{"/api/services", `{"name":"orders","version":"4.2.0","brokers":3,"storage":"10Gi"}`},
		{"/api/services/search/roll", `{"confirm":"search"}`},
		{"/api/services/payments/scale", `{"brokers":5}`},
		{"/api/services/edge-staging/autoscale", `{"autoscale":{"min":1,"max":4,"target":500,"scaleDownAfter":"2m"}}`},
	} {
		if r, body := post(t, hs.URL+ok.path, ok.body, map[string]string{"Origin": hs.URL}); r.StatusCode != 200 {
			t.Errorf("POST %s: %d %v", ok.path, r.StatusCode, body)
		}
	}
	v = get[ServicesView](t, hs.URL+"/api/services")
	byName := map[string]lifecycle.Service{}
	for _, s := range v.Services {
		byName[s.Name] = s
	}
	if s := byName["orders"]; s.Phase != lifecycle.Provisioning {
		t.Errorf("new cluster is %s", s.Phase)
	}
	if s := byName["search"]; s.Phase != lifecycle.Rolling {
		t.Errorf("search after a roll request is %s", s.Phase)
	}
	if s := byName["payments"]; s.Auto != nil || s.Brokers.Desired != 5 {
		t.Errorf("payments after scaling by hand: autoscale %v, %d brokers", s.Auto, s.Brokers.Desired)
	}
	if s := byName["edge-staging"]; s.Auto == nil || s.Auto.Max != 4 {
		t.Errorf("edge-staging autoscale %+v", s.Auto)
	}
}
