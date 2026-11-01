// Package schemas reads a Confluent-compatible Schema Registry: its
// subjects, each subject's latest schema and its compatibility level.
// Karapace, Apicurio's ccompat API and Redpanda's registry speak the same
// REST API.
package schemas

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Subject is one subject's latest schema.
type Subject struct {
	Name          string `json:"name"`
	Version       int    `json:"version"`
	ID            int    `json:"id"`
	Type          string `json:"type"` // AVRO, PROTOBUF or JSON
	Compatibility string `json:"compatibility"`
	Schema        string `json:"schema"`
	Versions      int    `json:"versions"`
}

// Client talks to one registry.
type Client struct {
	base *url.URL
	hc   *http.Client
}

// New returns a client for the registry at rawURL. Credentials in the URL
// (https://user:pass@host) are sent as basic auth.
func New(rawURL string) (*Client, error) {
	u, err := url.Parse(strings.TrimSuffix(rawURL, "/"))
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("schema registry %q: want an http or https URL", rawURL)
	}
	return &Client{base: u, hc: &http.Client{Timeout: 10 * time.Second}}, nil
}

// maxBody bounds what one answer may be: a registry is trusted, but a
// misconfigured URL could point anywhere.
const maxBody = 16 << 20

func (c *Client) get(ctx context.Context, path string, out any) error {
	u := *c.base
	u.User = nil
	u.Path += path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if c.base.User != nil {
		pass, _ := c.base.User.Password()
		req.SetBasicAuth(c.base.User.Username(), pass)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Code    int    `json:"error_code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &e) == nil && e.Message != "" {
			return &Error{Status: resp.StatusCode, Code: e.Code, Message: e.Message}
		}
		return &Error{Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
	}
	return json.Unmarshal(body, out)
}

// Error is a registry's error answer.
type Error struct {
	Status  int
	Code    int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("schema registry: %d %s", e.Status, e.Message) }

// Subjects lists every subject with its latest schema.
func (c *Client) Subjects(ctx context.Context) ([]Subject, error) {
	var names []string
	if err := c.get(ctx, "/subjects", &names); err != nil {
		return nil, err
	}
	global := "BACKWARD"
	var g struct {
		Level string `json:"compatibilityLevel"`
	}
	if err := c.get(ctx, "/config", &g); err == nil && g.Level != "" {
		global = g.Level
	}
	out := make([]Subject, len(names))
	errs := make([]error, len(names))
	sem := make(chan struct{}, 8) // a few at a time: registries are often small
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			out[i], errs[i] = c.subject(ctx, name, global)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b Subject) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

func (c *Client) subject(ctx context.Context, name, global string) (Subject, error) {
	esc := url.PathEscape(name)
	var latest struct {
		Version int    `json:"version"`
		ID      int    `json:"id"`
		Type    string `json:"schemaType"`
		Schema  string `json:"schema"`
	}
	if err := c.get(ctx, "/subjects/"+esc+"/versions/latest", &latest); err != nil {
		return Subject{}, fmt.Errorf("%s: %w", name, err)
	}
	var versions []int
	if err := c.get(ctx, "/subjects/"+esc+"/versions", &versions); err != nil {
		return Subject{}, fmt.Errorf("%s: %w", name, err)
	}
	s := Subject{Name: name, Version: latest.Version, ID: latest.ID, Type: latest.Type, Schema: latest.Schema, Versions: len(versions), Compatibility: global}
	if s.Type == "" {
		s.Type = "AVRO" // the registry leaves it out for Avro
	}
	var cfg struct {
		Level string `json:"compatibilityLevel"`
	}
	var se *Error
	switch err := c.get(ctx, "/config/"+esc, &cfg); {
	case err == nil && cfg.Level != "":
		s.Compatibility = cfg.Level
	case errors.As(err, &se) && se.Status == http.StatusNotFound:
		// No subject-level setting: the global one applies.
	case err != nil:
		return Subject{}, fmt.Errorf("%s: %w", name, err)
	}
	return s, nil
}
