package handler

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/streamscale/streamscale/analytics-api/api"
)

type specDoc struct {
	OpenAPI string `yaml:"openapi"`
	Paths   map[string]map[string]struct {
		OperationID string `yaml:"operationId"`
		Parameters  []struct {
			Ref  string `yaml:"$ref"`
			Name string `yaml:"name"`
		} `yaml:"parameters"`
		Responses map[string]any `yaml:"responses"`
	} `yaml:"paths"`
	Components struct {
		Parameters map[string]struct {
			Name string `yaml:"name"`
		} `yaml:"parameters"`
	} `yaml:"components"`
}

// sample values that pass validation, by parameter name.
var sampleParam = map[string]string{
	"from":        "2026-11-20T11:00:00Z",
	"to":          "2026-11-20T12:00:00Z",
	"interval":    "1m",
	"limit":       "10",
	"live_window": "30",
	"event_type":  "LOGIN",
	"kind":        "all",
	"severity":    "warning",
	"min_z":       "4",
	"class":       "permanent",
}

// TestOpenAPIMatchesRouter keeps the spec honest: same paths as the router,
// every documented parameter is accepted, undocumented ones are rejected.
func TestOpenAPIMatchesRouter(t *testing.T) {
	var doc specDoc
	if err := yaml.Unmarshal(api.OpenAPI, &doc); err != nil {
		t.Fatalf("openapi.yaml is not valid YAML: %v", err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.") {
		t.Fatalf("openapi version %q", doc.OpenAPI)
	}

	a := New(Options{Repo: &fakeRepo{}, Live: fakeLive{}, Now: func() time.Time { return now }, OpenAPI: api.OpenAPI})
	var routes, specPaths []string
	for _, r := range a.Routes() {
		routes = append(routes, r.Path)
	}
	for p, ops := range doc.Paths {
		specPaths = append(specPaths, p)
		if len(ops) != 1 || ops["get"].OperationID == "" {
			t.Errorf("%s: expected exactly one GET operation with operationId", p)
		}
		if _, ok := ops["get"].Responses["200"]; !ok {
			t.Errorf("%s: missing 200 response", p)
		}
	}
	sort.Strings(routes)
	sort.Strings(specPaths)
	if strings.Join(routes, ",") != strings.Join(specPaths, ",") {
		t.Fatalf("router %v\nspec   %v", routes, specPaths)
	}

	h := a.Router(slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	for p, ops := range doc.Paths {
		var names []string
		for _, prm := range ops["get"].Parameters {
			name := prm.Name
			if prm.Ref != "" {
				name = doc.Components.Parameters[strings.TrimPrefix(prm.Ref, "#/components/parameters/")].Name
			}
			if name == "" {
				t.Fatalf("%s: unresolved parameter %+v", p, prm)
			}
			if name == "cursor" {
				continue // needs a real cursor; covered by pagination tests
			}
			names = append(names, name+"="+sampleParam[name])
		}
		target := p
		if len(names) > 0 {
			target += "?" + strings.Join(names, "&")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code == http.StatusBadRequest {
			t.Errorf("%s rejects documented parameters: %s", target, rec.Body.String())
		}
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p+"?undocumented=1", nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s accepts an undocumented parameter (status %d)", p, rec.Code)
		}
	}
}
