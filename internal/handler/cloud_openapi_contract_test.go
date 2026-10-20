package handler

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/service"
)

// Prevents API contract drift: changes to cloud Gin paths/HTTP methods must
// update the checked-in OpenAPI artifact in the same change.
func TestCloudOpenAPIMatchesRuntimeRoutes(t *testing.T) {
	raw, err := os.ReadFile("../../api/cloud-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		OpenAPI string `json:"openapi"`
		Info    struct {
			Version string `json:"version"`
		} `json:"info"`
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("invalid OpenAPI JSON: %v", err)
	}
	if spec.OpenAPI != "3.1.0" || spec.Info.Version == "" {
		t.Fatal("OpenAPI version/info must be present")
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterCloudControlRoutes(r, service.NewCloudResourceService(nil, nil), ControlPlaneAuthConfig{}, nil, ControlPlaneAuditConfig{})
	RegisterCloudWorkflowRoutes(r, service.NewCloudWorkflowService(nil, nil), ControlPlaneAuthConfig{}, nil, ControlPlaneAuditConfig{})
	actual := make(map[string]bool)
	for _, route := range r.Routes() {
		if !strings.HasPrefix(route.Path, "/api/v1/cloud/") {
			continue
		}
		path := strings.ReplaceAll(strings.ReplaceAll(route.Path, ":id", "{id}"), ":tenant", "{tenant}")
		method := strings.ToLower(route.Method)
		actual[method+" "+path] = true
		operations := spec.Paths[path]
		if operations == nil || operations[method] == nil {
			t.Errorf("route absent from OpenAPI: %s %s", route.Method, route.Path)
			continue
		}
		var documented struct {
			Summary   string                     `json:"summary"`
			Responses map[string]json.RawMessage `json:"responses"`
		}
		if err := json.Unmarshal(operations[method], &documented); err != nil || documented.Summary == "" || len(documented.Responses) == 0 {
			t.Errorf("route missing summary/responses: %s %s: %v", route.Method, route.Path, err)
		}
	}
	if len(actual) == 0 {
		t.Fatal("no cloud routes registered")
	}
	for path, methods := range spec.Paths {
		for method := range methods {
			if method == "parameters" || strings.HasPrefix(method, "x-") {
				continue
			}
			if !actual[method+" "+path] {
				t.Errorf("OpenAPI has undeclared route: %s %s", strings.ToUpper(method), path)
			}
		}
	}
}
