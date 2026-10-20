package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/service"
	"github.com/yfj898/taskflow-go/internal/verification"
)

func TestVerificationRoutesFailClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name   string
		auth   ControlPlaneAuthConfig
		header string
		want   int
	}{
		{"auth disabled", ControlPlaneAuthConfig{}, "", http.StatusServiceUnavailable},
		{"missing identity", ControlPlaneAuthConfig{Enabled: true, BearerToken: "test", OperatorID: "svc"}, "", http.StatusUnauthorized},
		{"wrong identity", ControlPlaneAuthConfig{Enabled: true, BearerToken: "test", OperatorID: "svc"}, "Bearer wrong", http.StatusUnauthorized},
		{"feature disabled", ControlPlaneAuthConfig{Enabled: true, BearerToken: "test", OperatorID: "svc"}, "Bearer test", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			RegisterVerificationRoutes(r, service.NewVerificationService(nil, nil), tc.auth)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/verification-runs/d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3", nil)
			req.Header.Set("Authorization", tc.header)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status %d body %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestGenericTaskCreationRejectsReservedType(t *testing.T) {
	r := NewRouter(service.NewTaskService(nil, nil), nil, nil, 100, 100, false, nil)
	for _, kind := range []string{"verify_change", "VERIFY_CHANGE", " verify_change "} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(`{"name":"bypass","type":"`+kind+`"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("type %q accepted: %d %s", kind, w.Code, w.Body.String())
		}
	}
}

func TestVerificationProjectAuthorizationPrecedesIdempotency(t *testing.T) {
	// A nil repository deliberately makes any idempotency read a test failure.
	// The caller-controlled extra subject field cannot grant project access.
	cfg := &verification.Config{Projects: []verification.Project{{ID: "registered-project", Subjects: []string{"different-subject"}}}}
	r := gin.New()
	RegisterVerificationRoutes(r, service.NewVerificationService(nil, cfg), ControlPlaneAuthConfig{Enabled: true, BearerToken: "test", OperatorID: "caller"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/verification-runs", strings.NewReader(`{"run_id":"d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3","project_id":"registered-project","subject":"different-subject"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("authorization bypassed: %d %s", w.Code, w.Body.String())
	}
}
