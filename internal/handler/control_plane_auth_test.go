package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestControlPlaneBearerAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		config        ControlPlaneAuthConfig
		authorization string
		wantStatus    int
		wantOperator  string
		wantSlot      string
	}{
		{name: "disabled", config: ControlPlaneAuthConfig{}, wantStatus: http.StatusNoContent, wantOperator: "unauthenticated", wantSlot: "disabled"},
		{name: "missing", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret"}, wantStatus: http.StatusUnauthorized},
		{name: "wrong", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret"}, authorization: "Bearer wrong", wantStatus: http.StatusUnauthorized},
		{name: "wrong scheme", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret"}, authorization: "Basic secret", wantStatus: http.StatusUnauthorized},
		{name: "valid active", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", OperatorID: "admin-v2"}, authorization: "Bearer secret", wantStatus: http.StatusNoContent, wantOperator: "admin-v2", wantSlot: "active"},
		{name: "valid previous", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", PreviousBearerToken: "old-secret", OperatorID: "admin-v2", PreviousOperatorID: "admin-v1"}, authorization: "Bearer old-secret", wantStatus: http.StatusNoContent, wantOperator: "admin-v1", wantSlot: "previous"},
		{name: "valid additional", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", OperatorID: "requester", AdditionalBearerCredentials: "approver-a=approve-a;incident-commander=break-glass"}, authorization: "Bearer approve-a", wantStatus: http.StatusNoContent, wantOperator: "approver-a", wantSlot: "additional"},
		{name: "invalid additional config", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", AdditionalBearerCredentials: "broken-entry"}, authorization: "Bearer secret", wantStatus: http.StatusServiceUnavailable},
		{name: "previous inherits operator", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", PreviousBearerToken: "old-secret", OperatorID: "admin"}, authorization: "Bearer old-secret", wantStatus: http.StatusNoContent, wantOperator: "admin", wantSlot: "previous"},
		{name: "case insensitive scheme", config: ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", OperatorID: "admin"}, authorization: "bearer secret", wantStatus: http.StatusNoContent, wantOperator: "admin", wantSlot: "active"},
		{name: "misconfigured", config: ControlPlaneAuthConfig{Enabled: true}, wantStatus: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := gin.New()
			group := r.Group("/admin")
			group.Use(controlPlaneBearerAuth(tt.config))
			group.GET("/probe", func(c *gin.Context) {
				c.Header("X-Test-Operator", c.GetString(controlPlaneOperatorKey))
				c.Header("X-Test-Credential-Slot", c.GetString(controlPlaneCredentialSlotKey))
				c.Status(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/admin/probe", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", w.Code, tt.wantStatus, w.Body.String())
			}
			if tt.wantStatus == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", w.Header().Get("WWW-Authenticate"))
			}
			if tt.wantOperator != "" && w.Header().Get("X-Test-Operator") != tt.wantOperator {
				t.Fatalf("operator = %q, want %q", w.Header().Get("X-Test-Operator"), tt.wantOperator)
			}
			if tt.wantSlot != "" && w.Header().Get("X-Test-Credential-Slot") != tt.wantSlot {
				t.Fatalf("credential slot = %q, want %q", w.Header().Get("X-Test-Credential-Slot"), tt.wantSlot)
			}
		})
	}
}

func TestBearerToken(t *testing.T) {
	tests := []struct {
		header string
		want   string
		ok     bool
	}{
		{header: "Bearer token", want: "token", ok: true},
		{header: "bearer token", want: "token", ok: true},
		{header: "  Bearer   token  ", want: "token", ok: true},
		{header: "", ok: false},
		{header: "Bearer", ok: false},
		{header: "Bearer token extra", ok: false},
		{header: "Basic token", ok: false},
	}

	for _, tt := range tests {
		got, ok := bearerToken(tt.header)
		if got != tt.want || ok != tt.ok {
			t.Fatalf("bearerToken(%q) = (%q, %v), want (%q, %v)", tt.header, got, ok, tt.want, tt.ok)
		}
	}
}
