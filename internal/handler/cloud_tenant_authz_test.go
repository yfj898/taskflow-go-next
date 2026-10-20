package handler

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestCloudTenantAuthorizationOIDC(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": "tenant-test-key", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()

	auth := ControlPlaneAuthConfig{
		Enabled: true, Mode: "oidc", OIDCRBACEnabled: true, CloudTenantAuthzEnabled: true,
		OIDCIssuer: "https://auth.example", OIDCAudience: "taskflow", OIDCJWKSURL: jwks.URL,
		OIDCGroupsClaim: "groups", OIDCRoleMappings: "developers=control-plane-operator",
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(controlPlaneBearerAuth(auth), controlPlaneOIDCRBAC(auth), cloudTenantIdentityGuard(auth))
	r.POST("/api/v1/cloud/tenant/:tenant", func(c *gin.Context) {
		if !cloudTenantAllowed(c, auth, c.Param("tenant")) {
			return
		}
		c.Status(http.StatusNoContent)
	})
	r.GET("/api/v1/cloud/resources", func(c *gin.Context) {
		tenant, ok := cloudTenantForList(c, auth, c.Query("tenant_id"))
		if !ok {
			return
		}
		c.JSON(http.StatusOK, gin.H{"tenant_id": tenant})
	})

	now := time.Now().UTC()
	base := map[string]any{
		"iss": auth.OIDCIssuer, "aud": auth.OIDCAudience, "sub": "alice",
		"exp": now.Add(time.Minute).Unix(), "groups": []string{"developers"},
	}
	tokenFor := func(claims map[string]any) string {
		t.Helper()
		merged := make(map[string]any)
		for k, v := range base {
			merged[k] = v
		}
		for k, v := range claims {
			merged[k] = v
		}
		return signPhase41JWT(t, key, "tenant-test-key", merged)
	}
	issued := tokenFor(map[string]any{"tenant_ids": []string{"team-a", "team-b"}})
	oneTenant := tokenFor(map[string]any{"tenant_id": "team-a"})
	noTenant := tokenFor(map[string]any{})
	malformed := tokenFor(map[string]any{"tenant_ids": map[string]any{"team-a": true}})
	tampered := issued[:len(issued)-1] + "!"

	for _, tc := range []struct {
		name, method, path, token string
		want                      int
		bodyContains              string
	}{
		{"allowed", "POST", "/api/v1/cloud/tenant/team-a", oneTenant, 204, ""},
		{"denied_other_tenant", "POST", "/api/v1/cloud/tenant/team-b", oneTenant, 403, ""},
		{"multi_tenant_allowed", "POST", "/api/v1/cloud/tenant/team-b", issued, 204, ""},
		{"missing_claim", "POST", "/api/v1/cloud/tenant/team-a", noTenant, 403, ""},
		{"malformed_claim", "POST", "/api/v1/cloud/tenant/team-a", malformed, 403, ""},
		{"forged_header_not_a_grant", "POST", "/api/v1/cloud/tenant/team-b", oneTenant, 403, ""},
		{"unsigned_mutation", "POST", "/api/v1/cloud/tenant/team-a", tampered, 401, ""},
		{"list_single_tenant", "GET", "/api/v1/cloud/resources", oneTenant, 200, `"tenant_id":"team-a"`},
		{"list_multi_requires_selection", "GET", "/api/v1/cloud/resources", issued, 400, ""},
		{"list_multi_selected", "GET", "/api/v1/cloud/resources?tenant_id=team-b", issued, 200, `"tenant_id":"team-b"`},
		{"list_cross_tenant_denied", "GET", "/api/v1/cloud/resources?tenant_id=team-b", oneTenant, 403, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			req.Header.Set("X-Tenant-ID", "team-b")
			res := httptest.NewRecorder()
			r.ServeHTTP(res, req)
			if res.Code != tc.want || (tc.bodyContains != "" && !strings.Contains(res.Body.String(), tc.bodyContains)) {
				t.Fatalf("status=%d want=%d body=%q", res.Code, tc.want, res.Body.String())
			}
		})
	}
}

func TestCloudTenantAuthorizationDisabledCompatible(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	auth := ControlPlaneAuthConfig{}
	r.Use(controlPlaneBearerAuth(auth), cloudTenantIdentityGuard(auth))
	r.GET("/legacy", func(c *gin.Context) {
		if !cloudTenantAllowed(c, auth, "any-tenant") {
			return
		}
		selected, ok := cloudTenantForList(c, auth, "")
		if !ok || selected != "" {
			c.Status(http.StatusBadRequest)
			return
		}
		c.Status(http.StatusNoContent)
	})
	res := httptest.NewRecorder()
	r.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/legacy", nil))
	if res.Code != http.StatusNoContent {
		t.Fatalf("compatibility regression: status=%d body=%s", res.Code, res.Body.String())
	}
}
