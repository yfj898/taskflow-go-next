package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
	"github.com/yfj898/taskflow-go/internal/testsupport"
	"github.com/yfj898/taskflow-go/pkg/cloudclient"
)

// TestKeycloakRealTokenTenantIsolation validates a real external OIDC identity
// provider: Keycloak signs client_credentials access tokens; TaskFlow fetches
// and checks its remote JWKS; the requests reach real MySQL through Gin.
// Only provider-side Kubernetes effects are mocked for isolation.
//
// Run with four Keycloak *service account* client secrets and a disposable
// MySQL database (never an existing TaskFlow database).
func TestKeycloakRealTokenTenantIsolation(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_KEYCLOAK_MYSQL_DSN")
	issuer := strings.TrimRight(os.Getenv("TASKFLOW_KEYCLOAK_ISSUER"), "/")
	if dsn == "" || issuer == "" {
		t.Skip("requires TASKFLOW_KEYCLOAK_MYSQL_DSN and TASKFLOW_KEYCLOAK_ISSUER")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("Keycloak integration DSN must point to a disposable *_ci or *_test schema")
	}
	secrets := map[string]string{
		"taskflow-tenant-a":          os.Getenv("TASKFLOW_KEYCLOAK_TENANT_A_SECRET"),
		"taskflow-tenant-b":          os.Getenv("TASKFLOW_KEYCLOAK_TENANT_B_SECRET"),
		"taskflow-tenant-a-reader":   os.Getenv("TASKFLOW_KEYCLOAK_READER_SECRET"),
		"taskflow-tenant-a-no-grant": os.Getenv("TASKFLOW_KEYCLOAK_NO_GRANT_SECRET"),
	}
	for name, secret := range secrets {
		if secret == "" {
			t.Fatalf("missing Keycloak service-account credential for %s", name)
		}
	}
	client := &http.Client{Timeout: 6 * time.Second}
	tokenEndpoint := issuer + "/protocol/openid-connect/token"
	tokens := map[string]string{}
	for clientID, secret := range secrets {
		form := url.Values{}
		form.Set("grant_type", "client_credentials")
		form.Set("client_id", clientID)
		form.Set("client_secret", secret)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, tokenEndpoint, strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Keycloak %s token request failed: %v", clientID, err)
		}
		var out struct {
			AccessToken string `json:"access_token"`
			Error       string `json:"error"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if decodeErr != nil || resp.StatusCode != http.StatusOK || out.AccessToken == "" {
			t.Fatalf("Keycloak %s did not issue access token: status=%d reason=%s decode=%v", clientID, resp.StatusCode, out.Error, decodeErr)
		}
		tokens[clientID] = out.AccessToken
	}

	assertClaims := func(clientID, wantTenant, wantGroup string) {
		t.Helper()
		parts := strings.Split(tokens[clientID], ".")
		if len(parts) != 3 {
			t.Fatal("Keycloak access token is not a compact signed JWT")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims map[string]any
		if err := json.Unmarshal(decoded, &claims); err != nil {
			t.Fatal(err)
		}
		if claims["iss"] != issuer || !claimAudienceContains(claims["aud"], "taskflow-control-plane") {
			t.Fatalf("%s: signed token has wrong issuer or audience: iss=%v aud=%v", clientID, claims["iss"], claims["aud"])
		}
		if wantTenant == "" {
			if actual, exists := claims["tenant_id"]; exists && actual != nil && actual != "" {
				t.Fatalf("%s: expected no tenant claim, got=%v", clientID, actual)
			}
		} else if claims["tenant_id"] != wantTenant {
			t.Fatalf("%s: tenant claim mismatch: got=%v want=%q", clientID, claims["tenant_id"], wantTenant)
		}
		if wantGroup != "" {
			found := false
			for _, group := range claimStrings(claims["groups"]) {
				found = found || group == wantGroup
			}
			if !found {
				t.Fatalf("%s: missing expected groups entry %q, got %v", clientID, wantGroup, claims["groups"])
			}
		}
	}
	assertClaims("taskflow-tenant-a", "team-a", "operators")
	assertClaims("taskflow-tenant-b", "team-b", "operators")
	assertClaims("taskflow-tenant-a-reader", "team-a", "readers")
	assertClaims("taskflow-tenant-a-no-grant", "", "operators")

	db, err := repository.NewMySQL(dsn, 12, 6, true)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.CloudResource{}, &domain.CloudOrder{}, &domain.CloudWorkflow{}, &domain.CloudWorkflowStep{}, &domain.CloudTenantQuota{}, &domain.CloudTenantQuotaBucket{}); err != nil {
		t.Fatal(err)
	}
	store := repository.NewCloudResourceRepository(db)
	for _, tenant := range []string{"team-a", "team-b"} {
		_, err := store.SetTenantQuota(context.Background(), tenant, cloudcontrol.QuotaDemand{CPUMilli: 10000, MemoryMiB: 10240, Replicas: 10})
		if err != nil {
			t.Fatal(err)
		}
	}
	auth := ControlPlaneAuthConfig{
		Enabled: true, Mode: "oidc", OIDCRBACEnabled: true, CloudTenantAuthzEnabled: true,
		OIDCIssuer: issuer, OIDCAudience: "taskflow-control-plane",
		OIDCJWKSURL:     issuer + "/protocol/openid-connect/certs",
		OIDCGroupsClaim: "groups", OIDCRoleMappings: "operators=control-plane-operator;readers=control-plane-reader",
		OIDCJWKSCacheTTL: 5 * time.Second, OIDCClockSkew: 2 * time.Second,
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterCloudControlRoutes(router, service.NewCloudResourceService(store, noOpCloudTenantProvider{}), auth, nil, ControlPlaneAuditConfig{})
	RegisterCloudWorkflowRoutes(router, service.NewCloudWorkflowService(store, nil), auth, nil, ControlPlaneAuditConfig{})
	server := httptest.NewServer(router)
	defer server.Close()
	makeClient := func(id string) *cloudclient.Client {
		c, err := cloudclient.New(server.URL, func(context.Context) (string, error) { return tokens[id], nil }, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	clientA, clientB, reader := makeClient("taskflow-tenant-a"), makeClient("taskflow-tenant-b"), makeClient("taskflow-tenant-a-reader")
	requestID := fmt.Sprintf("keycloak-real-%x", time.Now().UnixNano())
	create := func(c *cloudclient.Client, tenant string) *cloudclient.Mutation {
		t.Helper()
		result, err := c.CreateResource(context.Background(), cloudclient.CreateResource{
			TenantID: tenant, Name: "oidc-app-" + requestID,
			Spec: map[string]any{"image": "nginx:1.27", "replicas": 1, "cpu": "200m", "memory": "256Mi"},
		}, requestID)
		if err != nil {
			t.Fatalf("Keycloak-backed %s create: %v", tenant, err)
		}
		return result
	}
	a, b := create(clientA, "team-a"), create(clientB, "team-b")
	if a.Resource.ID == b.Resource.ID || a.Resource.TenantID != "team-a" || b.Resource.TenantID != "team-b" {
		t.Fatalf("client identities crossed tenants: a=%+v b=%+v", a, b)
	}
	if replay := create(clientA, "team-a"); replay.Order.ID != a.Order.ID {
		t.Fatal("same-tenant idempotency replay allocated a second order")
	}
	if _, err := clientA.GetResource(context.Background(), b.Resource.ID); err == nil {
		t.Fatal("Keycloak tenant A JWT could read tenant B resource")
	}
	if _, err := clientB.GetResource(context.Background(), a.Resource.ID); err == nil {
		t.Fatal("Keycloak tenant B JWT could read tenant A resource")
	}
	if _, err := reader.GetResource(context.Background(), a.Resource.ID); err != nil {
		t.Fatalf("reader may read same tenant resource: %v", err)
	}
	if _, err := reader.DeleteResource(context.Background(), a.Resource.ID, requestID+"-reader"); err == nil {
		t.Fatal("reader was allowed to delete resource")
	} else if status, ok := err.(*cloudclient.APIError); !ok || status.StatusCode != http.StatusForbidden {
		t.Fatalf("reader mutation should be 403: %v", err)
	}
	noGrant := makeClient("taskflow-tenant-a-no-grant")
	if _, err := noGrant.GetResource(context.Background(), a.Resource.ID); err == nil {
		t.Fatal("token missing tenant claim was authorized")
	} else if status, ok := err.(*cloudclient.APIError); !ok || status.StatusCode != http.StatusForbidden {
		t.Fatalf("missing tenant claim should be 403: %v", err)
	}
	badJWT := tokens["taskflow-tenant-a"] + "invalid"
	tampered, err := cloudclient.New(server.URL, func(context.Context) (string, error) { return badJWT, nil }, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tampered.GetResource(context.Background(), a.Resource.ID); err == nil {
		t.Fatal("tampered Keycloak signature was accepted")
	} else if status, ok := err.(*cloudclient.APIError); !ok || status.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tampered JWT should return 401: %v", err)
	}
	for clientName := range tokens {
		if tokens[clientName] == "" {
			t.Fatal("missing Keycloak token")
		}
	}
	if _, err := clientA.DeleteResource(context.Background(), a.Resource.ID, requestID+"-delete"); err != nil {
		t.Fatalf("tenant A authorized delete failed: %v", err)
	}
	if got, err := store.GetResource(context.Background(), b.Resource.ID); err != nil || got.Generation != 1 {
		t.Fatalf("Keycloak tenant B resource changed after A mutation: %+v err=%v", got, err)
	}
	// Keep the *actual Keycloak-signed* JWT unchanged and vary only the
	// verifier's trusted policy/clock, proving issuer, audience, and expiration
	// checks independently of any locally signed test token.
	assertVerifierRejects := func(name string, verifierConfig ControlPlaneAuthConfig, advance time.Duration, expected string) {
		t.Helper()
		verifier, err := newOIDCVerifier(verifierConfig)
		if err != nil {
			t.Fatalf("%s configure verifier: %v", name, err)
		}
		if advance > 0 {
			verifier.now = func() time.Time { return time.Now().Add(advance) }
		}
		_, _, _, err = verifier.verifyWithTenants(context.Background(), tokens["taskflow-tenant-a"])
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Fatalf("%s should reject Keycloak token for %s: %v", name, expected, err)
		}
	}
	assertVerifierRejects("expired-clock", auth, 2*time.Hour, "expired")
	wrongAud := auth
	wrongAud.OIDCAudience = "unrelated-service"
	assertVerifierRejects("wrong-audience", wrongAud, 0, "audience")
	wrongIssuer := auth
	wrongIssuer.OIDCIssuer = "https://incorrect.example.org/realms/taskflow"
	assertVerifierRejects("wrong-issuer", wrongIssuer, 0, "issuer")
	t.Log("Keycloak-issued signed tokens verified against JWKS; two tenants, reader-only RBAC, absent grant and forged signature checks passed")
}
