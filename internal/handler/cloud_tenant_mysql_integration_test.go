package handler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
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

type noOpCloudTenantProvider struct{}

func (noOpCloudTenantProvider) Apply(context.Context, domain.CloudResource) (cloudcontrol.ProviderSnapshot, error) {
	return cloudcontrol.ProviderSnapshot{State: domain.CloudResourceActive}, nil
}
func (noOpCloudTenantProvider) Delete(context.Context, domain.CloudResource) (cloudcontrol.ProviderSnapshot, error) {
	return cloudcontrol.ProviderSnapshot{State: domain.CloudResourceDeleted}, nil
}
func (noOpCloudTenantProvider) Observe(context.Context, domain.CloudResource) (cloudcontrol.ProviderSnapshot, error) {
	return cloudcontrol.ProviderSnapshot{State: domain.CloudResourceActive}, nil
}

// Exercises actual HTTP auth -> Gin tenant binding -> service -> MySQL. Only
// the Kubernetes provider is mocked, and no Kubernetes side effects are run.
func TestCloudTenantRouterMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_MYSQL_INTEGRATION_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("unsafe integration DSN: database must be an explicit *_ci or *_test schema")
	}
	db, err := repository.NewMySQL(dsn, 10, 4, true)
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
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	tenantA, tenantB := "tenant-a-"+suffix, "tenant-b-"+suffix
	for _, tenant := range []string{tenantA, tenantB} {
		if _, err := store.SetTenantQuota(context.Background(), tenant, cloudcontrol.QuotaDemand{CPUMilli: 10000, MemoryMiB: 10240, Replicas: 10}); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(tenant string) (*domain.CloudResource, *domain.CloudOrder) {
		t.Helper()
		resource := &domain.CloudResource{
			TenantID: tenant, Name: "app-" + suffix, Kind: "deployment", Provider: "kubernetes",
			OrchestrationMode: domain.CloudOrchestrationDirect, DesiredState: domain.CloudResourceDesiredActive,
			ActualState: domain.CloudResourceActive, Generation: 1, DesiredSpec: `{"image":"nginx:1.27","replicas":1,"cpu":"200m","memory":"256Mi"}`,
		}
		order := &domain.CloudOrder{TenantID: tenant, Type: domain.CloudOrderCreate, Status: domain.CloudOrderPending, IdempotencyKey: "http-" + tenant}
		if err := store.CreateResourceWithOrder(context.Background(), resource, order, cloudcontrol.QuotaDemand{CPUMilli: 200, MemoryMiB: 256, Replicas: 1}); err != nil {
			t.Fatal(err)
		}
		return resource, order
	}
	aResource, _ := seed(tenantA)
	bResource, bOrder := seed(tenantB)
	foreignWorkflow := &domain.CloudWorkflow{ResourceID: bResource.ID, OrderID: bOrder.ID, TenantID: tenantB, Kind: domain.CloudWorkflowKindProvision, Status: domain.CloudWorkflowPending, Spec: "{}"}
	if err := db.Create(foreignWorkflow).Error; err != nil {
		t.Fatal(err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": "http-tenant-ci", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()
	auth := ControlPlaneAuthConfig{
		Enabled: true, Mode: "oidc", OIDCRBACEnabled: true, CloudTenantAuthzEnabled: true,
		OIDCIssuer: "https://issuer.example", OIDCAudience: "taskflow-http", OIDCJWKSURL: jwks.URL,
		OIDCGroupsClaim: "groups", OIDCRoleMappings: "operators=control-plane-operator",
	}
	aToken := signPhase41JWT(t, key, "http-tenant-ci", map[string]any{
		"iss": auth.OIDCIssuer, "aud": auth.OIDCAudience, "sub": "alice", "tenant_id": tenantA,
		"groups": []string{"operators"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	bToken := signPhase41JWT(t, key, "http-tenant-ci", map[string]any{
		"iss": auth.OIDCIssuer, "aud": auth.OIDCAudience, "sub": "bob", "tenant_id": tenantB,
		"groups": []string{"operators"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	resources := service.NewCloudResourceService(store, noOpCloudTenantProvider{})
	workflows := service.NewCloudWorkflowService(store, nil)
	RegisterCloudControlRoutes(r, resources, auth, nil, ControlPlaneAuditConfig{})
	RegisterCloudWorkflowRoutes(r, workflows, auth, nil, ControlPlaneAuditConfig{})
	call := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+aToken)
		req.Header.Set("X-Tenant-ID", tenantB) // spoofed header cannot grant access
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		return res
	}
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", fmt.Sprintf("/api/v1/cloud/resources/%d", aResource.ID), "", 200},
		{"GET", fmt.Sprintf("/api/v1/cloud/resources/%d", bResource.ID), "", 404},
		{"GET", fmt.Sprintf("/api/v1/cloud/resources/%d/orders", bResource.ID), "", 404},
		{"GET", fmt.Sprintf("/api/v1/cloud/workflows/%d", foreignWorkflow.ID), "", 404},
		{"GET", fmt.Sprintf("/api/v1/cloud/resources/%d/workflows", bResource.ID), "", 404},
		{"GET", "/api/v1/cloud/resources?tenant_id=" + tenantB, "", 403},
		{"GET", "/api/v1/cloud/tenants/" + tenantB + "/quota", "", 403},
		{"PUT", "/api/v1/cloud/tenants/" + tenantB + "/quota", `{"replica_limit":1}`, 403},
		{"POST", fmt.Sprintf("/api/v1/cloud/resources/%d/resize", bResource.ID), `{"spec":{}}`, 404},
		{"DELETE", fmt.Sprintf("/api/v1/cloud/resources/%d", bResource.ID), "", 404},
		{"POST", fmt.Sprintf("/api/v1/cloud/resources/%d/reconcile", bResource.ID), "", 404},
		{"POST", fmt.Sprintf("/api/v1/cloud/workflows/%d/reconcile", foreignWorkflow.ID), "", 404},
		{"POST", fmt.Sprintf("/api/v1/cloud/resources/%d/workflows/delete", bResource.ID), "", 404},
		{"POST", "/api/v1/cloud/workflows/provision", `{"tenant_id":"` + tenantB + `","name":"cross","spec":{}}`, 403},
		{"POST", "/api/v1/cloud/resources", `{"tenant_id":"` + tenantB + `","name":"cross"}`, 403},
	} {
		res := call(tc.method, tc.path, tc.body)
		if res.Code != tc.status {
			t.Fatalf("%s %s: got %d want %d body=%s", tc.method, tc.path, res.Code, tc.status, res.Body.String())
		}
	}
	list := call(http.MethodGet, "/api/v1/cloud/resources", "")
	if list.Code != 200 || !strings.Contains(list.Body.String(), tenantA) || strings.Contains(list.Body.String(), tenantB) {
		t.Fatalf("unfiltered list escaped tenant grants: status=%d body=%s", list.Code, list.Body.String())
	}
	// A separate Go API client accesses the actual signed-OIDC/Gin/MySQL
	// endpoint rather than handcrafting all calls inside the handler package.
	server := httptest.NewServer(r)
	defer server.Close()
	client, err := cloudclient.New(server.URL, func(context.Context) (string, error) {
		return aToken, nil
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetResource(context.Background(), aResource.ID)
	if err != nil || got.TenantID != tenantA {
		t.Fatalf("external client own resource: %+v, %v", got, err)
	}
	if _, err := client.GetResource(context.Background(), bResource.ID); err == nil {
		t.Fatal("external client was allowed to read tenant B resource")
	}
	clientResources, err := client.ListResources(context.Background(), tenantA, 20, 0)
	if err != nil || len(clientResources) != 1 || clientResources[0].ID != aResource.ID {
		t.Fatalf("external client tenant list: %+v, %v", clientResources, err)
	}
	clientB, err := cloudclient.New(server.URL, func(context.Context) (string, error) {
		return bToken, nil
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	sharedKey := "same-request-across-tenants-" + suffix
	create := func(c *cloudclient.Client, tenant string) *cloudclient.Mutation {
		t.Helper()
		mutation, err := c.CreateResource(context.Background(), cloudclient.CreateResource{
			TenantID: tenant, Name: "shared-key-" + suffix,
			Spec: map[string]any{"image": "nginx:1.27", "replicas": 1, "cpu": "200m", "memory": "256Mi"},
		}, sharedKey)
		if err != nil {
			t.Fatalf("create resource for %s: %v", tenant, err)
		}
		return mutation
	}
	newA, newB := create(client, tenantA), create(clientB, tenantB)
	if newA.Resource.ID == newB.Resource.ID || newA.Resource.TenantID != tenantA || newB.Resource.TenantID != tenantB {
		t.Fatalf("cross-tenant idempotency collision: a=%+v b=%+v", newA, newB)
	}
	if retry := create(client, tenantA); retry.Resource.ID != newA.Resource.ID || retry.Order.ID != newA.Order.ID {
		t.Fatalf("idempotency replay allocated a second tenant A resource: %+v", retry)
	}
	// One owned mutation succeeds through this same external client path.
	mutation, err := client.DeleteResource(context.Background(), aResource.ID, "sdk-delete-"+suffix)
	if err != nil || mutation.Resource.TenantID != tenantA {
		t.Fatalf("external client own deletion failed: %+v err=%v", mutation, err)
	}
	if got, err := store.GetResource(context.Background(), bResource.ID); err != nil || got.Generation != 1 {
		t.Fatalf("foreign resource was mutated: %+v err=%v", got, err)
	}
}
