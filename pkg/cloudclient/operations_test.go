package cloudclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResourceBusinessOrderLifecycle(t *testing.T) {
	const key = "business-order-20261009-001"
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer valid" {
			t.Errorf("authorization not forwarded")
		}
		paths = append(paths, r.Method+" "+r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v1/cloud/workflows/provision":
			if r.Header.Get("Idempotency-Key") != key {
				t.Error("provision idempotency key lost")
			}
			var in ProvisionWorkflow
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Fatal(err)
			}
			if in.TenantID != "team-a" || in.Name != "order-a" || in.Provider != "kubernetes" {
				t.Errorf("bad provision payload: %+v", in)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"workflow":{"id":100,"status":"running"},"resource":{"id":2,"tenant_id":"team-a"},"order":{"id":7}}`))
		case "/api/v1/cloud/resources/2/workflows/resize", "/api/v1/cloud/resources/2/workflows/delete":
			if r.Header.Get("Idempotency-Key") != key {
				t.Error("mutation idempotency key lost")
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"workflow":{"id":101,"status":"running"},"resource":{"id":2,"tenant_id":"team-a"}}`))
		case "/api/v1/cloud/resources/2/orders":
			if r.URL.Query().Get("limit") != "20" || r.URL.Query().Get("offset") != "3" {
				t.Error("pagination mismatch")
			}
			_, _ = w.Write([]byte(`{"items":[{"id":7,"resource_id":2,"tenant_id":"team-a","status":"pending"}]}`))
		case "/api/v1/cloud/resources/2/workflows":
			_, _ = w.Write([]byte(`{"items":[{"id":100,"status":"running"}]}`))
		case "/api/v1/cloud/tenants/team-a/quota":
			if r.Method == http.MethodPut {
				if r.Header.Get("Idempotency-Key") != key {
					t.Error("quota update idempotency header missing")
				}
				var q QuotaLimits
				if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
					t.Fatal(err)
				}
				if q.ReplicaLimit != 5 {
					t.Errorf("quota payload %+v", q)
				}
			}
			_, _ = w.Write([]byte(`{"tenant_id":"team-a","cpu_milli_limit":4000,"replica_limit":5}`))
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, func(context.Context) (string, error) { return "valid", nil }, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	q, err := client.GetQuota(ctx, "team-a")
	if err != nil || q.ReplicaLimit != 5 {
		t.Fatalf("quota get: %+v %v", q, err)
	}
	q, err = client.SetQuota(ctx, "team-a", QuotaLimits{CPUMilliLimit: 4000, ReplicaLimit: 5}, key)
	if err != nil || q.TenantID != "team-a" {
		t.Fatalf("quota set: %+v %v", q, err)
	}
	spec := json.RawMessage(`{"compute":{"namespace":"ops","image":"nginx:1.27","replicas":1},"network":{"enabled":false}}`)
	p, err := client.Provision(ctx, ProvisionWorkflow{TenantID: "team-a", Name: "order-a", Spec: spec}, key)
	if err != nil || p.Workflow.ID != 100 {
		t.Fatalf("provision: %+v %v", p, err)
	}
	res, err := client.ResizeWorkflow(ctx, 2, spec, key)
	if err != nil || res.Workflow.ID != 101 {
		t.Fatalf("resize: %+v %v", res, err)
	}
	del, err := client.DeleteWorkflow(ctx, 2, key)
	if err != nil || del.Workflow.ID != 101 {
		t.Fatalf("delete: %+v %v", del, err)
	}
	orders, err := client.ListOrders(ctx, 2, 20, 3)
	if err != nil || len(orders) != 1 || orders[0].ID != 7 {
		t.Fatalf("orders %+v %v", orders, err)
	}
	workflows, err := client.ListWorkflows(ctx, 2, 20, 3)
	if err != nil || len(workflows) != 1 || workflows[0].ID != 100 {
		t.Fatalf("workflows %+v %v", workflows, err)
	}
	if len(paths) != 7 {
		t.Errorf("request count=%d paths=%v", len(paths), paths)
	}
}

func TestOperationValidationPreventsUnintendedRequests(t *testing.T) {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { count++; w.WriteHeader(200) }))
	defer srv.Close()
	c, err := New(srv.URL, func(context.Context) (string, error) { return "t", nil }, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tenant := range []string{"", " team-a", "../admin", "team/a", "%2e%2e", "team?a"} {
		if _, err := c.GetQuota(ctx, tenant); err == nil {
			t.Errorf("unsafe tenant %q accepted", tenant)
		}
	}
	if _, err := c.SetQuota(ctx, "team-a", QuotaLimits{ReplicaLimit: -1}, "id"); err == nil {
		t.Error("negative quota accepted")
	}
	if _, err := c.DeleteWorkflow(ctx, 0, "id"); err == nil {
		t.Error("zero resource ID accepted")
	}
	if _, err := c.Provision(ctx, ProvisionWorkflow{TenantID: "team-a", Name: "x", Spec: json.RawMessage(`invalid`)}, "id"); err == nil {
		t.Error("invalid spec accepted")
	}
	if _, err := c.ListOrders(ctx, 5, 101, 0); err == nil {
		t.Error("oversized page accepted")
	}
	if _, err := c.ResizeWorkflow(ctx, 5, json.RawMessage(`{"ok":true}`), ""); err == nil || !strings.Contains(err.Error(), "idempotency") {
		t.Errorf("expected required idempotency key, got %v", err)
	}
	if count != 0 {
		t.Errorf("unsafe requests reached network: %d", count)
	}
}
