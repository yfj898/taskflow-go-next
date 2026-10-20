package cloudclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientCredentialsIdempotencyAndQuery(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer valid-token" {
			t.Errorf("incorrect auth header: %q", got)
		}
		switch r.URL.Path {
		case "/api/v1/cloud/resources":
			if r.Method == http.MethodGet {
				if r.URL.Query().Get("tenant_id") != "team + a" || r.URL.Query().Get("limit") != "30" {
					t.Errorf("query was not encoded correctly: %s", r.URL.RawQuery)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"id": 4, "tenant_id": "team + a"}}})
				return
			}
			if got := r.Header.Get("Idempotency-Key"); got != "same-request" {
				t.Errorf("missing explicit idempotency key %q", got)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"resource": map[string]any{"id": 5, "tenant_id": "team + a"}, "order": map[string]any{"id": 8}})
		case "/api/v1/cloud/resources/5":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 5, "tenant_id": "team + a"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	client, err := New(srv.URL, func(context.Context) (string, error) { return "valid-token", nil }, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateResource(context.Background(), CreateResource{TenantID: "team + a", Name: "app"}, ""); err == nil {
		t.Fatal("mutation without idempotency key must be rejected locally")
	}
	result, err := client.CreateResource(context.Background(), CreateResource{TenantID: "team + a", Name: "app"}, "same-request")
	if err != nil || result.Resource.ID != 5 || result.Order.ID != 8 {
		t.Fatalf("create: result=%+v err=%v", result, err)
	}
	resource, err := client.GetResource(context.Background(), 5)
	if err != nil || resource.ID != 5 {
		t.Fatalf("get resource: %+v err=%v", resource, err)
	}
	items, err := client.ListResources(context.Background(), "team + a", 30, 0)
	if err != nil || len(items) != 1 || items[0].ID != 4 {
		t.Fatalf("list: %+v err=%v", items, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("unexpected request/retry count: %d", calls.Load())
	}
}

func TestClientStatusAndCancelledRequests(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Request-ID", "request-test-01")
		w.WriteHeader(http.StatusConflict)
		_, _ = fmt.Fprint(w, `{"error":"quota exceeded"}`)
	}))
	defer srv.Close()
	client, err := New(srv.URL, func(context.Context) (string, error) { return "jwt", nil }, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteResource(context.Background(), 99, "stable-key")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 409 || apiErr.RequestID != "request-test-01" || apiErr.Message != "quota exceeded" {
		t.Fatalf("expected structured HTTP error; got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("mutation was unexpectedly retried %d times", calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.GetResource(ctx, 99); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context was ignored: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled request reached network: %d", calls.Load())
	}
	for _, bad := range []string{"", "localhost:9090", "ftp://host", "https://user:password@host", "https://host/?query=1"} {
		if _, err := New(bad, func(context.Context) (string, error) { return "token", nil }, nil); err == nil {
			t.Fatalf("invalid endpoint accepted: %q", bad)
		}
	}
}

func TestClientWaitWorkflow(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := calls.Add(1)
		status := "running"
		if count >= 3 {
			status = "succeeded"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow": map[string]any{"id": 91, "status": status}})
	}))
	defer srv.Close()
	client, err := New(srv.URL, func(context.Context) (string, error) { return "jwt", nil }, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := client.WaitWorkflow(ctx, 91, time.Millisecond)
	if err != nil || result.Workflow.Status != "succeeded" || calls.Load() != 3 {
		t.Fatalf("workflow wait: result=%+v err=%v calls=%d", result, err, calls.Load())
	}
	ctx2, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	if _, err := client.WaitWorkflow(ctx2, 91, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expired wait reached network: %d", calls.Load())
	}
}
