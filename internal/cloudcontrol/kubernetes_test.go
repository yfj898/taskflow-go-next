package cloudcontrol

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func TestKubernetesProviderApplyObserveDelete(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	deleted := false
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("unexpected authorization header: %q", r.Header.Get("Authorization"))
		}
		if r.URL.Path != "/apis/apps/v1/namespaces/team-a/deployments/demo" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPatch:
			patches++
			if got := r.URL.Query().Get("fieldManager"); got != "taskflow-cloud-control" {
				t.Fatalf("unexpected fieldManager: %q", got)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			var object map[string]any
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatalf("decode apply body: %v", err)
			}
			metadata := object["metadata"].(map[string]any)
			annotations := metadata["annotations"].(map[string]any)
			if annotations["taskflow.io/generation"] != "3" {
				t.Fatalf("generation annotation=%v", annotations["taskflow.io/generation"])
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"uid":"uid-1"}}`))
		case http.MethodGet:
			if deleted {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"uid":"uid-1","generation":7},"spec":{"replicas":3},"status":{"observedGeneration":7,"replicas":3,"readyReplicas":3,"availableReplicas":3}}`))
		case http.MethodDelete:
			deleted = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"Status","status":"Success"}`))
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	}))
	defer server.Close()

	provider, err := NewKubernetesProvider(server.URL, "test-token", "default", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	resource := domain.CloudResource{
		ID:           42,
		TenantID:     "tenant-a",
		Name:         "demo",
		Generation:   3,
		DesiredState: domain.CloudResourceDesiredActive,
		DesiredSpec:  `{"namespace":"team-a","image":"nginx:1.27","replicas":3,"cpu":"500m","memory":"256Mi","container_port":8080}`,
	}

	snapshot, err := provider.Apply(context.Background(), resource)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if snapshot.State != domain.CloudResourceActive || snapshot.ProviderResourceID != "uid-1" {
		t.Fatalf("unexpected apply snapshot: %+v", snapshot)
	}
	if !strings.Contains(snapshot.Spec, `"available_replicas":3`) {
		t.Fatalf("unexpected actual spec: %s", snapshot.Spec)
	}

	deleteSnapshot, err := provider.Delete(context.Background(), resource)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleteSnapshot.State != domain.CloudResourceDeleting {
		t.Fatalf("unexpected delete state: %s", deleteSnapshot.State)
	}
	observed, err := provider.Observe(context.Background(), resource)
	if err != nil {
		t.Fatalf("observe deleted: %v", err)
	}
	if observed.State != domain.CloudResourceDeleted {
		t.Fatalf("unexpected observed deleted state: %s", observed.State)
	}
	if patches != 1 {
		t.Fatalf("patch count=%d want=1", patches)
	}
}

func TestParseKubernetesDeploymentSpecValidation(t *testing.T) {
	t.Parallel()

	spec, err := ParseKubernetesDeploymentSpec(`{"image":"busybox:1.36","replicas":0}`, "platform")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Namespace != "platform" || spec.Replicas != 0 {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if _, err := ParseKubernetesDeploymentSpec(`{"image":"","replicas":1}`, "default"); err == nil {
		t.Fatal("expected empty image to fail validation")
	}
	if _, err := ParseKubernetesDeploymentSpec(`{"image":"busybox","replicas":1,"unknown":true}`, "default"); err == nil {
		t.Fatal("expected unknown field to fail validation")
	}
}
