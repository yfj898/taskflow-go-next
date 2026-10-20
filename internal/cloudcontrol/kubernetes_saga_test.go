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

func TestKubernetesSagaProviderLifecycle(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	seen := map[string]int{}
	deleted := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Method+" "+r.URL.Path]++
		isDeleted := deleted[r.URL.Path]
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer saga-token" {
			t.Fatalf("unexpected authorization header: %q", r.Header.Get("Authorization"))
		}
		if r.Method == http.MethodPatch && r.URL.Query().Get("fieldManager") != "taskflow-cloud-saga" {
			t.Fatalf("unexpected field manager: %q", r.URL.Query().Get("fieldManager"))
		}

		switch r.Method + " " + r.URL.Path {
		case "PATCH /api/v1/namespaces/team-a/services/demo-svc":
			_, _ = w.Write([]byte(`{"metadata":{"uid":"svc-uid"}}`))
		case "PATCH /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data":
			_, _ = w.Write([]byte(`{"metadata":{"uid":"pvc-uid"}}`))
		case "PATCH /apis/apps/v1/namespaces/team-a/deployments/demo":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			var deployment map[string]any
			if err := json.Unmarshal(body, &deployment); err != nil {
				t.Fatalf("decode deployment apply: %v", err)
			}
			spec := deployment["spec"].(map[string]any)
			template := spec["template"].(map[string]any)
			podSpec := template["spec"].(map[string]any)
			if _, ok := podSpec["volumes"]; !ok {
				t.Fatal("saga compute apply did not mount the storage PVC")
			}
			_, _ = w.Write([]byte(`{"metadata":{"uid":"deploy-uid"}}`))
		case "GET /apis/apps/v1/namespaces/team-a/deployments/demo":
			if isDeleted {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"metadata":{"uid":"deploy-uid","generation":4},"spec":{"replicas":2},"status":{"observedGeneration":4,"replicas":2,"readyReplicas":2,"availableReplicas":2}}`))
		case "GET /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data",
			"GET /api/v1/namespaces/team-a/services/demo-svc":
			if isDeleted {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"metadata":{"uid":"still-present"}}`))
		case "DELETE /apis/apps/v1/namespaces/team-a/deployments/demo",
			"DELETE /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data",
			"DELETE /api/v1/namespaces/team-a/services/demo-svc":
			mu.Lock()
			deleted[r.URL.Path] = true
			mu.Unlock()
			_, _ = w.Write([]byte(`{"kind":"Status","status":"Success"}`))
		default:
			t.Fatalf("unexpected kubernetes request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	provider, err := NewKubernetesProvider(server.URL, "saga-token", "default", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := ParseKubernetesBundleSpec(`{
      "compute":{"namespace":"team-a","image":"nginx:1.27","replicas":2,"cpu":"500m","memory":"256Mi","container_port":8080},
      "network":{"enabled":true,"port":80,"target_port":8080},
      "storage":{"enabled":true,"size":"1Gi","mount_path":"/data"}
    }`, "default")
	if err != nil {
		t.Fatal(err)
	}
	computeSpec, _ := KubernetesBundleComputeSpecJSON(bundle)
	resource := domain.CloudResource{
		ID:                99,
		TenantID:          "team-a",
		Name:              "demo",
		DesiredSpec:       computeSpec,
		DesiredState:      domain.CloudResourceDesiredActive,
		Generation:        1,
		OrchestrationMode: domain.CloudOrchestrationSaga,
	}

	network, err := provider.EnsureNetwork(context.Background(), resource, bundle)
	if err != nil || !network.Ready || network.ExternalID != "svc-uid" || !strings.Contains(network.Output, "demo-svc") {
		t.Fatalf("unexpected network result=%+v err=%v", network, err)
	}
	storage, err := provider.EnsureStorage(context.Background(), resource, bundle)
	if err != nil || !storage.Ready || storage.ExternalID != "pvc-uid" || !strings.Contains(storage.Output, "demo-data") {
		t.Fatalf("unexpected storage result=%+v err=%v", storage, err)
	}
	compute, err := provider.EnsureCompute(context.Background(), resource, bundle)
	if err != nil || !compute.Ready || compute.ExternalID != "deploy-uid" {
		t.Fatalf("unexpected compute result=%+v err=%v", compute, err)
	}
	deletedCompute, err := provider.DeleteCompute(context.Background(), resource, bundle)
	if err != nil || !deletedCompute.Ready {
		t.Fatalf("delete compute result=%+v err=%v", deletedCompute, err)
	}
	deletedStorage, err := provider.DeleteStorage(context.Background(), resource, bundle)
	if err != nil || !deletedStorage.Ready {
		t.Fatalf("delete storage result=%+v err=%v", deletedStorage, err)
	}
	deletedNetwork, err := provider.DeleteNetwork(context.Background(), resource, bundle)
	if err != nil || !deletedNetwork.Ready {
		t.Fatalf("delete network result=%+v err=%v", deletedNetwork, err)
	}

	for _, key := range []string{
		"PATCH /api/v1/namespaces/team-a/services/demo-svc",
		"PATCH /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data",
		"PATCH /apis/apps/v1/namespaces/team-a/deployments/demo",
		"DELETE /apis/apps/v1/namespaces/team-a/deployments/demo",
		"DELETE /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data",
		"DELETE /api/v1/namespaces/team-a/services/demo-svc",
		"GET /api/v1/namespaces/team-a/persistentvolumeclaims/demo-data",
		"GET /api/v1/namespaces/team-a/services/demo-svc",
	} {
		if seen[key] != 1 {
			t.Fatalf("request %q count=%d want=1", key, seen[key])
		}
	}
	if seen["GET /apis/apps/v1/namespaces/team-a/deployments/demo"] != 2 {
		t.Fatalf("deployment GET count=%d want=2", seen["GET /apis/apps/v1/namespaces/team-a/deployments/demo"])
	}
}
