package cloudcontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func (p *KubernetesProvider) EnsureNetwork(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	name := SagaNetworkResourceName(resource.Name)
	body := kubernetesServiceApply(resource, bundle, name)
	status, response, err := p.applyPatch(ctx, p.serviceURL(bundle.Compute.Namespace, name), body)
	if err != nil {
		return SagaStepResult{}, err
	}
	if status < 200 || status >= 300 {
		return SagaStepResult{}, kubernetesStatusError(status, response)
	}
	uid := metadataUID(response)
	output, _ := json.Marshal(map[string]any{"kind": "Service", "name": name, "namespace": bundle.Compute.Namespace, "uid": uid})
	return SagaStepResult{Ready: true, ExternalID: uid, Output: string(output)}, nil
}

func (p *KubernetesProvider) DeleteNetwork(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	name := SagaNetworkResourceName(resource.Name)
	ready, err := p.deleteObjectAndConfirm(ctx, p.serviceURL(bundle.Compute.Namespace, name))
	return SagaStepResult{Ready: ready}, err
}

func (p *KubernetesProvider) EnsureStorage(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	name := SagaStorageResourceName(resource.Name)
	body := kubernetesPVCApply(resource, bundle, name)
	status, response, err := p.applyPatch(ctx, p.pvcURL(bundle.Compute.Namespace, name), body)
	if err != nil {
		return SagaStepResult{}, err
	}
	if status < 200 || status >= 300 {
		return SagaStepResult{}, kubernetesStatusError(status, response)
	}
	uid := metadataUID(response)
	output, _ := json.Marshal(map[string]any{"kind": "PersistentVolumeClaim", "name": name, "namespace": bundle.Compute.Namespace, "uid": uid})
	return SagaStepResult{Ready: true, ExternalID: uid, Output: string(output)}, nil
}

func (p *KubernetesProvider) DeleteStorage(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	name := SagaStorageResourceName(resource.Name)
	ready, err := p.deleteObjectAndConfirm(ctx, p.pvcURL(bundle.Compute.Namespace, name))
	return SagaStepResult{Ready: ready}, err
}

func (p *KubernetesProvider) EnsureCompute(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	body := kubernetesBundleDeploymentApply(resource, bundle)
	status, response, err := p.applyPatch(ctx, p.deploymentURL(bundle.Compute.Namespace, resource.Name), body)
	if err != nil {
		return SagaStepResult{}, err
	}
	if status < 200 || status >= 300 {
		return SagaStepResult{}, kubernetesStatusError(status, response)
	}
	snapshot, err := p.Observe(ctx, resource)
	if err != nil {
		return SagaStepResult{}, err
	}
	return SagaStepResult{
		Ready:      snapshot.State == domain.CloudResourceActive,
		ExternalID: snapshot.ProviderResourceID,
		Output:     snapshot.Spec,
	}, nil
}

func (p *KubernetesProvider) DeleteCompute(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	ready, err := p.deleteObjectAndConfirm(ctx, p.deploymentURL(bundle.Compute.Namespace, resource.Name))
	return SagaStepResult{Ready: ready}, err
}

func (p *KubernetesProvider) applyPatch(ctx context.Context, endpoint string, body []byte) (int, []byte, error) {
	query := url.Values{}
	query.Set("fieldManager", "taskflow-cloud-saga")
	query.Set("force", "true")
	return p.request(ctx, http.MethodPatch, endpoint+"?"+query.Encode(), body, "application/apply-patch+yaml")
}

func (p *KubernetesProvider) deleteObjectAndConfirm(ctx context.Context, endpoint string) (bool, error) {
	status, response, err := p.request(ctx, http.MethodDelete, endpoint, nil, "application/json")
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound || status == http.StatusGone {
		return true, nil
	}
	if status < 200 || status >= 300 {
		return false, kubernetesStatusError(status, response)
	}
	status, response, err = p.request(ctx, http.MethodGet, endpoint, nil, "application/json")
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound || status == http.StatusGone {
		return true, nil
	}
	if status < 200 || status >= 300 {
		return false, kubernetesStatusError(status, response)
	}
	return false, nil
}

func (p *KubernetesProvider) serviceURL(namespace, name string) string {
	return p.apiURL + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/services/" + url.PathEscape(name)
}

func (p *KubernetesProvider) pvcURL(namespace, name string) string {
	return p.apiURL + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/persistentvolumeclaims/" + url.PathEscape(name)
}

func metadataUID(body []byte) string {
	var object struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	_ = json.Unmarshal(body, &object)
	return object.Metadata.UID
}

func kubernetesServiceApply(resource domain.CloudResource, bundle KubernetesBundleSpec, name string) []byte {
	labels := sagaLabels(resource)
	object := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
			"namespace": bundle.Compute.Namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": map[string]string{"app.kubernetes.io/name": resource.Name},
			"ports": []map[string]any{{
				"name":       "http",
				"port":       bundle.Network.Port,
				"targetPort": bundle.Network.TargetPort,
			}},
		},
	}
	body, _ := json.Marshal(object)
	return body
}

func kubernetesPVCApply(resource domain.CloudResource, bundle KubernetesBundleSpec, name string) []byte {
	spec := map[string]any{
		"accessModes": []string{"ReadWriteOnce"},
		"resources": map[string]any{
			"requests": map[string]string{"storage": bundle.Storage.Size},
		},
	}
	if strings.TrimSpace(bundle.Storage.StorageClassName) != "" {
		spec["storageClassName"] = bundle.Storage.StorageClassName
	}
	object := map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":      name,
			"namespace": bundle.Compute.Namespace,
			"labels":    sagaLabels(resource),
		},
		"spec": spec,
	}
	body, _ := json.Marshal(object)
	return body
}

func kubernetesBundleDeploymentApply(resource domain.CloudResource, bundle KubernetesBundleSpec) []byte {
	container := map[string]any{
		"name":  "workload",
		"image": bundle.Compute.Image,
	}
	if bundle.Compute.ContainerPort > 0 {
		container["ports"] = []map[string]any{{"containerPort": bundle.Compute.ContainerPort}}
	}
	requests := map[string]string{}
	if strings.TrimSpace(bundle.Compute.CPU) != "" {
		requests["cpu"] = bundle.Compute.CPU
	}
	if strings.TrimSpace(bundle.Compute.Memory) != "" {
		requests["memory"] = bundle.Compute.Memory
	}
	if len(requests) > 0 {
		container["resources"] = map[string]any{"requests": requests, "limits": requests}
	}
	podSpec := map[string]any{"containers": []map[string]any{container}}
	if bundle.Storage.Enabled {
		claimName := SagaStorageResourceName(resource.Name)
		container["volumeMounts"] = []map[string]any{{"name": "data", "mountPath": bundle.Storage.MountPath}}
		podSpec["volumes"] = []map[string]any{{
			"name":                  "data",
			"persistentVolumeClaim": map[string]any{"claimName": claimName},
		}}
	}
	labels := sagaLabels(resource)
	object := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      resource.Name,
			"namespace": bundle.Compute.Namespace,
			"labels":    labels,
			"annotations": map[string]string{
				"taskflow.io/resource-id":  strconv.FormatUint(resource.ID, 10),
				"taskflow.io/tenant-id":    resource.TenantID,
				"taskflow.io/generation":   strconv.FormatUint(resource.Generation, 10),
				"taskflow.io/orchestrator": "saga",
			},
		},
		"spec": map[string]any{
			"replicas": bundle.Compute.Replicas,
			"selector": map[string]any{"matchLabels": map[string]string{"app.kubernetes.io/name": resource.Name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": labels},
				"spec":     podSpec,
			},
		},
	}
	body, _ := json.Marshal(object)
	return body
}

func sagaLabels(resource domain.CloudResource) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       resource.Name,
		"app.kubernetes.io/managed-by": "taskflow-cloud-saga",
		"taskflow.io/resource-id":      strconv.FormatUint(resource.ID, 10),
	}
}

var _ SagaResourceProvider = (*KubernetesProvider)(nil)
