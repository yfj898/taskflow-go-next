package cloudcontrol

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

const (
	defaultServiceAccountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	defaultServiceAccountCA    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type KubernetesDeploymentSpec struct {
	Namespace     string `json:"namespace,omitempty"`
	Image         string `json:"image"`
	Replicas      int32  `json:"replicas"`
	CPU           string `json:"cpu,omitempty"`
	Memory        string `json:"memory,omitempty"`
	ContainerPort int32  `json:"container_port,omitempty"`
}

type KubernetesProvider struct {
	apiURL           string
	token            string
	defaultNamespace string
	client           *http.Client
}

func NewKubernetesProvider(apiURL, token, defaultNamespace string, client *http.Client) (*KubernetesProvider, error) {
	apiURL = strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if apiURL == "" {
		return nil, errors.New("kubernetes api url is required")
	}
	if _, err := url.ParseRequestURI(apiURL); err != nil {
		return nil, fmt.Errorf("invalid kubernetes api url: %w", err)
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if strings.TrimSpace(defaultNamespace) == "" {
		defaultNamespace = "default"
	}
	return &KubernetesProvider{
		apiURL:           apiURL,
		token:            strings.TrimSpace(token),
		defaultNamespace: defaultNamespace,
		client:           client,
	}, nil
}

// NewKubernetesProviderFromEnvironment builds an in-cluster or explicitly
// configured Kubernetes provider. It is opt-in so existing TaskFlow setups do
// not require Kubernetes credentials.
func NewKubernetesProviderFromEnvironment() (*KubernetesProvider, bool, error) {
	enabled, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv("CLOUD_CONTROL_ENABLED")))
	if !enabled {
		return nil, false, nil
	}

	apiURL := strings.TrimSpace(os.Getenv("CLOUD_CONTROL_KUBE_API_URL"))
	if apiURL == "" {
		host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
		port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
		if host == "" {
			return nil, false, errors.New("CLOUD_CONTROL_ENABLED requires CLOUD_CONTROL_KUBE_API_URL or in-cluster Kubernetes service env")
		}
		if port == "" {
			port = "443"
		}
		apiURL = "https://" + host + ":" + port
	}

	token := strings.TrimSpace(os.Getenv("CLOUD_CONTROL_KUBE_TOKEN"))
	if token == "" {
		data, err := os.ReadFile(defaultServiceAccountToken)
		if err != nil {
			return nil, false, fmt.Errorf("read kubernetes service account token: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.HasPrefix(apiURL, "https://") {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if caPath := strings.TrimSpace(os.Getenv("CLOUD_CONTROL_KUBE_CA_FILE")); caPath != "" {
			if err := appendCA(pool, caPath); err != nil {
				return nil, false, err
			}
		} else if _, err := os.Stat(defaultServiceAccountCA); err == nil {
			if err := appendCA(pool, defaultServiceAccountCA); err != nil {
				return nil, false, err
			}
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	provider, err := NewKubernetesProvider(
		apiURL,
		token,
		strings.TrimSpace(os.Getenv("CLOUD_CONTROL_KUBE_NAMESPACE")),
		client,
	)
	return provider, err == nil, err
}

func appendCA(pool *x509.CertPool, path string) error {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read kubernetes ca file: %w", err)
	}
	if ok := pool.AppendCertsFromPEM(pemData); !ok {
		return errors.New("kubernetes ca file contains no certificates")
	}
	return nil
}

func ParseKubernetesDeploymentSpec(raw, defaultNamespace string) (KubernetesDeploymentSpec, error) {
	var spec KubernetesDeploymentSpec
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return spec, fmt.Errorf("invalid kubernetes deployment spec: %w", err)
	}
	if strings.TrimSpace(spec.Namespace) == "" {
		spec.Namespace = defaultNamespace
	}
	if strings.TrimSpace(spec.Image) == "" {
		return spec, errors.New("deployment image is required")
	}
	if spec.Replicas < 0 {
		return spec, errors.New("deployment replicas must be >= 0")
	}
	if spec.ContainerPort < 0 || spec.ContainerPort > 65535 {
		return spec, errors.New("container_port must be between 0 and 65535")
	}
	return spec, nil
}

func (p *KubernetesProvider) Apply(ctx context.Context, resource domain.CloudResource) (ProviderSnapshot, error) {
	spec, err := ParseKubernetesDeploymentSpec(resource.DesiredSpec, p.defaultNamespace)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	body := kubernetesDeploymentApply(resource, spec)
	path := p.deploymentURL(spec.Namespace, resource.Name)
	query := url.Values{}
	query.Set("fieldManager", "taskflow-cloud-control")
	query.Set("force", "true")
	path += "?" + query.Encode()
	status, response, err := p.request(ctx, http.MethodPatch, path, body, "application/apply-patch+yaml")
	if err != nil {
		return ProviderSnapshot{}, err
	}
	if status < 200 || status >= 300 {
		return ProviderSnapshot{}, kubernetesStatusError(status, response)
	}
	return p.Observe(ctx, resource)
}

func (p *KubernetesProvider) Delete(ctx context.Context, resource domain.CloudResource) (ProviderSnapshot, error) {
	spec, err := ParseKubernetesDeploymentSpec(resource.DesiredSpec, p.defaultNamespace)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	status, response, err := p.request(ctx, http.MethodDelete, p.deploymentURL(spec.Namespace, resource.Name), nil, "application/json")
	if err != nil {
		return ProviderSnapshot{}, err
	}
	if status == http.StatusNotFound {
		return ProviderSnapshot{State: domain.CloudResourceDeleted, Spec: `{}`}, nil
	}
	if status < 200 || status >= 300 {
		return ProviderSnapshot{}, kubernetesStatusError(status, response)
	}
	return ProviderSnapshot{ProviderResourceID: resource.ProviderResourceID, State: domain.CloudResourceDeleting, Spec: resource.ActualSpec}, nil
}

func (p *KubernetesProvider) Observe(ctx context.Context, resource domain.CloudResource) (ProviderSnapshot, error) {
	spec, err := ParseKubernetesDeploymentSpec(resource.DesiredSpec, p.defaultNamespace)
	if err != nil {
		return ProviderSnapshot{}, err
	}
	status, response, err := p.request(ctx, http.MethodGet, p.deploymentURL(spec.Namespace, resource.Name), nil, "application/json")
	if err != nil {
		return ProviderSnapshot{}, err
	}
	if status == http.StatusNotFound {
		return ProviderSnapshot{State: domain.CloudResourceDeleted, Spec: `{}`}, nil
	}
	if status < 200 || status >= 300 {
		return ProviderSnapshot{}, kubernetesStatusError(status, response)
	}

	var deployment struct {
		Metadata struct {
			UID        string `json:"uid"`
			Generation int64  `json:"generation"`
		} `json:"metadata"`
		Spec struct {
			Replicas int32 `json:"replicas"`
		} `json:"spec"`
		Status struct {
			ObservedGeneration int64 `json:"observedGeneration"`
			Replicas           int32 `json:"replicas"`
			AvailableReplicas  int32 `json:"availableReplicas"`
			ReadyReplicas      int32 `json:"readyReplicas"`
		} `json:"status"`
	}
	if err := json.Unmarshal(response, &deployment); err != nil {
		return ProviderSnapshot{}, fmt.Errorf("decode kubernetes deployment: %w", err)
	}
	actual := map[string]any{
		"namespace":           spec.Namespace,
		"replicas":            deployment.Status.Replicas,
		"ready_replicas":      deployment.Status.ReadyReplicas,
		"available_replicas":  deployment.Status.AvailableReplicas,
		"desired_replicas":    deployment.Spec.Replicas,
		"kube_generation":     deployment.Metadata.Generation,
		"observed_generation": deployment.Status.ObservedGeneration,
	}
	actualJSON, _ := json.Marshal(actual)
	state := domain.CloudResourceProvisioning
	if resource.DesiredState == domain.CloudResourceDesiredDeleted {
		state = domain.CloudResourceDeleting
	} else if deployment.Status.ObservedGeneration >= deployment.Metadata.Generation && deployment.Status.AvailableReplicas >= deployment.Spec.Replicas {
		state = domain.CloudResourceActive
	}
	return ProviderSnapshot{
		ProviderResourceID: deployment.Metadata.UID,
		State:              state,
		Spec:               string(actualJSON),
	}, nil
}

func kubernetesDeploymentApply(resource domain.CloudResource, spec KubernetesDeploymentSpec) []byte {
	container := map[string]any{
		"name":  "workload",
		"image": spec.Image,
	}
	if spec.ContainerPort > 0 {
		container["ports"] = []map[string]any{{"containerPort": spec.ContainerPort}}
	}
	requests := map[string]string{}
	if strings.TrimSpace(spec.CPU) != "" {
		requests["cpu"] = spec.CPU
	}
	if strings.TrimSpace(spec.Memory) != "" {
		requests["memory"] = spec.Memory
	}
	if len(requests) > 0 {
		container["resources"] = map[string]any{"requests": requests, "limits": requests}
	}
	labels := map[string]string{
		"app.kubernetes.io/name":       resource.Name,
		"app.kubernetes.io/managed-by": "taskflow-cloud-control",
	}
	object := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      resource.Name,
			"namespace": spec.Namespace,
			"labels":    labels,
			"annotations": map[string]string{
				"taskflow.io/resource-id": strconv.FormatUint(resource.ID, 10),
				"taskflow.io/tenant-id":   resource.TenantID,
				"taskflow.io/generation":  strconv.FormatUint(resource.Generation, 10),
			},
		},
		"spec": map[string]any{
			"replicas": spec.Replicas,
			"selector": map[string]any{"matchLabels": map[string]string{"app.kubernetes.io/name": resource.Name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": labels},
				"spec":     map[string]any{"containers": []map[string]any{container}},
			},
		},
	}
	body, _ := json.Marshal(object)
	return body
}

func (p *KubernetesProvider) deploymentURL(namespace, name string) string {
	return p.apiURL + "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/deployments/" + url.PathEscape(name)
}

func (p *KubernetesProvider) request(ctx context.Context, method, endpoint string, body []byte, contentType string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if p.token != "" {
		request.Header.Set("Authorization", "Bearer "+p.token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, data, nil
}

func kubernetesStatusError(status int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 512 {
		message = message[:512]
	}
	if message == "" {
		message = http.StatusText(status)
	}
	return fmt.Errorf("kubernetes api status=%d: %s", status, message)
}
