package cloudcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type KubernetesNetworkSpec struct {
	Enabled    bool  `json:"enabled"`
	Port       int32 `json:"port,omitempty"`
	TargetPort int32 `json:"target_port,omitempty"`
}

type KubernetesStorageSpec struct {
	Enabled          bool   `json:"enabled"`
	Size             string `json:"size,omitempty"`
	MountPath        string `json:"mount_path,omitempty"`
	StorageClassName string `json:"storage_class_name,omitempty"`
}

type KubernetesBundleSpec struct {
	Compute KubernetesDeploymentSpec `json:"compute"`
	Network KubernetesNetworkSpec    `json:"network,omitempty"`
	Storage KubernetesStorageSpec    `json:"storage,omitempty"`
}

type SagaStepResult struct {
	Ready      bool
	ExternalID string
	Output     string
}

type SagaResourceProvider interface {
	EnsureNetwork(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
	DeleteNetwork(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
	EnsureStorage(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
	DeleteStorage(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
	EnsureCompute(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
	DeleteCompute(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error)
}

func ParseKubernetesBundleSpec(raw, defaultNamespace string) (KubernetesBundleSpec, error) {
	var spec KubernetesBundleSpec
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return spec, fmt.Errorf("invalid kubernetes bundle spec: %w", err)
	}

	computeJSON, err := json.Marshal(spec.Compute)
	if err != nil {
		return spec, err
	}
	compute, err := ParseKubernetesDeploymentSpec(string(computeJSON), defaultNamespace)
	if err != nil {
		return spec, err
	}
	spec.Compute = compute

	if spec.Network.Enabled {
		if spec.Network.TargetPort == 0 {
			spec.Network.TargetPort = compute.ContainerPort
		}
		if spec.Network.Port == 0 {
			spec.Network.Port = spec.Network.TargetPort
		}
		if spec.Network.Port <= 0 || spec.Network.Port > 65535 || spec.Network.TargetPort <= 0 || spec.Network.TargetPort > 65535 {
			return spec, errors.New("network port and target_port must be between 1 and 65535")
		}
	}

	if spec.Storage.Enabled {
		if strings.TrimSpace(spec.Storage.Size) == "" {
			return spec, errors.New("storage size is required when storage is enabled")
		}
		storageMiB, err := ParseMemoryMiB(spec.Storage.Size)
		if err != nil || storageMiB <= 0 {
			return spec, fmt.Errorf("invalid storage size %q", spec.Storage.Size)
		}
		if strings.TrimSpace(spec.Storage.MountPath) == "" {
			spec.Storage.MountPath = "/data"
		}
		if !strings.HasPrefix(spec.Storage.MountPath, "/") {
			return spec, errors.New("storage mount_path must be absolute")
		}
	}
	return spec, nil
}

func BundleQuotaDemand(spec KubernetesBundleSpec) (QuotaDemand, error) {
	return DeploymentQuotaDemand(spec.Compute)
}

func KubernetesBundleComputeSpecJSON(spec KubernetesBundleSpec) (string, error) {
	data, err := json.Marshal(spec.Compute)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func SagaNetworkResourceName(resourceName string) string {
	return kubernetesChildName(resourceName, "svc")
}

func SagaStorageResourceName(resourceName string) string {
	return kubernetesChildName(resourceName, "data")
}

func kubernetesChildName(resourceName, suffix string) string {
	suffix = "-" + suffix
	maxBase := 63 - len(suffix)
	base := strings.TrimRight(resourceName, "-")
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	if base == "" {
		base = "resource"
	}
	return base + suffix
}
