package gpuflow

import "fmt"

// RayJob is a deliberately small subset of the KubeRay RayJob CRD used by
// GPUFlow. It keeps the integration boundary dependency-free while still
// producing Kubernetes-compatible JSON/YAML fields.
type RayJob struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   RayJobMetadata `json:"metadata"`
	Spec       RayJobSpec     `json:"spec"`
}

type RayJobMetadata struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

type RayJobSpec struct {
	ShutdownAfterJobFinishes bool           `json:"shutdownAfterJobFinishes"`
	Entrypoint               string         `json:"entrypoint"`
	RayClusterSpec           RayClusterSpec `json:"rayClusterSpec"`
}

type RayClusterSpec struct {
	RayVersion       string               `json:"rayVersion"`
	HeadGroupSpec    RayHeadGroupSpec     `json:"headGroupSpec"`
	WorkerGroupSpecs []RayWorkerGroupSpec `json:"workerGroupSpecs"`
}

type RayHeadGroupSpec struct {
	RayStartParams map[string]string `json:"rayStartParams"`
	Template       RayPodTemplate    `json:"template"`
}

type RayWorkerGroupSpec struct {
	GroupName      string            `json:"groupName"`
	Replicas       int               `json:"replicas"`
	MinReplicas    int               `json:"minReplicas"`
	MaxReplicas    int               `json:"maxReplicas"`
	RayStartParams map[string]string `json:"rayStartParams"`
	Template       RayPodTemplate    `json:"template"`
}

type RayPodTemplate struct {
	Metadata RayPodMetadata `json:"metadata,omitempty"`
	Spec     RayPodSpec     `json:"spec"`
}

type RayPodMetadata struct {
	Labels map[string]string `json:"labels,omitempty"`
}

type RayPodSpec struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Containers   []RayContainer    `json:"containers"`
}

type RayContainer struct {
	Name      string       `json:"name"`
	Image     string       `json:"image"`
	Resources RayResources `json:"resources"`
}

type RayResources struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

func BuildRayJob(job Job, image, entrypoint, rayVersion string) (RayJob, error) {
	if err := validateJob(job); err != nil {
		return RayJob{}, err
	}
	if image == "" || entrypoint == "" || rayVersion == "" {
		return RayJob{}, ErrInvalidJob
	}
	gpuProduct := job.GPUType
	if gpuProduct == "" {
		gpuProduct = "any"
	}
	workerResources := map[string]string{
		"cpu":            milliCPU(job.CPUMilliPerReplica),
		"memory":         fmt.Sprintf("%dMi", job.MemoryMBPerReplica),
		"nvidia.com/gpu": fmt.Sprintf("%d", job.GPUPerReplica),
	}
	return RayJob{
		APIVersion: "ray.io/v1",
		Kind:       "RayJob",
		Metadata: RayJobMetadata{
			Name: job.ID,
			Labels: map[string]string{
				"gpuflow.yfj.dev/tenant":    job.Tenant,
				"gpuflow.yfj.dev/gpu-class": gpuProduct,
			},
		},
		Spec: RayJobSpec{
			ShutdownAfterJobFinishes: true,
			Entrypoint:               entrypoint,
			RayClusterSpec: RayClusterSpec{
				RayVersion: rayVersion,
				HeadGroupSpec: RayHeadGroupSpec{
					RayStartParams: map[string]string{"dashboard-host": "0.0.0.0"},
					Template: RayPodTemplate{Spec: RayPodSpec{Containers: []RayContainer{{
						Name: "ray-head", Image: image,
						Resources: RayResources{
							Requests: map[string]string{"cpu": "1", "memory": "2Gi"},
							Limits:   map[string]string{"cpu": "2", "memory": "4Gi"},
						},
					}}}},
				},
				WorkerGroupSpecs: []RayWorkerGroupSpec{{
					GroupName: "gpu-workers", Replicas: job.Replicas,
					MinReplicas: job.Replicas, MaxReplicas: job.Replicas,
					RayStartParams: map[string]string{},
					Template: RayPodTemplate{
						Metadata: RayPodMetadata{Labels: map[string]string{"gpuflow.yfj.dev/gpu-class": gpuProduct}},
						Spec: RayPodSpec{
							NodeSelector: gpuNodeSelector(job.GPUType),
							Containers: []RayContainer{{
								Name: "ray-worker", Image: image,
								Resources: RayResources{Requests: workerResources, Limits: workerResources},
							}},
						},
					},
				}},
			},
		},
	}, nil
}

func gpuNodeSelector(gpuType string) map[string]string {
	if gpuType == "" {
		return nil
	}
	return map[string]string{"gpuflow.yfj.dev/gpu-class": gpuType}
}

func milliCPU(value int) string {
	if value <= 0 {
		return "0"
	}
	if value%1000 == 0 {
		return fmt.Sprintf("%d", value/1000)
	}
	return fmt.Sprintf("%dm", value)
}
