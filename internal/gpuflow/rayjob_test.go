package gpuflow

import (
	"encoding/json"
	"testing"
)

func TestBuildRayJob(t *testing.T) {
	job := Job{
		ID: "llm-train", Tenant: "team-a", GPUType: "A100",
		Replicas: 2, GPUPerReplica: 1, CPUMilliPerReplica: 4000, MemoryMBPerReplica: 16384,
	}
	manifest, err := BuildRayJob(job, "rayproject/ray:2.49.0-py312-gpu", "python /workspace/train.py", "2.49.0")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.APIVersion != "ray.io/v1" || manifest.Kind != "RayJob" {
		t.Fatalf("unexpected GVK: %s %s", manifest.APIVersion, manifest.Kind)
	}
	workers := manifest.Spec.RayClusterSpec.WorkerGroupSpecs
	if len(workers) != 1 || workers[0].Replicas != 2 {
		t.Fatalf("unexpected workers: %+v", workers)
	}
	resources := workers[0].Template.Spec.Containers[0].Resources.Limits
	if resources["nvidia.com/gpu"] != "1" || resources["cpu"] != "4" || resources["memory"] != "16384Mi" {
		t.Fatalf("unexpected resources: %+v", resources)
	}
	if got := workers[0].Template.Spec.NodeSelector["gpuflow.yfj.dev/gpu-class"]; got != "A100" {
		t.Fatalf("gpu selector = %q", got)
	}
	if _, err := json.Marshal(manifest); err != nil {
		t.Fatalf("json marshal: %v", err)
	}
}
