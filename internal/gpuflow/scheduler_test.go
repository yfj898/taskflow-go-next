package gpuflow

import (
	"errors"
	"testing"
)

func testScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s, err := NewScheduler([]Node{
		{Name: "a100-1", GPUType: "A100", GPUs: 4, CPUMilli: 16000, MemoryMB: 64000},
		{Name: "a100-2", GPUType: "A100", GPUs: 4, CPUMilli: 16000, MemoryMB: 64000},
		{Name: "l4-1", GPUType: "L4", GPUs: 2, CPUMilli: 8000, MemoryMB: 32000},
	}, map[string]int{"team-a": 6, "team-b": 8})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGangPlacementIsAtomic(t *testing.T) {
	s := testScheduler(t)
	job := Job{ID: "train-1", Tenant: "team-a", Priority: 5, GPUType: "A100", Replicas: 4, GPUPerReplica: 1, CPUMilliPerReplica: 1000, MemoryMBPerReplica: 4000}
	decision, err := s.Schedule(job)
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	if len(decision.Placements) != 4 {
		t.Fatalf("placements = %d, want 4", len(decision.Placements))
	}
	snapshot := s.Snapshot()
	if snapshot.TenantUsage["team-a"] != 4 || snapshot.RunningJobs != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestTenantGPUQuota(t *testing.T) {
	s := testScheduler(t)
	_, err := s.Schedule(Job{ID: "too-large", Tenant: "team-a", Priority: 1, GPUType: "A100", Replicas: 7, GPUPerReplica: 1})
	if !errors.Is(err, ErrTenantQuota) {
		t.Fatalf("error = %v, want ErrTenantQuota", err)
	}
}

func TestPriorityPreemptionRequiresCheckpointableVictim(t *testing.T) {
	s := testScheduler(t)
	low := Job{ID: "low", Tenant: "team-b", Priority: 1, GPUType: "A100", Replicas: 4, GPUPerReplica: 1, Preemptible: true, Checkpointable: true}
	if _, err := s.Schedule(low); err != nil {
		t.Fatal(err)
	}
	low2 := low
	low2.ID = "low-2"
	if _, err := s.Schedule(low2); err != nil {
		t.Fatal(err)
	}

	high := Job{ID: "urgent", Tenant: "team-a", Priority: 100, GPUType: "A100", Replicas: 2, GPUPerReplica: 1}
	decision, err := s.Schedule(high)
	if err != nil {
		t.Fatalf("high priority schedule error = %v", err)
	}
	if len(decision.Preempted) == 0 {
		t.Fatal("expected at least one preempted job")
	}
	if decision.Preempted[0] != "low" && decision.Preempted[0] != "low-2" {
		t.Fatalf("unexpected preempted set: %v", decision.Preempted)
	}
}

func TestNonCheckpointableJobIsNotPreempted(t *testing.T) {
	s, err := NewScheduler([]Node{{Name: "gpu", GPUType: "A100", GPUs: 2, CPUMilli: 4000, MemoryMB: 16000}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Schedule(Job{ID: "protected", Tenant: "a", Priority: 1, GPUType: "A100", Replicas: 2, GPUPerReplica: 1, Preemptible: true, Checkpointable: false})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Schedule(Job{ID: "urgent", Tenant: "b", Priority: 100, GPUType: "A100", Replicas: 1, GPUPerReplica: 1})
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("error = %v, want ErrInsufficientCapacity", err)
	}
}

func TestGPUTypeConstraint(t *testing.T) {
	s := testScheduler(t)
	decision, err := s.Schedule(Job{ID: "infer", Tenant: "team-a", Priority: 1, GPUType: "L4", Replicas: 2, GPUPerReplica: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, placement := range decision.Placements {
		if placement.NodeName != "l4-1" {
			t.Fatalf("placement = %+v, want L4 node", placement)
		}
	}
}

func TestReleaseRestoresQuotaAndCapacity(t *testing.T) {
	s := testScheduler(t)
	job := Job{ID: "train", Tenant: "team-a", Priority: 1, GPUType: "A100", Replicas: 6, GPUPerReplica: 1}
	if _, err := s.Schedule(job); err != nil {
		t.Fatal(err)
	}
	if !s.Release(job.ID) {
		t.Fatal("Release() = false")
	}
	if got := s.Snapshot().TenantUsage["team-a"]; got != 0 {
		t.Fatalf("tenant usage = %d, want 0", got)
	}
	if _, err := s.Schedule(job); err != nil {
		t.Fatalf("reschedule after release: %v", err)
	}
}
