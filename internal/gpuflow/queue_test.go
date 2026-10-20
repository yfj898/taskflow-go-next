package gpuflow

import (
	"errors"
	"testing"
	"time"
)

func TestFairQueueWeightedShare(t *testing.T) {
	scheduler, err := NewScheduler([]Node{{Name: "gpu", GPUType: "A100", GPUs: 1, CPUMilli: 8000, MemoryMB: 32000}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	weights := map[string]int{"a": 1, "b": 1, "c": 2}
	queue := NewFairQueue(scheduler, QueueConfig{AgingInterval: time.Hour, MaxAgingBoost: 0, MaxConsecutivePerTenant: 4, TenantWeights: weights})
	base := time.Unix(0, 0)
	for i := 0; i < 100; i++ {
		for _, tenant := range []string{"a", "b", "c"} {
			job := Job{ID: tenant + "-" + itoaTest(i), Tenant: tenant, Priority: 1, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}
			if err := queue.Enqueue(job, base); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts := map[string]uint64{}
	for i := 0; i < 80; i++ {
		event, err := queue.DispatchOne(base.Add(time.Duration(i) * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		counts[event.Job.Tenant]++
		if !scheduler.Release(event.Job.ID) {
			t.Fatalf("failed to release %s", event.Job.ID)
		}
	}
	if counts["a"] != 20 || counts["b"] != 20 || counts["c"] != 40 {
		t.Fatalf("weighted counts = %v, want 20/20/40", counts)
	}
	if fairness := JainFairness(counts, weights); fairness < 0.9999 {
		t.Fatalf("fairness = %.6f, want nearly 1", fairness)
	}
}

func TestFairQueueAgingPreventsStarvation(t *testing.T) {
	scheduler, err := NewScheduler([]Node{{Name: "gpu", GPUType: "A100", GPUs: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	queue := NewFairQueue(scheduler, QueueConfig{AgingInterval: time.Second, MaxAgingBoost: 10, MaxConsecutivePerTenant: 4})
	base := time.Unix(0, 0)
	if err := queue.Enqueue(Job{ID: "old-low", Tenant: "a", Priority: 0, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}, base); err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(Job{ID: "new-high", Tenant: "b", Priority: 4, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}, base.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	event, err := queue.DispatchOne(base.Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if event.Job.ID != "old-low" {
		t.Fatalf("aging selected %s, want old-low", event.Job.ID)
	}
}

func TestFairQueueSkipsQuotaBlockedTenant(t *testing.T) {
	scheduler, err := NewScheduler([]Node{{Name: "gpu", GPUType: "A100", GPUs: 1}}, map[string]int{"blocked": 0, "ok": 1})
	if err != nil {
		t.Fatal(err)
	}
	queue := NewFairQueue(scheduler, QueueConfig{})
	now := time.Now()
	_ = queue.Enqueue(Job{ID: "blocked", Tenant: "blocked", Priority: 10, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}, now)
	_ = queue.Enqueue(Job{ID: "ok", Tenant: "ok", Priority: 1, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}, now)
	event, err := queue.DispatchOne(now)
	if err != nil {
		t.Fatal(err)
	}
	if event.Job.ID != "ok" {
		t.Fatalf("selected %s, want quota-admissible job", event.Job.ID)
	}
}

func TestFairQueueRequeuesCheckpointedPreemption(t *testing.T) {
	scheduler, err := NewScheduler([]Node{{Name: "gpu", GPUType: "A100", GPUs: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	low := Job{ID: "low", Tenant: "a", Priority: 1, GPUType: "A100", Replicas: 1, GPUPerReplica: 1, Preemptible: true, Checkpointable: true}
	if _, err := scheduler.Schedule(low); err != nil {
		t.Fatal(err)
	}
	queue := NewFairQueue(scheduler, QueueConfig{})
	now := time.Now()
	if err := queue.Enqueue(Job{ID: "urgent", Tenant: "b", Priority: 100, GPUType: "A100", Replicas: 1, GPUPerReplica: 1}, now); err != nil {
		t.Fatal(err)
	}
	event, err := queue.DispatchOne(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(event.Decision.PreemptedJobs) != 1 || event.Decision.PreemptedJobs[0].ID != "low" {
		t.Fatalf("preempted jobs = %+v", event.Decision.PreemptedJobs)
	}
	snapshot := queue.Snapshot()
	if snapshot.Pending != 1 || snapshot.PendingPerTenant["a"] != 1 || snapshot.Requeued != 1 {
		t.Fatalf("checkpointed victim was not requeued: %+v", snapshot)
	}
	if _, err := queue.DispatchOne(now); !errors.Is(err, ErrQueueBlocked) {
		t.Fatalf("dispatch while urgent owns GPU = %v, want ErrQueueBlocked", err)
	}
}

func itoaTest(v int) string {
	if v == 0 {
		return "0"
	}
	digits := [20]byte{}
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	return string(digits[i:])
}
