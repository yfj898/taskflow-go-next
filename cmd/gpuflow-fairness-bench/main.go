package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/yfj898/taskflow-go/internal/gpuflow"
)

type report struct {
	Dispatches      int                `json:"dispatches"`
	TenantWeights   map[string]int     `json:"tenant_weights"`
	TenantDispatch  map[string]uint64  `json:"tenant_dispatch"`
	TenantShare     map[string]float64 `json:"tenant_share"`
	JainFairness    float64            `json:"jain_fairness_normalized_by_weight"`
	P50WaitMS       float64            `json:"p50_wait_ms"`
	P95WaitMS       float64            `json:"p95_wait_ms"`
	DecisionRate    float64            `json:"decisions_per_sec"`
	RemainingQueued int                `json:"remaining_queued"`
}

func main() {
	scheduler, err := gpuflow.NewScheduler([]gpuflow.Node{{Name: "a100", GPUType: "A100", GPUs: 1, CPUMilli: 8000, MemoryMB: 32000}}, nil)
	if err != nil {
		panic(err)
	}
	weights := map[string]int{"team-a": 1, "team-b": 1, "team-c": 2}
	queue := gpuflow.NewFairQueue(scheduler, gpuflow.QueueConfig{
		AgingInterval:           time.Hour,
		MaxAgingBoost:           0,
		MaxConsecutivePerTenant: 4,
		TenantWeights:           weights,
	})
	base := time.Unix(0, 0)
	for i := 0; i < 1000; i++ {
		for _, tenant := range []string{"team-a", "team-b", "team-c"} {
			job := gpuflow.Job{
				ID: fmt.Sprintf("%s-%04d", tenant, i), Tenant: tenant, Priority: 1,
				GPUType: "A100", Replicas: 1, GPUPerReplica: 1,
				CPUMilliPerReplica: 1000, MemoryMBPerReplica: 1024,
			}
			if err := queue.Enqueue(job, base); err != nil {
				panic(err)
			}
		}
	}

	const dispatches = 1200
	waits := make([]float64, 0, dispatches)
	counts := map[string]uint64{}
	wallStart := time.Now()
	for i := 0; i < dispatches; i++ {
		now := base.Add(time.Duration(i) * 10 * time.Millisecond)
		event, err := queue.DispatchOne(now)
		if err != nil {
			panic(err)
		}
		counts[event.Job.Tenant]++
		waits = append(waits, event.WaitMS)
		if !scheduler.Release(event.Job.ID) {
			panic("release failed")
		}
	}
	wallElapsed := time.Since(wallStart)
	sort.Float64s(waits)
	shares := make(map[string]float64, len(counts))
	for tenant, count := range counts {
		shares[tenant] = float64(count) / dispatches
	}
	out := report{
		Dispatches: dispatches, TenantWeights: weights, TenantDispatch: counts, TenantShare: shares,
		JainFairness: gpuflow.JainFairness(counts, weights),
		P50WaitMS:    waits[(len(waits)-1)/2], P95WaitMS: waits[int(float64(len(waits)-1)*0.95)],
		DecisionRate: float64(dispatches) / wallElapsed.Seconds(), RemainingQueued: queue.Snapshot().Pending,
	}
	encoded, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(encoded))
}
