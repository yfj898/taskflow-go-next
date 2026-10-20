package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/yfj898/taskflow-go/internal/gpuflow"
)

type report struct {
	Jobs             int     `json:"jobs"`
	Nodes            int     `json:"nodes"`
	ElapsedMS        float64 `json:"elapsed_ms"`
	DecisionsPerSec  float64 `json:"decisions_per_sec"`
	Scheduled        int     `json:"scheduled"`
	CapacityRejected int     `json:"capacity_rejected"`
	QuotaRejected    int     `json:"quota_rejected"`
	Preemptions      int     `json:"preemptions"`
	PeakRunning      int     `json:"peak_running"`
}

func main() {
	jobsN := flag.Int("jobs", 100000, "number of synthetic AI jobs")
	nodesN := flag.Int("nodes", 64, "GPU nodes")
	flag.Parse()

	nodes := make([]gpuflow.Node, 0, *nodesN)
	for i := 0; i < *nodesN; i++ {
		gpuType := "A100"
		if i%4 == 0 {
			gpuType = "L4"
		}
		nodes = append(nodes, gpuflow.Node{
			Name: fmt.Sprintf("gpu-%03d", i), GPUType: gpuType,
			GPUs: 8, CPUMilli: 64000, MemoryMB: 256000,
		})
	}
	quota := map[string]int{"team-a": 192, "team-b": 192, "team-c": 192, "team-d": 192}
	scheduler, err := gpuflow.NewScheduler(nodes, quota)
	if err != nil {
		panic(err)
	}

	r := report{Jobs: *jobsN, Nodes: *nodesN}
	running := make([]string, 0, 512)
	start := time.Now()
	for i := 0; i < *jobsN; i++ {
		if len(running) > 300 || (i > 0 && i%5 == 0) {
			victim := running[0]
			running = running[1:]
			scheduler.Release(victim)
		}

		gpuType := "A100"
		if i%7 == 0 {
			gpuType = "L4"
		}
		replicas := 1 + (i % 4)
		priority := i % 10
		if i%97 == 0 {
			priority = 100
		}
		job := gpuflow.Job{
			ID: fmt.Sprintf("job-%d", i), Tenant: fmt.Sprintf("team-%c", 'a'+rune(i%4)),
			Priority: priority, GPUType: gpuType, Replicas: replicas, GPUPerReplica: 1,
			CPUMilliPerReplica: 1000, MemoryMBPerReplica: 2048,
			Preemptible: priority < 5, Checkpointable: priority < 5,
		}
		decision, err := scheduler.Schedule(job)
		switch err {
		case nil:
			r.Scheduled++
			r.Preemptions += len(decision.Preempted)
			for _, id := range decision.Preempted {
				for idx := range running {
					if running[idx] == id {
						running = append(running[:idx], running[idx+1:]...)
						break
					}
				}
			}
			running = append(running, job.ID)
			if len(running) > r.PeakRunning {
				r.PeakRunning = len(running)
			}
		case gpuflow.ErrTenantQuota:
			r.QuotaRejected++
		default:
			r.CapacityRejected++
		}
	}
	elapsed := time.Since(start)
	r.ElapsedMS = float64(elapsed.Microseconds()) / 1000
	r.DecisionsPerSec = float64(*jobsN) / elapsed.Seconds()
	encoded, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(encoded))
}
