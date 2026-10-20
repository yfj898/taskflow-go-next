package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yfj898/taskflow-go/internal/gpuflow"
)

type server struct {
	scheduler *gpuflow.Scheduler
	queue     *gpuflow.FairQueue
	requests  atomic.Uint64
	accepted  atomic.Uint64
	rejected  atomic.Uint64
	preempted atomic.Uint64
	released  atomic.Uint64
}

type scheduleResponse struct {
	Decision gpuflow.Decision `json:"decision"`
	Snapshot gpuflow.Snapshot `json:"snapshot"`
}

type queueResponse struct {
	Dispatches []gpuflow.DispatchEvent `json:"dispatches"`
	Queue      gpuflow.QueueSnapshot   `json:"queue"`
	Scheduler  gpuflow.Snapshot        `json:"scheduler"`
}

type rayJobRequest struct {
	Job        gpuflow.Job `json:"job"`
	Image      string      `json:"image"`
	Entrypoint string      `json:"entrypoint"`
	RayVersion string      `json:"ray_version"`
}

func main() {
	nodes := []gpuflow.Node{
		{Name: "a100-1", GPUType: "A100", GPUs: 8, CPUMilli: 64000, MemoryMB: 256000},
		{Name: "a100-2", GPUType: "A100", GPUs: 8, CPUMilli: 64000, MemoryMB: 256000},
		{Name: "l4-1", GPUType: "L4", GPUs: 4, CPUMilli: 32000, MemoryMB: 128000},
	}
	if raw := strings.TrimSpace(os.Getenv("GPUFLOW_NODES_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &nodes); err != nil {
			log.Fatalf("GPUFLOW_NODES_JSON: %v", err)
		}
	}

	quota := map[string]int{"team-a": 12, "team-b": 12}
	if raw := strings.TrimSpace(os.Getenv("GPUFLOW_TENANT_QUOTA_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &quota); err != nil {
			log.Fatalf("GPUFLOW_TENANT_QUOTA_JSON: %v", err)
		}
	}

	scheduler, err := gpuflow.NewScheduler(nodes, quota)
	if err != nil {
		log.Fatal(err)
	}

	weights := map[string]int{"team-a": 1, "team-b": 1}
	if raw := strings.TrimSpace(os.Getenv("GPUFLOW_TENANT_WEIGHT_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &weights); err != nil {
			log.Fatalf("GPUFLOW_TENANT_WEIGHT_JSON: %v", err)
		}
	}
	queue := gpuflow.NewFairQueue(scheduler, gpuflow.QueueConfig{
		AgingInterval:           time.Duration(intEnv("GPUFLOW_QUEUE_AGING_MS", 5000)) * time.Millisecond,
		MaxAgingBoost:           nonNegativeIntEnv("GPUFLOW_QUEUE_MAX_AGING_BOOST", 20),
		MaxConsecutivePerTenant: intEnv("GPUFLOW_QUEUE_MAX_CONSECUTIVE", 4),
		TenantWeights:           weights,
	})

	s := &server{scheduler: scheduler, queue: queue}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/gpu-jobs/schedule", s.schedule)
	mux.HandleFunc("/api/v1/gpu-jobs/enqueue", s.enqueue)
	mux.HandleFunc("/api/v1/gpu-jobs/dispatch", s.dispatch)
	mux.HandleFunc("/api/v1/gpu-jobs/release", s.release)
	mux.HandleFunc("/api/v1/gpu-jobs/rayjob", s.rayJob)
	mux.HandleFunc("/api/v1/gpu-jobs/snapshot", s.snapshot)
	mux.HandleFunc("/api/v1/gpu-jobs/queue", s.queueSnapshot)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	addr := getenv("GPUFLOW_ADDR", ":8090")
	log.Printf("gpuflow control plane listening on %s with %d nodes", addr, len(nodes))
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *server) schedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.requests.Add(1)
	var job gpuflow.Job
	if err := decodeJSON(r, &job); err != nil {
		s.rejected.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	decision, err := s.scheduler.Schedule(job)
	if err != nil {
		s.rejected.Add(1)
		status := http.StatusConflict
		if err == gpuflow.ErrTenantQuota {
			status = http.StatusTooManyRequests
		}
		http.Error(w, err.Error(), status)
		return
	}
	s.accepted.Add(1)
	s.preempted.Add(uint64(len(decision.Preempted)))
	writeJSON(w, http.StatusCreated, scheduleResponse{Decision: decision, Snapshot: s.scheduler.Snapshot()})
}

func (s *server) enqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var job gpuflow.Job
	if err := decodeJSON(r, &job); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	now := time.Now()
	if err := s.queue.Enqueue(job, now); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	events := s.queue.DispatchAvailable(now, 0)
	s.observeQueueDispatches(events)
	writeJSON(w, http.StatusAccepted, queueResponse{Dispatches: events, Queue: s.queue.Snapshot(), Scheduler: s.scheduler.Snapshot()})
}

func (s *server) dispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Limit int `json:"limit"`
	}
	if err := decodeJSON(r, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	events := s.queue.DispatchAvailable(time.Now(), body.Limit)
	s.observeQueueDispatches(events)
	writeJSON(w, http.StatusOK, queueResponse{Dispatches: events, Queue: s.queue.Snapshot(), Scheduler: s.scheduler.Snapshot()})
}

func (s *server) release(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := decodeJSON(r, &body); err != nil || strings.TrimSpace(body.ID) == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if !s.scheduler.Release(body.ID) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	s.released.Add(1)
	events := s.queue.DispatchAvailable(time.Now(), 0)
	s.observeQueueDispatches(events)
	writeJSON(w, http.StatusOK, queueResponse{Dispatches: events, Queue: s.queue.Snapshot(), Scheduler: s.scheduler.Snapshot()})
}

func (s *server) rayJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body rayJobRequest
	if err := decodeJSON(r, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	manifest, err := gpuflow.BuildRayJob(body.Job, body.Image, body.Entrypoint, body.RayVersion)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *server) snapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, s.scheduler.Snapshot())
}

func (s *server) queueSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, s.queue.Snapshot())
}

func (s *server) observeQueueDispatches(events []gpuflow.DispatchEvent) {
	for _, event := range events {
		s.accepted.Add(1)
		s.preempted.Add(uint64(len(event.Decision.Preempted)))
	}
}

func (s *server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_schedule_requests_total counter\ngpuflow_schedule_requests_total %d\n", s.requests.Load())
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_schedule_accepted_total counter\ngpuflow_schedule_accepted_total %d\n", s.accepted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_schedule_rejected_total counter\ngpuflow_schedule_rejected_total %d\n", s.rejected.Load())
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_preemptions_total counter\ngpuflow_preemptions_total %d\n", s.preempted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_releases_total counter\ngpuflow_releases_total %d\n", s.released.Load())

	queueSnapshot := s.queue.Snapshot()
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_queue_pending gauge\ngpuflow_queue_pending %d\n", queueSnapshot.Pending)
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_queue_requeued_total counter\ngpuflow_queue_requeued_total %d\n", queueSnapshot.Requeued)
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_queue_wait_seconds summary\ngpuflow_queue_wait_seconds_count %d\ngpuflow_queue_wait_seconds_sum %.9f\n", queueSnapshot.WaitCount, queueSnapshot.WaitSecondsSum)
	_, _ = fmt.Fprintf(w, "# TYPE gpuflow_queue_jain_fairness gauge\ngpuflow_queue_jain_fairness %.9f\n", queueSnapshot.JainFairness)
	for tenant, pending := range queueSnapshot.PendingPerTenant {
		_, _ = fmt.Fprintf(w, "gpuflow_queue_pending_by_tenant{tenant=%q} %d\n", tenant, pending)
	}
	for tenant, dispatched := range queueSnapshot.Dispatched {
		_, _ = fmt.Fprintf(w, "gpuflow_queue_dispatched_total{tenant=%q} %d\n", tenant, dispatched)
	}

	snapshot := s.scheduler.Snapshot()
	for _, node := range snapshot.Nodes {
		_, _ = fmt.Fprintf(w, "gpuflow_node_gpu_used{node=%q,gpu_type=%q} %d\n", node.Name, node.GPUType, node.GPUsUsed)
		_, _ = fmt.Fprintf(w, "gpuflow_node_gpu_total{node=%q,gpu_type=%q} %d\n", node.Name, node.GPUType, node.GPUsTotal)
	}
	for tenant, usage := range snapshot.TenantUsage {
		_, _ = fmt.Fprintf(w, "gpuflow_tenant_gpu_used{tenant=%q} %d\n", tenant, usage)
	}
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	value, err := strconv.Atoi(getenv(key, strconv.Itoa(fallback)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func nonNegativeIntEnv(key string, fallback int) int {
	value, err := strconv.Atoi(getenv(key, strconv.Itoa(fallback)))
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
