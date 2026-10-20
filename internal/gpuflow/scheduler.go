package gpuflow

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrInvalidJob           = errors.New("invalid gpu job")
	ErrDuplicateJob         = errors.New("job already running")
	ErrTenantQuota          = errors.New("tenant gpu quota exceeded")
	ErrInsufficientCapacity = errors.New("insufficient cluster capacity")
)

type Job struct {
	ID                 string `json:"id"`
	Tenant             string `json:"tenant"`
	Priority           int    `json:"priority"`
	GPUType            string `json:"gpu_type"`
	Replicas           int    `json:"replicas"`
	GPUPerReplica      int    `json:"gpu_per_replica"`
	CPUMilliPerReplica int    `json:"cpu_milli_per_replica"`
	MemoryMBPerReplica int    `json:"memory_mb_per_replica"`
	Preemptible        bool   `json:"preemptible"`
	Checkpointable     bool   `json:"checkpointable"`
}

func (j Job) TotalGPUs() int { return j.Replicas * j.GPUPerReplica }

type Node struct {
	Name     string `json:"name"`
	GPUType  string `json:"gpu_type"`
	GPUs     int    `json:"gpus"`
	CPUMilli int    `json:"cpu_milli"`
	MemoryMB int    `json:"memory_mb"`
}

type Placement struct {
	NodeName string `json:"node_name"`
	Replica  int    `json:"replica"`
	GPUs     int    `json:"gpus"`
	CPUMilli int    `json:"cpu_milli"`
	MemoryMB int    `json:"memory_mb"`
}

type Decision struct {
	Placements    []Placement `json:"placements"`
	Preempted     []string    `json:"preempted,omitempty"`
	PreemptedJobs []Job       `json:"preempted_jobs,omitempty"`
}

type Snapshot struct {
	Nodes       []NodeSnapshot `json:"nodes"`
	TenantUsage map[string]int `json:"tenant_usage"`
	RunningJobs int            `json:"running_jobs"`
}

type NodeSnapshot struct {
	Name          string `json:"name"`
	GPUType       string `json:"gpu_type"`
	GPUsUsed      int    `json:"gpus_used"`
	GPUsTotal     int    `json:"gpus_total"`
	CPUMilliUsed  int    `json:"cpu_milli_used"`
	CPUMilliTotal int    `json:"cpu_milli_total"`
	MemoryMBUsed  int    `json:"memory_mb_used"`
	MemoryMBTotal int    `json:"memory_mb_total"`
}

type allocation struct {
	jobID          string
	tenant         string
	priority       int
	preemptible    bool
	checkpointable bool
	placement      Placement
}

type nodeState struct {
	node        Node
	gpuUsed     int
	cpuUsed     int
	memoryUsed  int
	allocations map[string][]allocation
}

func (n *nodeState) clone() *nodeState {
	out := &nodeState{
		node:        n.node,
		gpuUsed:     n.gpuUsed,
		cpuUsed:     n.cpuUsed,
		memoryUsed:  n.memoryUsed,
		allocations: make(map[string][]allocation, len(n.allocations)),
	}
	for id, values := range n.allocations {
		copyValues := make([]allocation, len(values))
		copy(copyValues, values)
		out.allocations[id] = copyValues
	}
	return out
}

func (n *nodeState) fits(job Job) bool {
	if job.GPUType != "" && n.node.GPUType != job.GPUType {
		return false
	}
	return n.node.GPUs-n.gpuUsed >= job.GPUPerReplica &&
		n.node.CPUMilli-n.cpuUsed >= job.CPUMilliPerReplica &&
		n.node.MemoryMB-n.memoryUsed >= job.MemoryMBPerReplica
}

func (n *nodeState) add(job Job, replica int) Placement {
	p := Placement{
		NodeName: n.node.Name,
		Replica:  replica,
		GPUs:     job.GPUPerReplica,
		CPUMilli: job.CPUMilliPerReplica,
		MemoryMB: job.MemoryMBPerReplica,
	}
	n.gpuUsed += p.GPUs
	n.cpuUsed += p.CPUMilli
	n.memoryUsed += p.MemoryMB
	n.allocations[job.ID] = append(n.allocations[job.ID], allocation{
		jobID:          job.ID,
		tenant:         job.Tenant,
		priority:       job.Priority,
		preemptible:    job.Preemptible,
		checkpointable: job.Checkpointable,
		placement:      p,
	})
	return p
}

func (n *nodeState) remove(jobID string) []allocation {
	values := n.allocations[jobID]
	if len(values) == 0 {
		return nil
	}
	for _, a := range values {
		n.gpuUsed -= a.placement.GPUs
		n.cpuUsed -= a.placement.CPUMilli
		n.memoryUsed -= a.placement.MemoryMB
	}
	delete(n.allocations, jobID)
	return values
}

type runningJob struct {
	job        Job
	placements []Placement
}

type Scheduler struct {
	mu          sync.Mutex
	nodes       map[string]*nodeState
	order       []string
	tenantQuota map[string]int
	tenantUsage map[string]int
	running     map[string]runningJob
}

func NewScheduler(nodes []Node, tenantQuota map[string]int) (*Scheduler, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("nodes: %w", ErrInvalidJob)
	}
	s := &Scheduler{
		nodes:       make(map[string]*nodeState, len(nodes)),
		tenantQuota: make(map[string]int, len(tenantQuota)),
		tenantUsage: make(map[string]int),
		running:     make(map[string]runningJob),
	}
	for _, node := range nodes {
		if node.Name == "" || node.GPUs < 0 || node.CPUMilli < 0 || node.MemoryMB < 0 {
			return nil, fmt.Errorf("node %q: %w", node.Name, ErrInvalidJob)
		}
		if _, exists := s.nodes[node.Name]; exists {
			return nil, fmt.Errorf("duplicate node %q", node.Name)
		}
		s.nodes[node.Name] = &nodeState{node: node, allocations: make(map[string][]allocation)}
		s.order = append(s.order, node.Name)
	}
	sort.Strings(s.order)
	for tenant, quota := range tenantQuota {
		if quota < 0 {
			return nil, fmt.Errorf("tenant %q quota: %w", tenant, ErrInvalidJob)
		}
		s.tenantQuota[tenant] = quota
	}
	return s, nil
}

func validateJob(job Job) error {
	if job.ID == "" || job.Tenant == "" || job.Replicas <= 0 || job.GPUPerReplica < 0 || job.CPUMilliPerReplica < 0 || job.MemoryMBPerReplica < 0 {
		return ErrInvalidJob
	}
	return nil
}

func (s *Scheduler) Schedule(job Job) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := validateJob(job); err != nil {
		return Decision{}, err
	}
	if _, exists := s.running[job.ID]; exists {
		return Decision{}, ErrDuplicateJob
	}
	if quota, ok := s.tenantQuota[job.Tenant]; ok && s.tenantUsage[job.Tenant]+job.TotalGPUs() > quota {
		return Decision{}, ErrTenantQuota
	}

	working := cloneNodes(s.nodes)
	placements, ok := placeGang(working, s.order, job)
	if ok {
		s.commit(job, placements, nil)
		return Decision{Placements: placements}, nil
	}

	candidates := make([]runningJob, 0, len(s.running))
	for _, existing := range s.running {
		if existing.job.Priority < job.Priority && existing.job.Preemptible && existing.job.Checkpointable {
			candidates = append(candidates, existing)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].job.Priority == candidates[j].job.Priority {
			return candidates[i].job.TotalGPUs() > candidates[j].job.TotalGPUs()
		}
		return candidates[i].job.Priority < candidates[j].job.Priority
	})

	preempted := make([]string, 0)
	for _, candidate := range candidates {
		removeFromNodes(working, candidate.job.ID)
		preempted = append(preempted, candidate.job.ID)
		placements, ok = placeGang(working, s.order, job)
		if ok {
			preemptedJobs := make([]Job, 0, len(preempted))
			for _, id := range preempted {
				if running, exists := s.running[id]; exists {
					preemptedJobs = append(preemptedJobs, running.job)
				}
			}
			s.commit(job, placements, preempted)
			return Decision{Placements: placements, Preempted: preempted, PreemptedJobs: preemptedJobs}, nil
		}
	}
	return Decision{}, ErrInsufficientCapacity
}

func (s *Scheduler) commit(job Job, placements []Placement, preempted []string) {
	for _, id := range preempted {
		s.releaseLocked(id)
	}
	for _, p := range placements {
		node := s.nodes[p.NodeName]
		node.add(job, p.Replica)
	}
	s.tenantUsage[job.Tenant] += job.TotalGPUs()
	copyPlacements := make([]Placement, len(placements))
	copy(copyPlacements, placements)
	s.running[job.ID] = runningJob{job: job, placements: copyPlacements}
}

func (s *Scheduler) Release(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseLocked(jobID)
}

func (s *Scheduler) releaseLocked(jobID string) bool {
	running, ok := s.running[jobID]
	if !ok {
		return false
	}
	removeFromNodes(s.nodes, jobID)
	s.tenantUsage[running.job.Tenant] -= running.job.TotalGPUs()
	if s.tenantUsage[running.job.Tenant] <= 0 {
		delete(s.tenantUsage, running.job.Tenant)
	}
	delete(s.running, jobID)
	return true
}

func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Snapshot{
		Nodes:       make([]NodeSnapshot, 0, len(s.order)),
		TenantUsage: make(map[string]int, len(s.tenantUsage)),
		RunningJobs: len(s.running),
	}
	for _, name := range s.order {
		n := s.nodes[name]
		out.Nodes = append(out.Nodes, NodeSnapshot{
			Name: n.node.Name, GPUType: n.node.GPUType,
			GPUsUsed: n.gpuUsed, GPUsTotal: n.node.GPUs,
			CPUMilliUsed: n.cpuUsed, CPUMilliTotal: n.node.CPUMilli,
			MemoryMBUsed: n.memoryUsed, MemoryMBTotal: n.node.MemoryMB,
		})
	}
	for tenant, usage := range s.tenantUsage {
		out.TenantUsage[tenant] = usage
	}
	return out
}

func cloneNodes(nodes map[string]*nodeState) map[string]*nodeState {
	out := make(map[string]*nodeState, len(nodes))
	for name, node := range nodes {
		out[name] = node.clone()
	}
	return out
}

func removeFromNodes(nodes map[string]*nodeState, jobID string) {
	for _, node := range nodes {
		node.remove(jobID)
	}
}

func placeGang(nodes map[string]*nodeState, order []string, job Job) ([]Placement, bool) {
	placements := make([]Placement, 0, job.Replicas)
	for replica := 0; replica < job.Replicas; replica++ {
		var best *nodeState
		bestGPUWaste := int(^uint(0) >> 1)
		bestMemoryWaste := int(^uint(0) >> 1)
		for _, name := range order {
			node := nodes[name]
			if !node.fits(job) {
				continue
			}
			gpuWaste := node.node.GPUs - node.gpuUsed - job.GPUPerReplica
			memoryWaste := node.node.MemoryMB - node.memoryUsed - job.MemoryMBPerReplica
			if best == nil || gpuWaste < bestGPUWaste || (gpuWaste == bestGPUWaste && memoryWaste < bestMemoryWaste) {
				best = node
				bestGPUWaste = gpuWaste
				bestMemoryWaste = memoryWaste
			}
		}
		if best == nil {
			return nil, false
		}
		placements = append(placements, best.add(job, replica))
	}
	return placements, true
}
