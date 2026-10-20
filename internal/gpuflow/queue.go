package gpuflow

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrQueueEmpty     = errors.New("gpu queue empty")
	ErrQueueBlocked   = errors.New("gpu queue has no schedulable job")
	ErrQueueDuplicate = errors.New("job already queued")
)

type QueueConfig struct {
	AgingInterval           time.Duration
	MaxAgingBoost           int
	MaxConsecutivePerTenant int
	TenantWeights           map[string]int
}

type DispatchEvent struct {
	Job      Job           `json:"job"`
	Decision Decision      `json:"decision"`
	Wait     time.Duration `json:"-"`
	WaitMS   float64       `json:"wait_ms"`
}

type QueueSnapshot struct {
	Pending          int               `json:"pending"`
	PendingPerTenant map[string]int    `json:"pending_per_tenant"`
	Dispatched       map[string]uint64 `json:"dispatched"`
	JainFairness     float64           `json:"jain_fairness_normalized_by_weight"`
	Requeued         uint64            `json:"requeued"`
	WaitCount        uint64            `json:"wait_count"`
	WaitSecondsSum   float64           `json:"wait_seconds_sum"`
	MaxWaitMS        float64           `json:"max_wait_ms"`
}

type queuedJob struct {
	job         Job
	submittedAt time.Time
	sequence    uint64
}

type FairQueue struct {
	mu        sync.Mutex
	scheduler *Scheduler
	cfg       QueueConfig
	pending   []queuedJob
	pendingID map[string]struct{}
	sequence  uint64
	served    map[string]uint64

	lastTenant  string
	consecutive int
	requeued    uint64
	waitCount   uint64
	waitTotal   time.Duration
	maxWait     time.Duration
}

func NewFairQueue(scheduler *Scheduler, cfg QueueConfig) *FairQueue {
	if cfg.AgingInterval <= 0 {
		cfg.AgingInterval = 5 * time.Second
	}
	if cfg.MaxAgingBoost < 0 {
		cfg.MaxAgingBoost = 0
	}
	if cfg.MaxConsecutivePerTenant <= 0 {
		cfg.MaxConsecutivePerTenant = 4
	}
	weights := make(map[string]int, len(cfg.TenantWeights))
	for tenant, weight := range cfg.TenantWeights {
		if weight <= 0 {
			weight = 1
		}
		weights[tenant] = weight
	}
	cfg.TenantWeights = weights
	return &FairQueue{
		scheduler: scheduler,
		cfg:       cfg,
		pendingID: make(map[string]struct{}),
		served:    make(map[string]uint64),
	}
}

func (q *FairQueue) Enqueue(job Job, submittedAt time.Time) error {
	if err := validateJob(job); err != nil {
		return err
	}
	if submittedAt.IsZero() {
		submittedAt = time.Now()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.pendingID[job.ID]; exists {
		return ErrQueueDuplicate
	}
	q.sequence++
	q.pending = append(q.pending, queuedJob{job: job, submittedAt: submittedAt, sequence: q.sequence})
	q.pendingID[job.ID] = struct{}{}
	return nil
}

func (q *FairQueue) Cancel(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.pending {
		if q.pending[i].job.ID == jobID {
			q.removePending(i)
			return true
		}
	}
	return false
}

func (q *FairQueue) DispatchOne(now time.Time) (DispatchEvent, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return DispatchEvent{}, ErrQueueEmpty
	}
	if now.IsZero() {
		now = time.Now()
	}

	indexes := make([]int, len(q.pending))
	for i := range indexes {
		indexes[i] = i
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		return q.betterCandidate(q.pending[indexes[i]], q.pending[indexes[j]], now)
	})

	for _, idx := range indexes {
		candidate := q.pending[idx]
		decision, err := q.scheduler.Schedule(candidate.job)
		if err != nil {
			if errors.Is(err, ErrTenantQuota) || errors.Is(err, ErrInsufficientCapacity) {
				continue
			}
			return DispatchEvent{}, err
		}

		q.removePending(idx)
		wait := now.Sub(candidate.submittedAt)
		if wait < 0 {
			wait = 0
		}
		q.waitCount++
		q.waitTotal += wait
		if wait > q.maxWait {
			q.maxWait = wait
		}
		q.served[candidate.job.Tenant]++
		if q.lastTenant == candidate.job.Tenant {
			q.consecutive++
		} else {
			q.lastTenant = candidate.job.Tenant
			q.consecutive = 1
		}

		for _, preempted := range decision.PreemptedJobs {
			if _, exists := q.pendingID[preempted.ID]; exists {
				continue
			}
			q.sequence++
			q.pending = append(q.pending, queuedJob{job: preempted, submittedAt: now, sequence: q.sequence})
			q.pendingID[preempted.ID] = struct{}{}
			q.requeued++
		}
		return DispatchEvent{Job: candidate.job, Decision: decision, Wait: wait, WaitMS: float64(wait.Microseconds()) / 1000}, nil
	}
	return DispatchEvent{}, ErrQueueBlocked
}

func (q *FairQueue) DispatchAvailable(now time.Time, limit int) []DispatchEvent {
	events := make([]DispatchEvent, 0)
	for limit <= 0 || len(events) < limit {
		event, err := q.DispatchOne(now)
		if err != nil {
			break
		}
		events = append(events, event)
	}
	return events
}

func (q *FairQueue) Snapshot() QueueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := QueueSnapshot{
		Pending:          len(q.pending),
		PendingPerTenant: make(map[string]int),
		Dispatched:       make(map[string]uint64, len(q.served)),
		Requeued:         q.requeued,
		WaitCount:        q.waitCount,
		WaitSecondsSum:   q.waitTotal.Seconds(),
		MaxWaitMS:        float64(q.maxWait.Microseconds()) / 1000,
	}
	for _, item := range q.pending {
		out.PendingPerTenant[item.job.Tenant]++
	}
	for tenant, count := range q.served {
		out.Dispatched[tenant] = count
	}
	out.JainFairness = JainFairness(q.served, q.cfg.TenantWeights)
	return out
}

func (q *FairQueue) betterCandidate(a, b queuedJob, now time.Time) bool {
	priorityA := q.effectivePriority(a, now)
	priorityB := q.effectivePriority(b, now)
	if priorityA != priorityB {
		return priorityA > priorityB
	}
	blockedA := q.lastTenant == a.job.Tenant && q.consecutive >= q.cfg.MaxConsecutivePerTenant
	blockedB := q.lastTenant == b.job.Tenant && q.consecutive >= q.cfg.MaxConsecutivePerTenant
	if blockedA != blockedB {
		return !blockedA
	}
	weightA := q.weight(a.job.Tenant)
	weightB := q.weight(b.job.Tenant)
	left := q.served[a.job.Tenant] * uint64(weightB)
	right := q.served[b.job.Tenant] * uint64(weightA)
	if left != right {
		return left < right
	}
	if !a.submittedAt.Equal(b.submittedAt) {
		return a.submittedAt.Before(b.submittedAt)
	}
	return a.sequence < b.sequence
}

func (q *FairQueue) effectivePriority(item queuedJob, now time.Time) int {
	wait := now.Sub(item.submittedAt)
	if wait <= 0 || q.cfg.MaxAgingBoost == 0 {
		return item.job.Priority
	}
	boost := int(wait / q.cfg.AgingInterval)
	if boost > q.cfg.MaxAgingBoost {
		boost = q.cfg.MaxAgingBoost
	}
	return item.job.Priority + boost
}

func (q *FairQueue) weight(tenant string) int {
	if weight := q.cfg.TenantWeights[tenant]; weight > 0 {
		return weight
	}
	return 1
}

func (q *FairQueue) removePending(index int) {
	delete(q.pendingID, q.pending[index].job.ID)
	copy(q.pending[index:], q.pending[index+1:])
	q.pending = q.pending[:len(q.pending)-1]
}

func JainFairness(dispatched map[string]uint64, weights map[string]int) float64 {
	tenants := make(map[string]struct{}, len(dispatched)+len(weights))
	for tenant := range dispatched {
		tenants[tenant] = struct{}{}
	}
	for tenant := range weights {
		tenants[tenant] = struct{}{}
	}
	if len(tenants) == 0 {
		return 0
	}
	var sum, sumSquares float64
	for tenant := range tenants {
		value := dispatched[tenant]
		weight := weights[tenant]
		if weight <= 0 {
			weight = 1
		}
		normalized := float64(value) / float64(weight)
		sum += normalized
		sumSquares += normalized * normalized
	}
	if sumSquares == 0 {
		return 0
	}
	return (sum * sum) / (float64(len(tenants)) * sumSquares)
}
