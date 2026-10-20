package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

const (
	AISLORealtime = "realtime"
	AISLOStandard = "standard"
	AISLOBatch    = "batch"
)

var ErrAIBackpressure = errors.New("AI provider concurrency saturated")
var ErrAISLODeadline = errors.New("AI SLO deadline exceeded")

type AISLOPolicy struct {
	QueueWait time.Duration
	Timeout   time.Duration
	Priority  int
}

type AISLOConfig struct {
	Realtime AISLOPolicy
	Standard AISLOPolicy
	Batch    AISLOPolicy
}

type AIFairSchedulingConfig struct {
	Enabled                 bool
	MaxConsecutivePerTenant int
	StarvationAfter         time.Duration
}

func normalizeFairSchedulingConfig(config AIFairSchedulingConfig) AIFairSchedulingConfig {
	if config.MaxConsecutivePerTenant <= 0 {
		config.MaxConsecutivePerTenant = 2
	}
	if config.StarvationAfter <= 0 {
		config.StarvationAfter = 2 * time.Second
	}
	return config
}

func DefaultAISLOConfig() AISLOConfig {
	return AISLOConfig{
		Realtime: AISLOPolicy{QueueWait: 50 * time.Millisecond, Timeout: 3 * time.Second, Priority: 0},
		Standard: AISLOPolicy{QueueWait: time.Second, Timeout: 15 * time.Second, Priority: 1},
		Batch:    AISLOPolicy{QueueWait: 5 * time.Second, Timeout: 30 * time.Second, Priority: 2},
	}
}

func normalizeSLOClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case AISLORealtime:
		return AISLORealtime
	case AISLOBatch:
		return AISLOBatch
	case "", AISLOStandard:
		return AISLOStandard
	default:
		return ""
	}
}

func normalizeSLOConfig(config AISLOConfig) AISLOConfig {
	defaults := DefaultAISLOConfig()
	if config.Realtime.QueueWait <= 0 {
		config.Realtime.QueueWait = defaults.Realtime.QueueWait
	}
	if config.Realtime.Timeout <= 0 {
		config.Realtime.Timeout = defaults.Realtime.Timeout
	}
	config.Realtime.Priority = defaults.Realtime.Priority
	if config.Standard.QueueWait <= 0 {
		config.Standard.QueueWait = defaults.Standard.QueueWait
	}
	if config.Standard.Timeout <= 0 {
		config.Standard.Timeout = defaults.Standard.Timeout
	}
	config.Standard.Priority = defaults.Standard.Priority
	if config.Batch.QueueWait <= 0 {
		config.Batch.QueueWait = defaults.Batch.QueueWait
	}
	if config.Batch.Timeout <= 0 {
		config.Batch.Timeout = defaults.Batch.Timeout
	}
	config.Batch.Priority = defaults.Batch.Priority
	return config
}

func (config AISLOConfig) policy(class string) AISLOPolicy {
	switch normalizeSLOClass(class) {
	case AISLORealtime:
		return config.Realtime
	case AISLOBatch:
		return config.Batch
	default:
		return config.Standard
	}
}

type modelSlotWaiter struct {
	priority int
	tenantID string
	seq      uint64
	enqueued time.Time
	ready    chan struct{}
}

type modelConcurrencyGate struct {
	mu      sync.Mutex
	limit   int
	active  int
	nextSeq uint64
	waiters []*modelSlotWaiter
	fair    AIFairSchedulingConfig

	lastTenant      string
	consecutiveWins int
}

type modelQueueStateObserver func(active, queued, limit int)

func newModelConcurrencyGate(limit int, fair AIFairSchedulingConfig) *modelConcurrencyGate {
	if limit <= 0 {
		limit = 1
	}
	return &modelConcurrencyGate{limit: limit, fair: normalizeFairSchedulingConfig(fair)}
}

func (g *modelConcurrencyGate) acquire(ctx context.Context, priority int, tenantID string, maxWait time.Duration) (time.Duration, func(), error) {
	return g.acquireObserved(ctx, priority, tenantID, maxWait, nil)
}

func (g *modelConcurrencyGate) acquireObserved(
	ctx context.Context,
	priority int,
	tenantID string,
	maxWait time.Duration,
	onQueued modelQueueStateObserver,
) (time.Duration, func(), error) {
	started := time.Now()
	tenantID = normalizeTenantID(tenantID)
	g.mu.Lock()
	if g.active < g.limit {
		g.active++
		g.recordAdmissionLocked(tenantID)
		g.mu.Unlock()
		return 0, g.release, nil
	}
	g.nextSeq++
	w := &modelSlotWaiter{priority: priority, tenantID: tenantID, seq: g.nextSeq, enqueued: time.Now(), ready: make(chan struct{}, 1)}
	g.waiters = append(g.waiters, w)
	if onQueued != nil {
		// Report the queue transition while holding the gate lock so a concurrent
		// release cannot publish a newer queue state before this enqueue event.
		// Observers must be non-blocking and must not call back into the gate.
		onQueued(g.active, len(g.waiters), g.limit)
	}
	g.mu.Unlock()

	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-w.ready:
		return time.Since(started), g.release, nil
	case <-ctx.Done():
		if !g.cancel(w) {
			<-w.ready
			g.release()
		}
		return time.Since(started), nil, ctx.Err()
	case <-timer.C:
		if !g.cancel(w) {
			<-w.ready
			g.release()
		}
		return time.Since(started), nil, ErrAIBackpressure
	}
}

func (g *modelConcurrencyGate) cancel(target *modelSlotWaiter) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, waiter := range g.waiters {
		if waiter == target {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (g *modelConcurrencyGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.waiters) == 0 {
		if g.active > 0 {
			g.active--
		}
		return
	}
	best, promoted, rotated := g.pickNextLocked(time.Now())
	w := g.waiters[best]
	g.waiters = append(g.waiters[:best], g.waiters[best+1:]...)
	g.recordAdmissionLocked(w.tenantID)
	if promoted {
		appmetrics.AITenantFairScheduling.WithLabelValues("starvation_promoted").Inc()
	}
	if rotated {
		appmetrics.AITenantFairScheduling.WithLabelValues("tenant_rotated").Inc()
	}
	w.ready <- struct{}{}
}

func (g *modelConcurrencyGate) pickNextLocked(now time.Time) (best int, starvationPromoted, tenantRotated bool) {
	best = 0
	bestPriority := g.effectivePriorityLocked(g.waiters[0], now)
	for i := 1; i < len(g.waiters); i++ {
		priority := g.effectivePriorityLocked(g.waiters[i], now)
		if priority < bestPriority || (priority == bestPriority && g.waiters[i].seq < g.waiters[best].seq) {
			best = i
			bestPriority = priority
		}
	}

	if g.fair.Enabled && g.lastTenant != "" && g.consecutiveWins >= g.fair.MaxConsecutivePerTenant {
		rotated := -1
		for i, waiter := range g.waiters {
			if g.effectivePriorityLocked(waiter, now) != bestPriority || waiter.tenantID == g.lastTenant {
				continue
			}
			if rotated < 0 || waiter.seq < g.waiters[rotated].seq {
				rotated = i
			}
		}
		if rotated >= 0 {
			best = rotated
			tenantRotated = true
		}
	}

	starvationPromoted = g.effectivePriorityLocked(g.waiters[best], now) < g.waiters[best].priority
	return best, starvationPromoted, tenantRotated
}

func (g *modelConcurrencyGate) effectivePriorityLocked(waiter *modelSlotWaiter, now time.Time) int {
	if !g.fair.Enabled || g.fair.StarvationAfter <= 0 || waiter.priority <= 0 {
		return waiter.priority
	}
	steps := int(now.Sub(waiter.enqueued) / g.fair.StarvationAfter)
	priority := waiter.priority - steps
	if priority < 0 {
		priority = 0
	}
	return priority
}

func (g *modelConcurrencyGate) recordAdmissionLocked(tenantID string) {
	if !g.fair.Enabled {
		return
	}
	if tenantID == g.lastTenant {
		g.consecutiveWins++
		return
	}
	g.lastTenant = tenantID
	g.consecutiveWins = 1
}

func normalizeTenantID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "__default__"
	}
	return value
}

func (g *modelConcurrencyGate) snapshot() (active, queued, limit int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active, len(g.waiters), g.limit
}

func backpressureError(class, provider, model string) error {
	err := fmt.Errorf("%w: slo=%s provider=%s model=%s", ErrAIBackpressure, class, provider, model)
	if class == AISLORealtime {
		return Permanent(err)
	}
	return Retryable(err)
}

func sloDeadlineError(class, provider, model string) error {
	err := fmt.Errorf("%w: slo=%s provider=%s model=%s", ErrAISLODeadline, class, provider, model)
	if class == AISLORealtime {
		return Permanent(err)
	}
	return Retryable(err)
}
