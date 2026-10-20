package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type sharedTestCoordinator struct {
	mu     sync.Mutex
	active map[string]int
}

func newSharedTestCoordinator() *sharedTestCoordinator {
	return &sharedTestCoordinator{active: make(map[string]int)}
}

func (c *sharedTestCoordinator) Acquire(ctx context.Context, provider, model string, limit int, _ time.Duration, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error) {
	started := time.Now()
	key := provider + ":" + model
	deadline := started.Add(maxWait)
	for {
		c.mu.Lock()
		active := c.active[key]
		if active < limit {
			c.active[key] = active + 1
			c.mu.Unlock()
			return time.Since(started), active + 1, &sharedTestLease{coordinator: c, key: key}, nil
		}
		c.mu.Unlock()
		if maxWait == 0 || !time.Now().Before(deadline) {
			return time.Since(started), active, nil, ErrAIGlobalConcurrency
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return time.Since(started), active, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type sharedTestLease struct {
	coordinator *sharedTestCoordinator
	key         string
	once        sync.Once
}

func (l *sharedTestLease) Release() {
	l.once.Do(func() {
		l.coordinator.mu.Lock()
		if l.coordinator.active[l.key] > 0 {
			l.coordinator.active[l.key]--
		}
		l.coordinator.mu.Unlock()
	})
}

func TestAIDistributedConcurrencySharedAcrossRouters(t *testing.T) {
	coordinator := newSharedTestCoordinator()
	block := make(chan struct{})
	primaryOne := &phase6Provider{
		name:    "shared-primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 4, GlobalMaxConcurrency: 1},
		block:   block,
		started: make(chan struct{}),
	}
	primaryTwo := &phase6Provider{
		name: "shared-primary",
		info: AIProviderInfo{Model: "model-a", MaxConcurrency: 4, GlobalMaxConcurrency: 1},
	}
	fallback := &phase6Provider{
		name: "fallback",
		info: AIProviderInfo{Model: "model-a", MaxConcurrency: 4},
	}
	config := AIRoutingConfig{
		SLO:                    phase6SLOConfig(40 * time.Millisecond),
		DistributedCoordinator: coordinator,
		DistributedLeaseTTL:    time.Second,
	}
	routerOne := NewAIProviderRouter(config, primaryOne)
	routerTwo := NewAIProviderRouter(config, primaryTwo, fallback)

	done := make(chan error, 1)
	go func() {
		_, err := routerOne.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLOStandard})
		done <- err
	}()
	select {
	case <-primaryOne.started:
	case <-time.After(time.Second):
		t.Fatal("first router did not enter provider")
	}

	execution, err := routerTwo.ExecuteDetailed(context.Background(), AITaskType, AIRequest{Prompt: "overflow", SLOClass: AISLORealtime})
	if err != nil {
		t.Fatalf("second router fallback failed: %v", err)
	}
	if execution.Audit.SelectedProvider != "fallback" {
		t.Fatalf("selected provider=%q, want fallback", execution.Audit.SelectedProvider)
	}
	if len(execution.Audit.Attempts) < 2 || execution.Audit.Attempts[0].Result != "global_backpressure" {
		t.Fatalf("unexpected attempts: %+v", execution.Audit.Attempts)
	}

	close(block)
	if err := <-done; err != nil {
		t.Fatalf("holder failed: %v", err)
	}
}

func TestAIAdaptiveLoadSheddingRejectsBatchAtOverload(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	config := AIRoutingConfig{
		SLO: phase6SLOConfig(time.Second),
		LoadShedding: AILoadSheddingConfig{
			Enabled:              true,
			DegradedQueueDepth:   1,
			OverloadedQueueDepth: 2,
		},
	}
	router := NewAIProviderRouter(config, provider)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-provider.started

	waiterDone := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "queued", SLOClass: AISLOBatch})
			waiterDone <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, queued, _ := router.providers[0].gateFor("model-a").snapshot()
		if queued >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("batch waiters did not queue")
		}
		time.Sleep(time.Millisecond)
	}

	_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "shed-me", SLOClass: AISLOBatch})
	if err == nil || !errors.Is(err, ErrAILoadShed) || !IsRetryable(err) {
		t.Fatalf("load shed error=%v retryable=%v", err, IsRetryable(err))
	}

	close(provider.block)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := <-waiterDone; err != nil {
			t.Fatalf("queued request failed: %v", err)
		}
	}
}
