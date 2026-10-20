package executor

import (
	"context"
	"testing"
	"time"
)

func phase9Router(provider AIProvider, starvation time.Duration, maxConsecutive int) *AIProviderRouter {
	return NewAIProviderRouter(AIRoutingConfig{
		SLO: phase6SLOConfig(2 * time.Second),
		FairScheduling: AIFairSchedulingConfig{
			Enabled:                 true,
			MaxConsecutivePerTenant: maxConsecutive,
			StarvationAfter:         starvation,
		},
	}, provider)
}

func TestAITenantFairSchedulingRotatesWithinSameSLO(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := phase9Router(provider, time.Second, 1)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", TenantID: "tenant-a", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-provider.started

	done := make(chan error, 3)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "a-1", TenantID: "tenant-a", SLOClass: AISLOStandard})
		done <- err
	}()
	time.Sleep(15 * time.Millisecond)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "a-2", TenantID: "tenant-a", SLOClass: AISLOStandard})
		done <- err
	}()
	time.Sleep(15 * time.Millisecond)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "b-1", TenantID: "tenant-b", SLOClass: AISLOStandard})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(provider.block)

	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	order := provider.executionOrder()
	if len(order) != 4 || order[0] != "holder" || order[1] != "b-1" {
		t.Fatalf("execution order = %v, want tenant-b rotated immediately after tenant-a holder", order)
	}
}

func TestAITenantStarvationAgingPromotesBatch(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := phase9Router(provider, 25*time.Millisecond, 100)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", TenantID: "tenant-a", SLOClass: AISLORealtime})
		holderDone <- err
	}()
	<-provider.started

	batchDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "old-batch", TenantID: "tenant-b", SLOClass: AISLOBatch})
		batchDone <- err
	}()
	// Batch starts at priority 2. Two starvation intervals promote it to the
	// same effective priority as realtime.
	time.Sleep(65 * time.Millisecond)
	realtimeDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "new-realtime", TenantID: "tenant-a", SLOClass: AISLORealtime})
		realtimeDone <- err
	}()
	time.Sleep(15 * time.Millisecond)
	close(provider.block)

	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if err := <-batchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-realtimeDone; err != nil {
		t.Fatal(err)
	}
	order := provider.executionOrder()
	if len(order) != 3 || order[1] != "old-batch" || order[2] != "new-realtime" {
		t.Fatalf("execution order = %v, want aged batch before new realtime", order)
	}
}

func TestAIConcurrencyGateReportsQueuedStateImmediately(t *testing.T) {
	gate := newModelConcurrencyGate(1, AIFairSchedulingConfig{})
	_, releaseHolder, err := gate.acquire(context.Background(), 1, "tenant-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	type queueState struct {
		active int
		queued int
		limit  int
	}
	observed := make(chan queueState, 1)
	waiterDone := make(chan error, 1)
	go func() {
		_, releaseWaiter, acquireErr := gate.acquireObserved(
			context.Background(),
			1,
			"tenant-b",
			time.Second,
			func(active, queued, limit int) {
				observed <- queueState{active: active, queued: queued, limit: limit}
			},
		)
		if acquireErr == nil {
			releaseWaiter()
		}
		waiterDone <- acquireErr
	}()

	select {
	case state := <-observed:
		if state.active != 1 || state.queued != 1 || state.limit != 1 {
			t.Fatalf("queue state = %+v, want active=1 queued=1 limit=1", state)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("queue observer did not run while waiter was blocked")
	}

	active, queued, limit := gate.snapshot()
	if active != 1 || queued != 1 || limit != 1 {
		t.Fatalf("gate snapshot = active=%d queued=%d limit=%d, want 1/1/1", active, queued, limit)
	}

	releaseHolder()
	if err := <-waiterDone; err != nil {
		t.Fatal(err)
	}
}
