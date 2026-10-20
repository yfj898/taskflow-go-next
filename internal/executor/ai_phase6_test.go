package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type phase6Provider struct {
	name    string
	info    AIProviderInfo
	block   chan struct{}
	started chan struct{}
	once    sync.Once

	mu    sync.Mutex
	order []string
}

func (p *phase6Provider) Name() string         { return p.name }
func (p *phase6Provider) Info() AIProviderInfo { return p.info }
func (p *phase6Provider) Generate(ctx context.Context, req AIRequest) (AIResponse, error) {
	p.mu.Lock()
	p.order = append(p.order, req.Prompt)
	p.mu.Unlock()
	if req.Prompt == "holder" && p.block != nil {
		p.once.Do(func() { close(p.started) })
		select {
		case <-p.block:
		case <-ctx.Done():
			return AIResponse{}, ctx.Err()
		}
	}
	return AIResponse{Result: p.name + ":" + req.Prompt, Model: p.info.Model}, nil
}

func (p *phase6Provider) executionOrder() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

func phase6SLOConfig(queueWait time.Duration) AISLOConfig {
	return AISLOConfig{
		Realtime: AISLOPolicy{QueueWait: queueWait, Timeout: 5 * time.Second},
		Standard: AISLOPolicy{QueueWait: queueWait, Timeout: 5 * time.Second},
		Batch:    AISLOPolicy{QueueWait: queueWait, Timeout: 5 * time.Second},
	}
}

func TestAISLOPriorityRealtimeRunsBeforeQueuedBatch(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := NewAIProviderRouter(AIRoutingConfig{SLO: phase6SLOConfig(2 * time.Second)}, provider)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-provider.started

	batchDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "batch", SLOClass: AISLOBatch})
		batchDone <- err
	}()
	time.Sleep(25 * time.Millisecond)
	realtimeDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "realtime", SLOClass: AISLORealtime})
		realtimeDone <- err
	}()
	time.Sleep(25 * time.Millisecond)
	close(provider.block)

	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if err := <-realtimeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-batchDone; err != nil {
		t.Fatal(err)
	}
	order := provider.executionOrder()
	if len(order) != 3 || order[0] != "holder" || order[1] != "realtime" || order[2] != "batch" {
		t.Fatalf("execution order = %v, want [holder realtime batch]", order)
	}
}

func TestAIBackpressureFallsBackToAvailableProvider(t *testing.T) {
	primary := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	fallback := &phase6Provider{
		name:    "fallback",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		started: make(chan struct{}),
	}
	router := NewAIProviderRouter(AIRoutingConfig{SLO: phase6SLOConfig(40 * time.Millisecond)}, primary, fallback)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-primary.started

	execution, err := router.ExecuteDetailed(context.Background(), AITaskType, AIRequest{Prompt: "overflow", SLOClass: AISLORealtime})
	if err != nil {
		t.Fatalf("ExecuteDetailed() error = %v", err)
	}
	if execution.Result != "fallback:overflow" || execution.Audit.SelectedProvider != "fallback" {
		t.Fatalf("execution = %+v", execution)
	}
	if len(execution.Audit.Attempts) < 2 || execution.Audit.Attempts[0].Result != "backpressure" {
		t.Fatalf("attempts = %+v", execution.Audit.Attempts)
	}
	close(primary.block)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}

func TestAIRealtimeBackpressureFailsFastWhenNoFallback(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := NewAIProviderRouter(AIRoutingConfig{SLO: phase6SLOConfig(30 * time.Millisecond)}, provider)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-provider.started

	started := time.Now()
	_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "overflow", SLOClass: AISLORealtime})
	if err == nil || IsRetryable(err) || !errors.Is(err, ErrAIBackpressure) {
		t.Fatalf("error = %v retryable=%v", err, IsRetryable(err))
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatalf("realtime backpressure did not fail fast: %s", time.Since(started))
	}
	close(provider.block)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}

func TestAIRealtimeSLODeadlineIsPermanent(t *testing.T) {
	provider := &phase6Provider{
		name:    "primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := NewAIProviderRouter(AIRoutingConfig{
		SLO: AISLOConfig{
			Realtime: AISLOPolicy{QueueWait: time.Second, Timeout: 40 * time.Millisecond},
			Standard: AISLOPolicy{QueueWait: time.Second, Timeout: time.Second},
			Batch:    AISLOPolicy{QueueWait: time.Second, Timeout: time.Second},
		},
	}, provider)

	_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", SLOClass: AISLORealtime})
	if err == nil || IsRetryable(err) || !errors.Is(err, ErrAISLODeadline) {
		t.Fatalf("error = %v retryable=%v", err, IsRetryable(err))
	}
}

func TestAIPerModelConcurrencySeparatesDynamicModels(t *testing.T) {
	provider := &phase6Provider{
		name:    "dynamic",
		info:    AIProviderInfo{MaxConcurrency: 1},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	router := NewAIProviderRouter(AIRoutingConfig{SLO: phase6SLOConfig(time.Second)}, provider)

	holderDone := make(chan error, 1)
	go func() {
		_, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "holder", Model: "model-a", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	<-provider.started

	started := time.Now()
	result, err := router.Execute(context.Background(), AITaskType, AIRequest{Prompt: "other", Model: "model-b", SLOClass: AISLORealtime})
	if err != nil {
		t.Fatalf("model-b execution error = %v", err)
	}
	if result != "dynamic:other" {
		t.Fatalf("result = %q", result)
	}
	if time.Since(started) > 200*time.Millisecond {
		t.Fatalf("model-b was blocked by model-a gate: %s", time.Since(started))
	}
	close(provider.block)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}

func TestParseAIRequestRejectsUnknownSLOClass(t *testing.T) {
	_, err := parseAIRequest(&domain.Task{Payload: `{"prompt":"hello","slo_class":"urgent"}`})
	if err == nil || IsRetryable(err) {
		t.Fatalf("expected permanent validation error, got %v", err)
	}
}
