package executor

import (
	"context"
	"sync"
	"testing"
	"time"
)

type phase8Provider struct {
	name     string
	info     AIProviderInfo
	delay    time.Duration
	result   string
	mu       sync.Mutex
	calls    int
	canceled int
}

func (p *phase8Provider) Name() string         { return p.name }
func (p *phase8Provider) Info() AIProviderInfo { return p.info }

func (p *phase8Provider) Generate(ctx context.Context, _ AIRequest) (AIResponse, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		p.mu.Lock()
		p.canceled++
		p.mu.Unlock()
		return AIResponse{}, ctx.Err()
	case <-timer.C:
		return AIResponse{
			Result: p.result,
			Model:  p.info.Model,
			Usage: AIUsage{
				InputTokens:  2,
				OutputTokens: 3,
				TotalTokens:  5,
				CostUSD:      0.001,
			},
		}, nil
	}
}

func (p *phase8Provider) snapshot() (calls, canceled int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.canceled
}

func phase8Router(primary, secondary AIProvider, hedge AIHedgeConfig) *AIProviderRouter {
	return NewAIProviderRouter(AIRoutingConfig{
		Strategy: AIRoutingLatency,
		SLO: AISLOConfig{
			Realtime: AISLOPolicy{QueueWait: 100 * time.Millisecond, Timeout: time.Second},
			Standard: AISLOPolicy{QueueWait: 100 * time.Millisecond, Timeout: time.Second},
			Batch:    AISLOPolicy{QueueWait: 100 * time.Millisecond, Timeout: time.Second},
		},
		Hedge: hedge,
	}, primary, secondary)
}

func TestAIHedgeSecondaryWinsTailLatency(t *testing.T) {
	primary := &phase8Provider{
		name:   "slow-primary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1, OutputCostPer1M: 1},
		delay:  200 * time.Millisecond,
		result: "slow",
	}
	secondary := &phase8Provider{
		name:   "fast-secondary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1, OutputCostPer1M: 1},
		delay:  15 * time.Millisecond,
		result: "fast",
	}
	router := phase8Router(primary, secondary, AIHedgeConfig{
		Enabled:         true,
		Delay:           25 * time.Millisecond,
		CancelWait:      100 * time.Millisecond,
		MaxExtraCostUSD: 0.01,
	})

	started := time.Now()
	execution, err := router.ExecuteDetailed(context.Background(), AITaskType, AIRequest{
		Prompt:   "hello",
		SLOClass: AISLORealtime,
	})
	if err != nil {
		t.Fatalf("ExecuteDetailed: %v", err)
	}
	if execution.Result != "fast" {
		t.Fatalf("result=%q", execution.Result)
	}
	if time.Since(started) >= 150*time.Millisecond {
		t.Fatalf("hedge did not reduce tail latency: %s", time.Since(started))
	}
	if !execution.Audit.HedgeTriggered || execution.Audit.HedgeWinner != "fast-secondary" {
		t.Fatalf("hedge audit=%+v", execution.Audit)
	}
	primaryCalls, primaryCanceled := primary.snapshot()
	secondaryCalls, _ := secondary.snapshot()
	if primaryCalls != 1 || secondaryCalls != 1 || primaryCanceled != 1 {
		t.Fatalf("primary calls/canceled=%d/%d secondary calls=%d", primaryCalls, primaryCanceled, secondaryCalls)
	}
	if primary.breakerStateForTest(router) == "open" {
		t.Fatal("hedge cancellation must not open provider circuit")
	}
}

func TestAIHedgeFastPrimaryDoesNotLaunchSecondary(t *testing.T) {
	primary := &phase8Provider{
		name:   "fast-primary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1, OutputCostPer1M: 1},
		delay:  5 * time.Millisecond,
		result: "primary",
	}
	secondary := &phase8Provider{
		name:   "secondary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1, OutputCostPer1M: 1},
		delay:  5 * time.Millisecond,
		result: "secondary",
	}
	router := phase8Router(primary, secondary, AIHedgeConfig{Enabled: true, Delay: 50 * time.Millisecond, MaxExtraCostUSD: 0.01})
	execution, err := router.ExecuteDetailed(context.Background(), AITaskType, AIRequest{Prompt: "hello", SLOClass: AISLORealtime})
	if err != nil || execution.Result != "primary" {
		t.Fatalf("execution=%+v err=%v", execution, err)
	}
	_, secondaryCanceled := secondary.snapshot()
	secondaryCalls, _ := secondary.snapshot()
	if secondaryCalls != 0 || secondaryCanceled != 0 || execution.Audit.HedgeTriggered {
		t.Fatalf("secondary calls=%d canceled=%d hedge=%v", secondaryCalls, secondaryCanceled, execution.Audit.HedgeTriggered)
	}
}

func TestAIHedgeCostGuardDisablesDuplicateCall(t *testing.T) {
	primary := &phase8Provider{
		name:   "primary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1, OutputCostPer1M: 1},
		delay:  40 * time.Millisecond,
		result: "primary",
	}
	secondary := &phase8Provider{
		name:   "expensive-secondary",
		info:   AIProviderInfo{Model: "model-a", MaxConcurrency: 2, InputCostPer1M: 1000, OutputCostPer1M: 1000},
		delay:  5 * time.Millisecond,
		result: "secondary",
	}
	router := phase8Router(primary, secondary, AIHedgeConfig{Enabled: true, Delay: 5 * time.Millisecond, MaxExtraCostUSD: 0.00001})
	execution, err := router.ExecuteDetailed(context.Background(), AITaskType, AIRequest{Prompt: "hello", SLOClass: AISLORealtime})
	if err != nil || execution.Result != "primary" {
		t.Fatalf("execution=%+v err=%v", execution, err)
	}
	secondaryCalls, _ := secondary.snapshot()
	if secondaryCalls != 0 {
		t.Fatalf("cost guard allowed %d secondary calls", secondaryCalls)
	}
}

func (p *phase8Provider) breakerStateForTest(router *AIProviderRouter) string {
	for _, runtime := range router.providers {
		if runtime.provider == p {
			return runtime.breaker.State()
		}
	}
	return "missing"
}
