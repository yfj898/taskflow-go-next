package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type staticAIProvider struct {
	name   string
	info   AIProviderInfo
	result string
}

func (p *staticAIProvider) Name() string         { return p.name }
func (p *staticAIProvider) Info() AIProviderInfo { return p.info }
func (p *staticAIProvider) Generate(context.Context, AIRequest) (AIResponse, error) {
	return AIResponse{Result: p.result, Model: p.info.Model}, nil
}

func TestAIRoutingPolicyContextOverridesDefaultStrategy(t *testing.T) {
	exec := NewAIProviderExecutorWithRouting(
		AIRoutingConfig{Strategy: AIRoutingLatency, DefaultOutputTokens: 256},
		&staticAIProvider{
			name:   "expensive",
			info:   AIProviderInfo{Model: "a", InputCostPer1M: 10, OutputCostPer1M: 30},
			result: "expensive",
		},
		&staticAIProvider{
			name:   "cheap",
			info:   AIProviderInfo{Model: "b", InputCostPer1M: 0.1, OutputCostPer1M: 0.5},
			result: "cheap",
		},
	)
	ctx := WithAIRoutingPolicy(context.Background(), AIRoutingPolicySnapshot{
		Version:             9,
		Strategy:            AIRoutingCost,
		DefaultOutputTokens: 256,
	})
	task := &domain.Task{ID: 1, Type: AITaskType, Payload: "route me"}
	detailed, err := exec.ExecuteDetailed(ctx, task)
	if err != nil {
		t.Fatalf("ExecuteDetailed() error = %v", err)
	}
	if detailed.Result != "cheap" {
		t.Fatalf("result = %q, want cheap", detailed.Result)
	}
	var audit AIRouteDecisionAudit
	if err := json.Unmarshal([]byte(detailed.MetadataJSON), &audit); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if audit.PolicyVersion != 9 || audit.Strategy != AIRoutingCost || audit.SelectedProvider != "cheap" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
}

func TestReplayAIRouteDecisionCanCompareStrategies(t *testing.T) {
	audit := AIRouteDecisionAudit{
		Kind:             "ai_route",
		Strategy:         AIRoutingCost,
		SelectedProvider: "cheap-slow",
		Candidates: []AIRouteCandidateAudit{
			{
				Provider:         "fast-expensive",
				EstimatedCostUSD: 0.008,
				CostKnown:        true,
				EWMALatencyMS:    10,
				FailureRate:      0,
				Index:            0,
			},
			{
				Provider:         "cheap-slow",
				EstimatedCostUSD: 0.0001,
				CostKnown:        true,
				EWMALatencyMS:    200,
				FailureRate:      0,
				Index:            1,
			},
		},
	}
	metadata, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}

	costReplay, err := ReplayAIRouteDecision(string(metadata), AIRoutingCost)
	if err != nil {
		t.Fatalf("cost replay: %v", err)
	}
	if costReplay.ReplayProvider != "cheap-slow" || !costReplay.SameSelection {
		t.Fatalf("unexpected cost replay: %+v", costReplay)
	}

	latencyReplay, err := ReplayAIRouteDecision(string(metadata), AIRoutingLatency)
	if err != nil {
		t.Fatalf("latency replay: %v", err)
	}
	if latencyReplay.ReplayProvider != "fast-expensive" || latencyReplay.SameSelection {
		t.Fatalf("unexpected latency replay: %+v", latencyReplay)
	}
}

func TestAIRouteAuditCapturesFallbackAttempts(t *testing.T) {
	primary := &controlledAIProvider{name: "primary", retryableFailures: 1, result: "primary"}
	fallback := &controlledAIProvider{name: "fallback", result: "fallback"}
	exec := NewAIProviderExecutorWithRouting(
		AIRoutingConfig{Strategy: AIRoutingCost, CircuitFailures: 3, CircuitOpenFor: time.Second},
		primary,
		fallback,
	)
	detailed, err := exec.ExecuteDetailed(context.Background(), &domain.Task{ID: 2, Type: AITaskType, Payload: "hello"})
	if err != nil {
		t.Fatalf("ExecuteDetailed() error = %v", err)
	}
	var audit AIRouteDecisionAudit
	if err := json.Unmarshal([]byte(detailed.MetadataJSON), &audit); err != nil {
		t.Fatal(err)
	}
	if audit.FallbackCount != 1 || len(audit.Attempts) != 2 || audit.SelectedProvider != "fallback" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
}

type controlledAIProvider struct {
	name              string
	retryableFailures int
	result            string
}

func (p *controlledAIProvider) Name() string { return p.name }
func (p *controlledAIProvider) Generate(context.Context, AIRequest) (AIResponse, error) {
	if p.retryableFailures > 0 {
		p.retryableFailures--
		return AIResponse{}, Retryable(errors.New("temporary provider failure"))
	}
	return AIResponse{Result: p.result}, nil
}
