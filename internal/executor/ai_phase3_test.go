package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func TestOpenAICompatibleProviderChatCompletions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		var req openAIChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Model != "mock-model" || len(req.Messages) != 1 || req.Messages[0].Content != "hello" {
			t.Fatalf("unexpected request: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock-model",
			"choices": []any{map[string]any{
				"message": map[string]any{"role": "assistant", "content": "openai:hello"},
			}},
			"usage": map[string]any{"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
		})
	}))
	defer server.Close()

	provider := NewOpenAICompatibleProvider("openai", server.URL, "secret", time.Second, AIProviderInfo{Model: "mock-model"})
	resp, err := provider.Generate(context.Background(), AIRequest{Prompt: "hello"})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if resp.Result != "openai:hello" || resp.Model != "mock-model" || resp.Usage.TotalTokens != 5 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestAIRoutingCostStrategyChoosesCheaperProvider(t *testing.T) {
	expensiveCalls := 0
	cheapCalls := 0
	expensive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		expensiveCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "expensive"})
	}))
	defer expensive.Close()
	cheap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cheapCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "cheap"})
	}))
	defer cheap.Close()

	exec := NewAIProviderExecutorWithRouting(
		AIRoutingConfig{Strategy: AIRoutingCost},
		NewHTTPAIProviderWithInfo("expensive", expensive.URL, "", time.Second, AIProviderInfo{
			InputCostPer1M: 10, OutputCostPer1M: 30,
		}),
		NewHTTPAIProviderWithInfo("cheap", cheap.URL, "", time.Second, AIProviderInfo{
			InputCostPer1M: 0.1, OutputCostPer1M: 0.5,
		}),
	)
	result, err := exec.Execute(context.Background(), &domain.Task{ID: 1, Type: AITaskType, Payload: "hello"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "cheap" || cheapCalls != 1 || expensiveCalls != 0 {
		t.Fatalf("result=%q cheapCalls=%d expensiveCalls=%d", result, cheapCalls, expensiveCalls)
	}
}

func TestAIRoutingCapabilityFiltersProviders(t *testing.T) {
	textCalls := 0
	reasoningCalls := 0
	textServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		textCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "text"})
	}))
	defer textServer.Close()
	reasoningServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reasoningCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "reasoning"})
	}))
	defer reasoningServer.Close()

	exec := NewAIProviderExecutorWithRouting(
		AIRoutingConfig{Strategy: AIRoutingBalanced},
		NewHTTPAIProviderWithInfo("text", textServer.URL, "", time.Second, AIProviderInfo{Capabilities: []string{"text"}}),
		NewHTTPAIProviderWithInfo("reasoning", reasoningServer.URL, "", time.Second, AIProviderInfo{Capabilities: []string{"text", "reasoning"}}),
	)
	result, err := exec.Execute(context.Background(), &domain.Task{
		ID: 2, Type: AITaskType, Payload: `{"prompt":"solve","capability":"reasoning"}`,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "reasoning" || reasoningCalls != 1 || textCalls != 0 {
		t.Fatalf("result=%q reasoningCalls=%d textCalls=%d", result, reasoningCalls, textCalls)
	}
}

func TestAIRoutingProviderCircuitSkipsRepeatedFailure(t *testing.T) {
	primaryCalls := 0
	fallbackCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("down"))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "fallback"})
	}))
	defer fallback.Close()

	exec := NewAIProviderExecutorWithRouting(
		AIRoutingConfig{Strategy: AIRoutingCost, CircuitFailures: 1, CircuitOpenFor: time.Minute},
		NewHTTPAIProviderWithInfo("primary", primary.URL, "", time.Second, AIProviderInfo{InputCostPer1M: 0.1, OutputCostPer1M: 0.1}),
		NewHTTPAIProviderWithInfo("fallback", fallback.URL, "", time.Second, AIProviderInfo{InputCostPer1M: 10, OutputCostPer1M: 10}),
	)
	for i := 0; i < 2; i++ {
		result, err := exec.Execute(context.Background(), &domain.Task{ID: uint64(i + 1), Type: AITaskType, Payload: "hello"})
		if err != nil || result != "fallback" {
			t.Fatalf("run %d result=%q err=%v", i, result, err)
		}
	}
	if primaryCalls != 1 || fallbackCalls != 2 {
		t.Fatalf("primaryCalls=%d fallbackCalls=%d", primaryCalls, fallbackCalls)
	}
}

func TestParseAIRequestRejectsUnknownStrategy(t *testing.T) {
	_, err := parseAIRequest(&domain.Task{Payload: `{"prompt":"hello","routing_strategy":"random"}`})
	if err == nil || IsRetryable(err) {
		t.Fatalf("expected permanent validation error, got %v", err)
	}
}
