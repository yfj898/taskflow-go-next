package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestAIHTTPExecutorSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		var req aiGatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.TaskID != 42 || req.Prompt != "summarize this" || req.RetryCount != 1 {
			t.Fatalf("unexpected request: %+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(aiGatewayResponse{Result: "done"}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	exec := NewAIHTTPExecutor(server.URL, "secret", time.Second)
	result, err := exec.Execute(context.Background(), &domain.Task{
		ID: 42, Name: "ai", Type: AITaskType, Payload: "summarize this", RetryCount: 1,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "done" {
		t.Fatalf("result = %q, want done", result)
	}
}

func TestAIHTTPExecutorClassifiesHTTPFailures(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "bad request", status: http.StatusBadRequest, retryable: false},
		{name: "rate limited", status: http.StatusTooManyRequests, retryable: true},
		{name: "server failure", status: http.StatusServiceUnavailable, retryable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte("gateway error"))
			}))
			defer server.Close()

			exec := NewAIHTTPExecutor(server.URL, "", time.Second)
			_, err := exec.Execute(context.Background(), &domain.Task{ID: 1, Type: AITaskType})
			if err == nil {
				t.Fatal("Execute() error = nil")
			}
			if got := IsRetryable(err); got != tt.retryable {
				t.Fatalf("IsRetryable() = %v, want %v; err=%v", got, tt.retryable, err)
			}
		})
	}
}

func TestAIHTTPExecutorRequiresEndpoint(t *testing.T) {
	exec := NewAIHTTPExecutor("", "", time.Second)
	_, err := exec.Execute(context.Background(), &domain.Task{ID: 1, Type: AITaskType})
	if err == nil || IsRetryable(err) {
		t.Fatalf("expected permanent configuration error, got %v", err)
	}
}

func TestRouterUsesAIPlugin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(aiGatewayResponse{Result: "ai-result"}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	router := NewRouter(NewDemoExecutor())
	router.Register(AITaskType, NewAIHTTPExecutor(server.URL, "", time.Second))
	result, err := router.Execute(context.Background(), &domain.Task{ID: 7, Type: AITaskType, Payload: "hello"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "ai-result" {
		t.Fatalf("result = %q, want ai-result", result)
	}
}

func TestAIProviderFallbackOnRetryableFailure(t *testing.T) {
	primaryCalls := 0
	fallbackCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("temporary outage"))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{
			Result: "fallback-result",
			Model:  "mock-fallback-v1",
			Usage:  AIUsage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5, CostUSD: 0.001},
		})
	}))
	defer fallback.Close()

	exec := NewAIProviderExecutor(
		NewHTTPAIProvider("primary", primary.URL, "", time.Second),
		NewHTTPAIProvider("fallback", fallback.URL, "", time.Second),
	)
	result, err := exec.Execute(context.Background(), &domain.Task{ID: 9, Type: AITaskType, Payload: "hello"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "fallback-result" || primaryCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("result=%q primaryCalls=%d fallbackCalls=%d", result, primaryCalls, fallbackCalls)
	}
}

func TestAIProviderDoesNotFallbackOnPermanentFailure(t *testing.T) {
	fallbackCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid prompt"))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "should-not-run"})
	}))
	defer fallback.Close()

	exec := NewAIProviderExecutor(
		NewHTTPAIProvider("primary", primary.URL, "", time.Second),
		NewHTTPAIProvider("fallback", fallback.URL, "", time.Second),
	)
	_, err := exec.Execute(context.Background(), &domain.Task{ID: 10, Type: AITaskType})
	if err == nil || IsRetryable(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("fallback calls = %d, want 0", fallbackCalls)
	}
}

func TestHTTPAIProviderPropagatesTraceContext(t *testing.T) {
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(oldPropagator)

	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))

	var traceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
		_ = json.NewEncoder(w).Encode(aiGatewayResponse{Result: "ok"})
	}))
	defer server.Close()

	provider := NewHTTPAIProvider("primary", server.URL, "", time.Second)
	if _, err := provider.Generate(ctx, AIRequest{TaskID: 1, Prompt: "hello"}); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if traceparent == "" {
		t.Fatal("traceparent header was not propagated")
	}
}

func TestNormalizeAIUsage(t *testing.T) {
	got := normalizeAIUsage(AIUsage{InputTokens: 4, OutputTokens: 6})
	if got.TotalTokens != 10 {
		t.Fatalf("TotalTokens = %d, want 10", got.TotalTokens)
	}
}
