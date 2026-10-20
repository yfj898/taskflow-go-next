package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const maxAIResponseBytes = 1 << 20

type AIRequest struct {
	TaskID       uint64
	Name         string
	Prompt       string
	RetryCount   int
	TenantID     string
	Model        string
	Capability   string
	MaxCostUSD   float64
	Strategy     string
	SLOClass     string
	DisableHedge bool
}

type AIUsage struct {
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	TotalTokens  int     `json:"total_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

type AIResponse struct {
	Result   string  `json:"result"`
	Provider string  `json:"provider,omitempty"`
	Model    string  `json:"model,omitempty"`
	Usage    AIUsage `json:"usage,omitempty"`
}

type AIProvider interface {
	Name() string
	Generate(ctx context.Context, req AIRequest) (AIResponse, error)
}

type AIProviderInfo struct {
	Model                string
	Capabilities         []string
	InputCostPer1M       float64
	OutputCostPer1M      float64
	MaxConcurrency       int
	GlobalMaxConcurrency int
}

type AIProviderWithInfo interface {
	AIProvider
	Info() AIProviderInfo
}

type HTTPAIProvider struct {
	name        string
	endpoint    string
	bearerToken string
	client      *http.Client
	info        AIProviderInfo
}

type AIHTTPExecutor struct {
	router *AIProviderRouter
}

type aiGatewayRequest struct {
	TaskID     uint64 `json:"task_id"`
	Name       string `json:"name"`
	Prompt     string `json:"prompt"`
	RetryCount int    `json:"retry_count"`
}

type aiGatewayResponse = AIResponse

func NewHTTPAIProvider(name, endpoint, bearerToken string, timeout time.Duration) *HTTPAIProvider {
	return NewHTTPAIProviderWithInfo(name, endpoint, bearerToken, timeout, AIProviderInfo{})
}

func NewHTTPAIProviderWithInfo(name, endpoint, bearerToken string, timeout time.Duration, info AIProviderInfo) *HTTPAIProvider {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "primary"
	}
	return &HTTPAIProvider{
		name:        name,
		endpoint:    strings.TrimSpace(endpoint),
		bearerToken: strings.TrimSpace(bearerToken),
		client:      &http.Client{Timeout: timeout},
		info:        normalizeProviderInfo(info),
	}
}

func (p *HTTPAIProvider) Name() string         { return p.name }
func (p *HTTPAIProvider) Info() AIProviderInfo { return p.info }

func NewAIProviderExecutor(providers ...AIProvider) *AIHTTPExecutor {
	return NewAIProviderExecutorWithRouting(AIRoutingConfig{}, providers...)
}

func NewAIProviderExecutorWithRouting(routing AIRoutingConfig, providers ...AIProvider) *AIHTTPExecutor {
	filtered := make([]AIProvider, 0, len(providers))
	for _, provider := range providers {
		if provider != nil {
			filtered = append(filtered, provider)
		}
	}
	return &AIHTTPExecutor{router: NewAIProviderRouter(routing, filtered...)}
}

// NewAIHTTPExecutor preserves the Phase 1 API while routing through the new
// provider abstraction introduced in Phase 2.
func NewAIHTTPExecutor(endpoint, bearerToken string, timeout time.Duration) *AIHTTPExecutor {
	return NewAIProviderExecutor(NewHTTPAIProvider("primary", endpoint, bearerToken, timeout))
}

func (e *AIHTTPExecutor) Execute(ctx context.Context, task *domain.Task) (string, error) {
	result, err := e.ExecuteDetailed(ctx, task)
	return result.Result, err
}

func (e *AIHTTPExecutor) ExecuteDetailed(ctx context.Context, task *domain.Task) (ExecutionResult, error) {
	if task == nil {
		return ExecutionResult{}, Permanent(errors.New("AI task is nil"))
	}
	if e.router == nil || e.router.ProviderCount() == 0 {
		return ExecutionResult{}, Permanent(errors.New("AI provider is not configured"))
	}

	req, err := parseAIRequest(task)
	if err != nil {
		return ExecutionResult{}, err
	}
	execution, execErr := e.router.ExecuteDetailed(ctx, task.Type, req)
	metadata, marshalErr := json.Marshal(execution.Audit)
	if marshalErr != nil && execErr == nil {
		return ExecutionResult{}, Permanent(fmt.Errorf("encode AI route audit: %w", marshalErr))
	}
	metadataJSON := string(metadata)
	task.ExecutionMetadata = metadataJSON
	return ExecutionResult{Result: execution.Result, MetadataJSON: metadataJSON}, execErr
}

func (p *HTTPAIProvider) Generate(ctx context.Context, request AIRequest) (AIResponse, error) {
	if p.endpoint == "" {
		return AIResponse{}, Permanent(errors.New("AI_ENDPOINT is not configured"))
	}

	body, err := json.Marshal(aiGatewayRequest{
		TaskID: request.TaskID, Name: request.Name, Prompt: request.Prompt, RetryCount: request.RetryCount,
	})
	if err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("encode AI gateway request: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("build AI gateway request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	if p.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.bearerToken)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return AIResponse{}, ctx.Err()
		}
		return AIResponse{}, Retryable(fmt.Errorf("AI gateway request failed: %w", err))
	}
	defer resp.Body.Close()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAIResponseBytes+1))
	if readErr != nil {
		return AIResponse{}, Retryable(fmt.Errorf("read AI gateway response: %w", readErr))
	}
	if len(responseBody) > maxAIResponseBytes {
		return AIResponse{}, Permanent(fmt.Errorf("AI gateway response exceeds %d bytes", maxAIResponseBytes))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("AI gateway HTTP %d: %s", resp.StatusCode, compactAIError(responseBody))
		if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return AIResponse{}, Retryable(err)
		}
		return AIResponse{}, Permanent(err)
	}

	var decoded aiGatewayResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("decode AI gateway response: %w", err))
	}
	if strings.TrimSpace(decoded.Result) == "" {
		return AIResponse{}, Permanent(errors.New("AI gateway response missing result"))
	}
	return decoded, nil
}

func normalizeAIUsage(usage AIUsage) AIUsage {
	if usage.InputTokens < 0 {
		usage.InputTokens = 0
	}
	if usage.OutputTokens < 0 {
		usage.OutputTokens = 0
	}
	if usage.TotalTokens <= 0 {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	if usage.CostUSD < 0 {
		usage.CostUSD = 0
	}
	return usage
}

func recordAIUsage(provider string, usage AIUsage) {
	if usage.InputTokens > 0 {
		appmetrics.AITokens.WithLabelValues(provider, "input").Add(float64(usage.InputTokens))
	}
	if usage.OutputTokens > 0 {
		appmetrics.AITokens.WithLabelValues(provider, "output").Add(float64(usage.OutputTokens))
	}
	if usage.TotalTokens > 0 {
		appmetrics.AITokens.WithLabelValues(provider, "total").Add(float64(usage.TotalTokens))
	}
	if usage.CostUSD > 0 {
		appmetrics.AICostUSD.WithLabelValues(provider).Add(usage.CostUSD)
	}
}

func compactAIError(body []byte) string {
	message := strings.Join(strings.Fields(string(body)), " ")
	if len(message) > 256 {
		message = message[:256] + "..."
	}
	if message == "" {
		return http.StatusText(http.StatusBadGateway)
	}
	return message
}
