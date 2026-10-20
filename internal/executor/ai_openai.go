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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type OpenAICompatibleProvider struct {
	name        string
	endpoint    string
	bearerToken string
	client      *http.Client
	info        AIProviderInfo
}

type openAIChatRequest struct {
	Model    string              `json:"model"`
	Messages []openAIChatMessage `json:"messages"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message openAIChatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

func NewOpenAICompatibleProvider(name, endpoint, bearerToken string, timeout time.Duration, info AIProviderInfo) *OpenAICompatibleProvider {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "openai-compatible"
	}
	return &OpenAICompatibleProvider{
		name:        name,
		endpoint:    resolveOpenAIChatEndpoint(endpoint),
		bearerToken: strings.TrimSpace(bearerToken),
		client:      &http.Client{Timeout: timeout},
		info:        normalizeProviderInfo(info),
	}
}

func (p *OpenAICompatibleProvider) Name() string         { return p.name }
func (p *OpenAICompatibleProvider) Info() AIProviderInfo { return p.info }

func (p *OpenAICompatibleProvider) Generate(ctx context.Context, request AIRequest) (AIResponse, error) {
	if p.endpoint == "" {
		return AIResponse{}, Permanent(errors.New("OpenAI-compatible endpoint is not configured"))
	}
	model := firstNonEmpty(request.Model, p.info.Model)
	if model == "" {
		return AIResponse{}, Permanent(errors.New("OpenAI-compatible provider requires a model"))
	}

	body, err := json.Marshal(openAIChatRequest{
		Model:    model,
		Messages: []openAIChatMessage{{Role: "user", Content: request.Prompt}},
	})
	if err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("encode OpenAI-compatible request: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("build OpenAI-compatible request: %w", err))
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
		return AIResponse{}, Retryable(fmt.Errorf("OpenAI-compatible request failed: %w", err))
	}
	defer resp.Body.Close()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAIResponseBytes+1))
	if readErr != nil {
		return AIResponse{}, Retryable(fmt.Errorf("read OpenAI-compatible response: %w", readErr))
	}
	if len(responseBody) > maxAIResponseBytes {
		return AIResponse{}, Permanent(fmt.Errorf("OpenAI-compatible response exceeds %d bytes", maxAIResponseBytes))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return AIResponse{}, classifyAIHTTPStatus("OpenAI-compatible", resp.StatusCode, responseBody)
	}

	var decoded openAIChatResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return AIResponse{}, Permanent(fmt.Errorf("decode OpenAI-compatible response: %w", err))
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return AIResponse{}, Permanent(errors.New("OpenAI-compatible response missing assistant content"))
	}
	usage := AIUsage{
		InputTokens:  decoded.Usage.PromptTokens,
		OutputTokens: decoded.Usage.CompletionTokens,
		TotalTokens:  decoded.Usage.TotalTokens,
	}
	return AIResponse{
		Result:   decoded.Choices[0].Message.Content,
		Provider: p.name,
		Model:    firstNonEmpty(decoded.Model, model),
		Usage:    usage,
	}, nil
}

func resolveOpenAIChatEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" || strings.HasSuffix(endpoint, "/chat/completions") {
		return endpoint
	}
	if strings.HasSuffix(endpoint, "/v1") {
		return endpoint + "/chat/completions"
	}
	return endpoint + "/v1/chat/completions"
}

func classifyAIHTTPStatus(prefix string, statusCode int, body []byte) error {
	err := fmt.Errorf("%s HTTP %d: %s", prefix, statusCode, compactAIError(body))
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		return Retryable(err)
	}
	return Permanent(err)
}
