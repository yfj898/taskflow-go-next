package executor

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

const AITaskType = "ai_generate"

// Router keeps optional task-type plugins outside the queue/worker core. Types
// without a dedicated plugin continue to use the fallback executor.
type Router struct {
	fallback Executor
	handlers map[string]Executor
}

func NewRouter(fallback Executor) *Router {
	return &Router{fallback: fallback, handlers: make(map[string]Executor)}
}

func (r *Router) Register(taskType string, exec Executor) {
	taskType = strings.ToLower(strings.TrimSpace(taskType))
	if taskType == "" || exec == nil {
		return
	}
	r.handlers[taskType] = exec
}

func (r *Router) Execute(ctx context.Context, task *domain.Task) (string, error) {
	result, err := r.ExecuteDetailed(ctx, task)
	return result.Result, err
}

func (r *Router) ExecuteDetailed(ctx context.Context, task *domain.Task) (ExecutionResult, error) {
	if task != nil {
		if exec, ok := r.handlers[strings.ToLower(strings.TrimSpace(task.Type))]; ok {
			return RunDetailed(exec, ctx, task)
		}
		if strings.EqualFold(strings.TrimSpace(task.Type), domain.VerificationTaskType) {
			return ExecutionResult{}, Permanent(errors.New("verification executor is not registered"))
		}
	}
	return RunDetailed(r.fallback, ctx, task)
}

func NewTaskExecutor(aiEndpoint, aiBearerToken string, aiTimeout time.Duration) Executor {
	return NewTaskExecutorWithProviders(aiTimeout, AIProviderConfig{
		Name: "primary", Endpoint: aiEndpoint, BearerToken: aiBearerToken,
	})
}

type AIProviderConfig struct {
	Name                 string
	Kind                 string
	Endpoint             string
	BearerToken          string
	Model                string
	Capabilities         []string
	InputCostPer1M       float64
	OutputCostPer1M      float64
	MaxConcurrency       int
	GlobalMaxConcurrency int
}

func NewTaskExecutorWithProviders(aiTimeout time.Duration, configs ...AIProviderConfig) Executor {
	return NewTaskExecutorWithRouting(aiTimeout, AIRoutingConfig{}, configs...)
}

func NewTaskExecutorWithRouting(aiTimeout time.Duration, routing AIRoutingConfig, configs ...AIProviderConfig) Executor {
	router := NewRouter(NewDemoExecutor())
	providers := make([]AIProvider, 0, len(configs))
	for _, cfg := range configs {
		if strings.TrimSpace(cfg.Endpoint) == "" {
			continue
		}
		info := AIProviderInfo{
			Model:                cfg.Model,
			Capabilities:         cfg.Capabilities,
			InputCostPer1M:       cfg.InputCostPer1M,
			OutputCostPer1M:      cfg.OutputCostPer1M,
			MaxConcurrency:       cfg.MaxConcurrency,
			GlobalMaxConcurrency: cfg.GlobalMaxConcurrency,
		}
		switch strings.ToLower(strings.TrimSpace(cfg.Kind)) {
		case "openai", "openai-compatible", "openai_compatible":
			providers = append(providers, NewOpenAICompatibleProvider(cfg.Name, cfg.Endpoint, cfg.BearerToken, aiTimeout, info))
		default:
			providers = append(providers, NewHTTPAIProviderWithInfo(cfg.Name, cfg.Endpoint, cfg.BearerToken, aiTimeout, info))
		}
	}
	if len(providers) == 0 {
		providers = append(providers, NewHTTPAIProvider("primary", "", "", aiTimeout))
	}
	router.Register(AITaskType, NewAIProviderExecutorWithRouting(routing, providers...))
	return router
}

func ParseAICapabilities(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' '
	})
}
