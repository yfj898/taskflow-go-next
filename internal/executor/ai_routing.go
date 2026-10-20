package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	AIRoutingBalanced    = "balanced"
	AIRoutingCost        = "cost"
	AIRoutingLatency     = "latency"
	AIRoutingReliability = "reliability"
)

type AIRoutingConfig struct {
	Strategy               string
	CircuitFailures        int
	CircuitOpenFor         time.Duration
	EWMAAlpha              float64
	DefaultOutputTokens    int
	SLO                    AISLOConfig
	FairScheduling         AIFairSchedulingConfig
	DistributedCoordinator DistributedConcurrencyCoordinator
	TenantCoordinator      DistributedTenantConcurrencyCoordinator
	TenantQuota            AITenantDistributedQuotaConfig
	DistributedLeaseTTL    time.Duration
	LoadShedding           AILoadSheddingConfig
	Hedge                  AIHedgeConfig
}

type AIHedgeConfig struct {
	Enabled         bool
	Delay           time.Duration
	CancelWait      time.Duration
	MaxExtraCostUSD float64
}

type AIRoutingPolicySnapshot struct {
	Version                 uint64 `json:"version"`
	Strategy                string `json:"strategy"`
	DefaultOutputTokens     int    `json:"default_output_tokens"`
	TenantQuotaManaged      bool   `json:"tenant_quota_managed"`
	TenantQuotaEnabled      bool   `json:"tenant_quota_enabled"`
	TenantQuotaDefaultLimit int    `json:"tenant_quota_default_limit"`
	TenantQuotaOverrides    string `json:"tenant_quota_overrides,omitempty"`
}

type aiRoutingPolicyContextKey struct{}

func WithAIRoutingPolicy(ctx context.Context, policy AIRoutingPolicySnapshot) context.Context {
	return context.WithValue(ctx, aiRoutingPolicyContextKey{}, policy)
}

func AIRoutingPolicyFromContext(ctx context.Context) (AIRoutingPolicySnapshot, bool) {
	if ctx == nil {
		return AIRoutingPolicySnapshot{}, false
	}
	policy, ok := ctx.Value(aiRoutingPolicyContextKey{}).(AIRoutingPolicySnapshot)
	return policy, ok
}

type aiTaskEnvelope struct {
	Prompt          *string `json:"prompt"`
	TenantID        string  `json:"tenant_id,omitempty"`
	Model           string  `json:"model,omitempty"`
	Capability      string  `json:"capability,omitempty"`
	MaxCostUSD      float64 `json:"max_cost_usd,omitempty"`
	RoutingStrategy string  `json:"routing_strategy,omitempty"`
	SLOClass        string  `json:"slo_class,omitempty"`
	DisableHedge    bool    `json:"disable_hedge,omitempty"`
}

type aiProviderRuntime struct {
	provider AIProvider
	info     AIProviderInfo
	breaker  *CircuitBreaker
	fair     AIFairSchedulingConfig

	gateMu sync.Mutex
	gates  map[string]*modelConcurrencyGate

	mu          sync.RWMutex
	calls       uint64
	failures    uint64
	ewmaLatency time.Duration
}

type AIProviderRouter struct {
	config    AIRoutingConfig
	providers []*aiProviderRuntime
}

type aiCandidate struct {
	runtime       *aiProviderRuntime
	score         float64
	estimatedCost float64
	costKnown     bool
	index         int
}

type AIRouteCandidateAudit struct {
	Provider         string  `json:"provider"`
	Model            string  `json:"model,omitempty"`
	Score            float64 `json:"score"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd,omitempty"`
	CostKnown        bool    `json:"cost_known"`
	EWMALatencyMS    float64 `json:"ewma_latency_ms"`
	FailureRate      float64 `json:"failure_rate"`
	CircuitState     string  `json:"circuit_state"`
	Index            int     `json:"index"`
}

type AIRouteAttemptAudit struct {
	Provider    string  `json:"provider"`
	Model       string  `json:"model,omitempty"`
	Result      string  `json:"result"`
	LatencyMS   float64 `json:"latency_ms"`
	QueueWaitMS float64 `json:"queue_wait_ms,omitempty"`
	Error       string  `json:"error,omitempty"`
}

type AIRouteDecisionAudit struct {
	Kind                   string                  `json:"kind"`
	PolicyVersion          uint64                  `json:"policy_version"`
	Strategy               string                  `json:"strategy"`
	SLOClass               string                  `json:"slo_class"`
	PressureState          string                  `json:"pressure_state,omitempty"`
	TenantID               string                  `json:"tenant_id,omitempty"`
	TenantFairScheduling   bool                    `json:"tenant_fair_scheduling,omitempty"`
	TenantMaxConsecutive   int                     `json:"tenant_max_consecutive,omitempty"`
	TenantStarvationMS     float64                 `json:"tenant_starvation_ms,omitempty"`
	TenantDistributedQuota bool                    `json:"tenant_distributed_quota,omitempty"`
	TenantConcurrencyLimit int                     `json:"tenant_concurrency_limit,omitempty"`
	TenantQuotaSource      string                  `json:"tenant_quota_source,omitempty"`
	RequestedModel         string                  `json:"requested_model,omitempty"`
	Capability             string                  `json:"capability,omitempty"`
	MaxCostUSD             float64                 `json:"max_cost_usd,omitempty"`
	DefaultOutputTokens    int                     `json:"default_output_tokens"`
	SelectedProvider       string                  `json:"selected_provider,omitempty"`
	SelectedModel          string                  `json:"selected_model,omitempty"`
	EstimatedCostUSD       float64                 `json:"estimated_cost_usd,omitempty"`
	ActualCostUSD          float64                 `json:"actual_cost_usd,omitempty"`
	InputTokens            int                     `json:"input_tokens,omitempty"`
	OutputTokens           int                     `json:"output_tokens,omitempty"`
	TotalTokens            int                     `json:"total_tokens,omitempty"`
	TotalLatencyMS         float64                 `json:"total_latency_ms"`
	FallbackCount          int                     `json:"fallback_count"`
	HedgeTriggered         bool                    `json:"hedge_triggered,omitempty"`
	HedgeDelayMS           float64                 `json:"hedge_delay_ms,omitempty"`
	HedgeWinner            string                  `json:"hedge_winner,omitempty"`
	HedgeExtraCostUSD      float64                 `json:"hedge_extra_cost_usd,omitempty"`
	Candidates             []AIRouteCandidateAudit `json:"candidates"`
	Attempts               []AIRouteAttemptAudit   `json:"attempts"`
}

type AIRouteExecution struct {
	Result string
	Audit  AIRouteDecisionAudit
}

type AIRouteReplay struct {
	Strategy         string `json:"strategy"`
	OriginalProvider string `json:"original_provider"`
	ReplayProvider   string `json:"replay_provider"`
	SameSelection    bool   `json:"same_selection"`
}

func NewAIProviderRouter(config AIRoutingConfig, providers ...AIProvider) *AIProviderRouter {
	config = normalizeRoutingConfig(config)
	runtimes := make([]*aiProviderRuntime, 0, len(providers))
	for _, provider := range providers {
		if provider == nil {
			continue
		}
		info := AIProviderInfo{}
		if withInfo, ok := provider.(AIProviderWithInfo); ok {
			info = withInfo.Info()
		}
		info = normalizeProviderInfo(info)
		runtimes = append(runtimes, &aiProviderRuntime{
			provider: provider,
			info:     info,
			breaker:  NewCircuitBreaker(config.CircuitFailures, config.CircuitOpenFor),
			fair:     config.FairScheduling,
			gates:    make(map[string]*modelConcurrencyGate),
		})
	}
	return &AIProviderRouter{config: config, providers: runtimes}
}

func (r *AIProviderRouter) ProviderCount() int {
	if r == nil {
		return 0
	}
	return len(r.providers)
}

func (r *AIProviderRouter) Execute(ctx context.Context, taskType string, req AIRequest) (string, error) {
	execution, err := r.ExecuteDetailed(ctx, taskType, req)
	return execution.Result, err
}

func (r *AIProviderRouter) ExecuteDetailed(ctx context.Context, taskType string, req AIRequest) (AIRouteExecution, error) {
	policy, hasPolicy := AIRoutingPolicyFromContext(ctx)
	strategy := normalizeRoutingStrategy(req.Strategy)
	if strategy == "" {
		if hasPolicy && normalizeRoutingStrategy(policy.Strategy) != "" {
			strategy = normalizeRoutingStrategy(policy.Strategy)
		} else {
			strategy = r.config.Strategy
		}
	}
	defaultOutputTokens := r.config.DefaultOutputTokens
	if hasPolicy && policy.DefaultOutputTokens > 0 {
		defaultOutputTokens = policy.DefaultOutputTokens
	}
	tenantQuota, tenantQuotaSource := r.tenantQuotaConfig(ctx)
	sloClass := normalizeSLOClass(req.SLOClass)
	if strings.TrimSpace(req.SLOClass) != "" && sloClass == "" {
		return AIRouteExecution{}, Permanent(fmt.Errorf("unsupported AI SLO class %q", req.SLOClass))
	}
	if sloClass == "" {
		sloClass = AISLOStandard
	}
	sloPolicy := r.config.SLO.policy(sloClass)
	pressureState := r.pressureState()
	var err error
	sloPolicy, err = applyLoadShedding(pressureState, sloClass, sloPolicy, r.config.SLO.Realtime)
	if err != nil {
		appmetrics.AISLORequests.WithLabelValues(sloClass, "load_shed").Inc()
		return AIRouteExecution{Audit: AIRouteDecisionAudit{
			Kind:          "ai_route",
			PolicyVersion: policy.Version,
			Strategy:      strategy,
			SLOClass:      sloClass,
			PressureState: pressureState,
		}}, err
	}
	sloCtx, cancelSLO := context.WithTimeout(ctx, sloPolicy.Timeout)
	defer cancelSLO()

	candidates, err := r.rankCandidates(req, strategy, defaultOutputTokens)
	audit := AIRouteDecisionAudit{
		Kind:                   "ai_route",
		PolicyVersion:          policy.Version,
		Strategy:               strategy,
		SLOClass:               sloClass,
		PressureState:          pressureState,
		TenantID:               req.TenantID,
		TenantFairScheduling:   r.config.FairScheduling.Enabled,
		TenantMaxConsecutive:   r.config.FairScheduling.MaxConsecutivePerTenant,
		TenantStarvationMS:     float64(r.config.FairScheduling.StarvationAfter) / float64(time.Millisecond),
		TenantDistributedQuota: tenantQuota.Enabled,
		TenantConcurrencyLimit: tenantQuota.limitFor(req.TenantID),
		TenantQuotaSource:      tenantQuotaSource,
		RequestedModel:         req.Model,
		Capability:             req.Capability,
		MaxCostUSD:             req.MaxCostUSD,
		DefaultOutputTokens:    defaultOutputTokens,
		Candidates:             make([]AIRouteCandidateAudit, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		latency, failureRate := candidate.runtime.snapshot()
		audit.Candidates = append(audit.Candidates, AIRouteCandidateAudit{
			Provider:         candidate.runtime.provider.Name(),
			Model:            firstNonEmpty(candidate.runtime.info.Model, req.Model),
			Score:            candidate.score,
			EstimatedCostUSD: candidate.estimatedCost,
			CostKnown:        candidate.costKnown,
			EWMALatencyMS:    float64(latency) / float64(time.Millisecond),
			FailureRate:      failureRate,
			CircuitState:     candidate.runtime.breaker.State(),
			Index:            candidate.index,
		})
	}
	if err != nil {
		appmetrics.AISLORequests.WithLabelValues(sloClass, "rejected").Inc()
		return AIRouteExecution{Audit: audit}, err
	}

	startedTotal := time.Now()
	var lastErr error
	var previousProvider string
	startIndex := 0
	if r.hedgeEligible(req, sloClass, pressureState, candidates) {
		hedged := r.executeHedgedPair(sloCtx, taskType, req, strategy, sloClass, sloPolicy, candidates, audit, startedTotal)
		audit = hedged.execution.Audit
		if hedged.success {
			return hedged.execution, nil
		}
		if hedged.attemptedCount > 0 {
			startIndex = hedged.attemptedCount
			lastErr = hedged.lastErr
			previousProvider = hedged.lastProvider
		}
	}
	for _, candidate := range candidates[startIndex:] {
		runtime := candidate.runtime
		providerName := runtime.provider.Name()
		modelName := firstNonEmpty(runtime.info.Model, req.Model, "unknown")
		gate := runtime.gateFor(modelName)
		if err := runtime.breaker.Allow(); err != nil {
			appmetrics.AIProviderCircuitSkips.WithLabelValues(providerName).Inc()
			audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
				Provider: providerName,
				Model:    runtime.info.Model,
				Result:   "circuit_open",
				Error:    err.Error(),
			})
			continue
		}

		active, queued, _ := gate.snapshot()
		appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
		appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
		if active >= runtime.info.MaxConcurrency {
			appmetrics.AIBackpressure.WithLabelValues(providerName, modelName, sloClass, "queued").Inc()
		}
		queueWait, releaseSlot, acquireErr := gate.acquireObserved(
			sloCtx,
			sloPolicy.Priority,
			req.TenantID,
			sloPolicy.QueueWait,
			func(active, queued, _ int) {
				appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
				appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
			},
		)
		appmetrics.AIModelQueueWait.WithLabelValues(providerName, modelName, sloClass).Observe(queueWait.Seconds())
		active, queued, _ = gate.snapshot()
		appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
		appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
		if acquireErr != nil {
			if ctx.Err() != nil {
				return AIRouteExecution{Audit: audit}, ctx.Err()
			}
			bpErr := backpressureError(sloClass, providerName, modelName)
			result := "backpressure"
			if errors.Is(acquireErr, context.DeadlineExceeded) && sloCtx.Err() != nil {
				bpErr = sloDeadlineError(sloClass, providerName, modelName)
				result = "slo_deadline"
			}
			lastErr = bpErr
			appmetrics.AIBackpressure.WithLabelValues(providerName, modelName, sloClass, result).Inc()
			audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
				Provider:    providerName,
				Model:       modelName,
				Result:      result,
				QueueWaitMS: float64(queueWait) / float64(time.Millisecond),
				Error:       bpErr.Error(),
			})
			audit.FallbackCount++
			previousProvider = providerName
			continue
		}

		var tenantLease DistributedConcurrencyLease
		remainingWait := sloPolicy.QueueWait - queueWait
		if remainingWait < 0 {
			remainingWait = 0
		}
		tenantWait, acquiredTenantLease, tenantOutcome, tenantErr := r.acquireTenantQuota(
			sloCtx,
			tenantQuota,
			providerName,
			modelName,
			req.TenantID,
			sloClass,
			remainingWait,
		)
		queueWait += tenantWait
		if tenantOutcome != "" {
			appmetrics.AITenantDistributedConcurrency.WithLabelValues(providerName, modelName, sloClass, tenantOutcome).Inc()
		}
		if tenantErr != nil {
			releaseSlot()
			active, queued, _ = gate.snapshot()
			appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
			appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
			if ctx.Err() != nil {
				return AIRouteExecution{Audit: audit}, ctx.Err()
			}
			lastErr = tenantErr
			audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
				Provider:    providerName,
				Model:       modelName,
				Result:      tenantOutcome,
				QueueWaitMS: float64(queueWait) / float64(time.Millisecond),
				Error:       tenantErr.Error(),
			})
			audit.FallbackCount++
			previousProvider = providerName
			continue
		}
		tenantLease = acquiredTenantLease

		var distributedLease DistributedConcurrencyLease
		globalActiveObserved := 0
		if r.config.DistributedCoordinator != nil && runtime.info.GlobalMaxConcurrency > 0 {
			appmetrics.AIGlobalConcurrencyLimit.WithLabelValues(providerName, modelName).Set(float64(runtime.info.GlobalMaxConcurrency))
			remainingWait := sloPolicy.QueueWait - queueWait
			if remainingWait < 0 {
				remainingWait = 0
			}
			globalWait, globalActive, lease, globalErr := r.config.DistributedCoordinator.Acquire(
				sloCtx,
				providerName,
				modelName,
				runtime.info.GlobalMaxConcurrency,
				r.config.DistributedLeaseTTL,
				remainingWait,
			)
			queueWait += globalWait
			globalActiveObserved = globalActive
			appmetrics.AIGlobalConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(globalActive))
			if globalErr != nil {
				if tenantLease != nil {
					tenantLease.Release()
				}
				releaseSlot()
				active, queued, _ = gate.snapshot()
				appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
				appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
				if ctx.Err() != nil {
					return AIRouteExecution{Audit: audit}, ctx.Err()
				}
				result := "coordinator_error"
				routeErr := Retryable(fmt.Errorf("distributed AI concurrency coordinator: %w", globalErr))
				if errors.Is(globalErr, ErrAIGlobalConcurrency) {
					result = "global_backpressure"
					routeErr = globalConcurrencyError(sloClass, providerName, modelName)
				} else if errors.Is(globalErr, context.DeadlineExceeded) && sloCtx.Err() != nil {
					result = "slo_deadline"
					routeErr = sloDeadlineError(sloClass, providerName, modelName)
				}
				lastErr = routeErr
				appmetrics.AIGlobalConcurrency.WithLabelValues(providerName, modelName, sloClass, result).Inc()
				audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
					Provider:    providerName,
					Model:       modelName,
					Result:      result,
					QueueWaitMS: float64(queueWait) / float64(time.Millisecond),
					Error:       routeErr.Error(),
				})
				audit.FallbackCount++
				previousProvider = providerName
				continue
			}
			distributedLease = lease
			appmetrics.AIGlobalConcurrency.WithLabelValues(providerName, modelName, sloClass, "acquired").Inc()
		}

		reason := "selected"
		if previousProvider != "" {
			reason = "fallback"
			appmetrics.AIFallbacks.WithLabelValues(previousProvider, providerName).Inc()
		}
		appmetrics.AIRouteSelections.WithLabelValues(providerName, strategy, reason).Inc()

		started := time.Now()
		providerCtx, span := otel.Tracer("taskflow/ai").Start(
			sloCtx,
			"ai.provider.generate",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("ai.provider", providerName),
				attribute.String("ai.routing.strategy", strategy),
				attribute.Float64("ai.routing.score", candidate.score),
				attribute.String("ai.request.model", req.Model),
				attribute.String("ai.request.capability", req.Capability),
				attribute.String("ai.slo_class", sloClass),
				attribute.Float64("ai.queue_wait_ms", float64(queueWait)/float64(time.Millisecond)),
				attribute.String("task.type", taskType),
			),
		)

		resp, callErr := runtime.provider.Generate(providerCtx, req)
		latency := time.Since(started)
		if distributedLease != nil {
			distributedLease.Release()
			if globalActiveObserved > 0 {
				globalActiveObserved--
			}
			appmetrics.AIGlobalConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(globalActiveObserved))
		}
		if tenantLease != nil {
			tenantLease.Release()
		}
		releaseSlot()
		active, queued, _ = gate.snapshot()
		appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
		appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
		appmetrics.AIProviderDuration.WithLabelValues(providerName).Observe(latency.Seconds())
		runtime.observe(latency, callErr != nil && IsRetryable(callErr), r.config.EWMAAlpha)

		if callErr == nil {
			runtime.breaker.Success()
			usage := normalizeAIUsage(resp.Usage)
			if usage.CostUSD == 0 {
				usage.CostUSD = actualProviderCost(runtime.info, usage)
			}
			appmetrics.AIProviderCalls.WithLabelValues(providerName, "success").Inc()
			recordAIUsage(providerName, usage)
			selectedModel := firstNonEmpty(resp.Model, runtime.info.Model, req.Model)
			span.SetAttributes(
				attribute.String("ai.model", selectedModel),
				attribute.Int("ai.input_tokens", usage.InputTokens),
				attribute.Int("ai.output_tokens", usage.OutputTokens),
				attribute.Int("ai.total_tokens", usage.TotalTokens),
				attribute.Float64("ai.cost_usd", usage.CostUSD),
			)
			span.End()
			audit.SelectedProvider = providerName
			audit.SelectedModel = selectedModel
			audit.EstimatedCostUSD = candidate.estimatedCost
			audit.ActualCostUSD = usage.CostUSD
			audit.InputTokens = usage.InputTokens
			audit.OutputTokens = usage.OutputTokens
			audit.TotalTokens = usage.TotalTokens
			audit.TotalLatencyMS = float64(time.Since(startedTotal)) / float64(time.Millisecond)
			audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
				Provider:    providerName,
				Model:       selectedModel,
				Result:      "success",
				LatencyMS:   float64(latency) / float64(time.Millisecond),
				QueueWaitMS: float64(queueWait) / float64(time.Millisecond),
			})
			appmetrics.AISLORequests.WithLabelValues(sloClass, "success").Inc()
			return AIRouteExecution{Result: resp.Result, Audit: audit}, nil
		}
		if errors.Is(callErr, context.DeadlineExceeded) && ctx.Err() == nil && sloCtx.Err() != nil {
			callErr = sloDeadlineError(sloClass, providerName, modelName)
		}

		lastErr = callErr
		result := "retryable_error"
		if errors.Is(callErr, ErrAIBackpressure) || errors.Is(callErr, ErrAISLODeadline) {
			if errors.Is(callErr, ErrAISLODeadline) {
				result = "slo_deadline"
			} else {
				result = "backpressure"
			}
			runtime.breaker.Success()
		} else if !IsRetryable(callErr) {
			result = "permanent_error"
			runtime.breaker.Success()
		} else {
			runtime.breaker.Failure()
		}
		appmetrics.AIProviderCalls.WithLabelValues(providerName, result).Inc()
		span.RecordError(callErr)
		span.SetStatus(otelcodes.Error, callErr.Error())
		span.End()
		audit.Attempts = append(audit.Attempts, AIRouteAttemptAudit{
			Provider:    providerName,
			Model:       runtime.info.Model,
			Result:      result,
			LatencyMS:   float64(latency) / float64(time.Millisecond),
			QueueWaitMS: float64(queueWait) / float64(time.Millisecond),
			Error:       callErr.Error(),
		})

		if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) || !IsRetryable(callErr) {
			audit.TotalLatencyMS = float64(time.Since(startedTotal)) / float64(time.Millisecond)
			appmetrics.AISLORequests.WithLabelValues(sloClass, "failed").Inc()
			return AIRouteExecution{Audit: audit}, callErr
		}
		audit.FallbackCount++
		previousProvider = providerName
	}

	audit.TotalLatencyMS = float64(time.Since(startedTotal)) / float64(time.Millisecond)
	if lastErr != nil {
		appmetrics.AISLORequests.WithLabelValues(sloClass, "backpressure").Inc()
		return AIRouteExecution{Audit: audit}, lastErr
	}
	appmetrics.AISLORequests.WithLabelValues(sloClass, "failed").Inc()
	return AIRouteExecution{Audit: audit}, Retryable(ErrCircuitOpen)
}

func (r *AIProviderRouter) rankCandidates(req AIRequest, strategy string, defaultOutputTokens int) ([]aiCandidate, error) {
	candidates := make([]aiCandidate, 0, len(r.providers))
	for index, runtime := range r.providers {
		if !providerMatches(runtime.info, req) {
			continue
		}
		estimatedCost, costKnown := estimatedProviderCost(runtime.info, req.Prompt, defaultOutputTokens)
		if req.MaxCostUSD > 0 && (!costKnown || estimatedCost > req.MaxCostUSD) {
			continue
		}
		latency, failureRate := runtime.snapshot()
		if latency <= 0 {
			latency = 250 * time.Millisecond
		}
		costForScore := estimatedCost
		if !costKnown {
			costForScore = 0.01
		}
		candidates = append(candidates, aiCandidate{
			runtime:       runtime,
			score:         routeScore(strategy, latency, failureRate, costForScore),
			estimatedCost: estimatedCost,
			costKnown:     costKnown,
			index:         index,
		})
	}
	if len(candidates) == 0 {
		return nil, Permanent(fmt.Errorf("no AI provider satisfies model=%q capability=%q max_cost_usd=%g", req.Model, req.Capability, req.MaxCostUSD))
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if math.Abs(candidates[i].score-candidates[j].score) < 1e-9 {
			return candidates[i].index < candidates[j].index
		}
		return candidates[i].score < candidates[j].score
	})
	return candidates, nil
}

func ReplayAIRouteDecision(metadataJSON, strategyOverride string) (AIRouteReplay, error) {
	var audit AIRouteDecisionAudit
	if err := json.Unmarshal([]byte(metadataJSON), &audit); err != nil {
		return AIRouteReplay{}, fmt.Errorf("decode AI route audit: %w", err)
	}
	if audit.Kind != "ai_route" || len(audit.Candidates) == 0 {
		return AIRouteReplay{}, errors.New("audit does not contain replayable AI route candidates")
	}

	strategy := normalizeRoutingStrategy(strategyOverride)
	if strings.TrimSpace(strategyOverride) == "" {
		strategy = normalizeRoutingStrategy(audit.Strategy)
	}
	if strategy == "" {
		return AIRouteReplay{}, fmt.Errorf("unsupported AI routing strategy %q", strategyOverride)
	}

	type replayCandidate struct {
		provider string
		index    int
		score    float64
	}
	replayed := make([]replayCandidate, 0, len(audit.Candidates))
	for _, candidate := range audit.Candidates {
		cost := candidate.EstimatedCostUSD
		if !candidate.CostKnown {
			cost = 0.01
		}
		latency := time.Duration(candidate.EWMALatencyMS * float64(time.Millisecond))
		if latency <= 0 {
			latency = 250 * time.Millisecond
		}
		replayed = append(replayed, replayCandidate{
			provider: candidate.Provider,
			index:    candidate.Index,
			score:    routeScore(strategy, latency, candidate.FailureRate, cost),
		})
	}
	sort.SliceStable(replayed, func(i, j int) bool {
		if math.Abs(replayed[i].score-replayed[j].score) < 1e-9 {
			return replayed[i].index < replayed[j].index
		}
		return replayed[i].score < replayed[j].score
	})

	return AIRouteReplay{
		Strategy:         strategy,
		OriginalProvider: audit.SelectedProvider,
		ReplayProvider:   replayed[0].provider,
		SameSelection:    audit.SelectedProvider == replayed[0].provider,
	}, nil
}

func parseAIRequest(task *domain.Task) (AIRequest, error) {
	req := AIRequest{TaskID: task.ID, Name: task.Name, Prompt: task.Payload, RetryCount: task.RetryCount}
	var envelope aiTaskEnvelope
	if err := json.Unmarshal([]byte(task.Payload), &envelope); err == nil && envelope.Prompt != nil {
		req.Prompt = *envelope.Prompt
		req.TenantID = strings.TrimSpace(envelope.TenantID)
		req.Model = strings.TrimSpace(envelope.Model)
		req.Capability = strings.ToLower(strings.TrimSpace(envelope.Capability))
		req.MaxCostUSD = envelope.MaxCostUSD
		req.Strategy = normalizeRoutingStrategy(envelope.RoutingStrategy)
		req.SLOClass = normalizeSLOClass(envelope.SLOClass)
		req.DisableHedge = envelope.DisableHedge
		if envelope.MaxCostUSD < 0 {
			return AIRequest{}, Permanent(errors.New("max_cost_usd must be non-negative"))
		}
		if strings.TrimSpace(envelope.RoutingStrategy) != "" && req.Strategy == "" {
			return AIRequest{}, Permanent(fmt.Errorf("unsupported AI routing strategy %q", envelope.RoutingStrategy))
		}
		if strings.TrimSpace(envelope.SLOClass) != "" && req.SLOClass == "" {
			return AIRequest{}, Permanent(fmt.Errorf("unsupported AI SLO class %q", envelope.SLOClass))
		}
	}
	return req, nil
}

type AIBudgetHints struct {
	TenantID   string
	Prompt     string
	MaxCostUSD float64
}

func ParseAIBudgetHints(task *domain.Task) (AIBudgetHints, error) {
	if task == nil {
		return AIBudgetHints{}, Permanent(errors.New("AI task is nil"))
	}
	req, err := parseAIRequest(task)
	if err != nil {
		return AIBudgetHints{}, err
	}
	return AIBudgetHints{TenantID: req.TenantID, Prompt: req.Prompt, MaxCostUSD: req.MaxCostUSD}, nil
}

func EstimateAITokenReservation(prompt string, defaultOutputTokens int) int64 {
	inputTokens := (len([]rune(prompt)) + 3) / 4
	if inputTokens < 1 {
		inputTokens = 1
	}
	if defaultOutputTokens < 1 {
		defaultOutputTokens = 1
	}
	return int64(inputTokens + defaultOutputTokens)
}

func normalizeRoutingConfig(config AIRoutingConfig) AIRoutingConfig {
	if normalizeRoutingStrategy(config.Strategy) == "" {
		config.Strategy = AIRoutingBalanced
	} else {
		config.Strategy = normalizeRoutingStrategy(config.Strategy)
	}
	if config.CircuitFailures <= 0 {
		config.CircuitFailures = 3
	}
	if config.CircuitOpenFor <= 0 {
		config.CircuitOpenFor = 10 * time.Second
	}
	if config.EWMAAlpha <= 0 || config.EWMAAlpha > 1 {
		config.EWMAAlpha = 0.3
	}
	if config.DefaultOutputTokens <= 0 {
		config.DefaultOutputTokens = 256
	}
	config.SLO = normalizeSLOConfig(config.SLO)
	config.FairScheduling = normalizeFairSchedulingConfig(config.FairScheduling)
	config.TenantQuota = normalizeTenantDistributedQuotaConfig(config.TenantQuota)
	if config.DistributedLeaseTTL <= 0 {
		config.DistributedLeaseTTL = 30 * time.Second
	}
	config.LoadShedding = normalizeLoadSheddingConfig(config.LoadShedding)
	if config.Hedge.Delay <= 0 {
		config.Hedge.Delay = 100 * time.Millisecond
	}
	if config.Hedge.CancelWait <= 0 {
		config.Hedge.CancelWait = 100 * time.Millisecond
	}
	if config.Hedge.MaxExtraCostUSD < 0 {
		config.Hedge.MaxExtraCostUSD = 0
	}
	return config
}

func normalizeRoutingStrategy(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case AIRoutingBalanced:
		return AIRoutingBalanced
	case AIRoutingCost:
		return AIRoutingCost
	case AIRoutingLatency:
		return AIRoutingLatency
	case AIRoutingReliability:
		return AIRoutingReliability
	default:
		return ""
	}
}

func NormalizeAIRoutingStrategy(strategy string) string {
	return normalizeRoutingStrategy(strategy)
}

func normalizeProviderInfo(info AIProviderInfo) AIProviderInfo {
	info.Model = strings.TrimSpace(info.Model)
	seen := make(map[string]struct{}, len(info.Capabilities))
	capabilities := make([]string, 0, len(info.Capabilities))
	for _, capability := range info.Capabilities {
		capability = strings.ToLower(strings.TrimSpace(capability))
		if capability == "" {
			continue
		}
		if _, ok := seen[capability]; ok {
			continue
		}
		seen[capability] = struct{}{}
		capabilities = append(capabilities, capability)
	}
	info.Capabilities = capabilities
	if info.InputCostPer1M < 0 {
		info.InputCostPer1M = 0
	}
	if info.OutputCostPer1M < 0 {
		info.OutputCostPer1M = 0
	}
	if info.MaxConcurrency <= 0 {
		info.MaxConcurrency = 4
	}
	if info.GlobalMaxConcurrency < 0 {
		info.GlobalMaxConcurrency = 0
	}
	return info
}

func providerMatches(info AIProviderInfo, req AIRequest) bool {
	if req.Model != "" {
		if info.Model != "" && !strings.EqualFold(info.Model, req.Model) {
			return false
		}
	}
	if req.Capability != "" {
		if len(info.Capabilities) == 0 {
			return false
		}
		for _, capability := range info.Capabilities {
			if strings.EqualFold(capability, req.Capability) {
				return true
			}
		}
		return false
	}
	return true
}

func estimatedProviderCost(info AIProviderInfo, prompt string, outputTokens int) (float64, bool) {
	if info.InputCostPer1M <= 0 && info.OutputCostPer1M <= 0 {
		return 0, false
	}
	inputTokens := (len([]rune(prompt)) + 3) / 4
	if inputTokens < 1 {
		inputTokens = 1
	}
	if outputTokens < 1 {
		outputTokens = 1
	}
	cost := float64(inputTokens)*info.InputCostPer1M/1_000_000 + float64(outputTokens)*info.OutputCostPer1M/1_000_000
	return cost, true
}

func actualProviderCost(info AIProviderInfo, usage AIUsage) float64 {
	if info.InputCostPer1M <= 0 && info.OutputCostPer1M <= 0 {
		return 0
	}
	return float64(usage.InputTokens)*info.InputCostPer1M/1_000_000 + float64(usage.OutputTokens)*info.OutputCostPer1M/1_000_000
}

func routeScore(strategy string, latency time.Duration, failureRate, costUSD float64) float64 {
	latencyMS := float64(latency) / float64(time.Millisecond)
	switch strategy {
	case AIRoutingCost:
		return costUSD*1_000_000 + failureRate*1000 + latencyMS*0.01
	case AIRoutingLatency:
		return latencyMS + failureRate*1000 + costUSD*1000
	case AIRoutingReliability:
		return failureRate*10_000 + latencyMS*0.1 + costUSD*1000
	default:
		return latencyMS*0.4 + failureRate*1000*0.4 + costUSD*1_000_000*0.2
	}
}

func (runtime *aiProviderRuntime) observe(latency time.Duration, failed bool, alpha float64) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.calls++
	if failed {
		runtime.failures++
	}
	if runtime.ewmaLatency == 0 {
		runtime.ewmaLatency = latency
		return
	}
	runtime.ewmaLatency = time.Duration(alpha*float64(latency) + (1-alpha)*float64(runtime.ewmaLatency))
}

func (runtime *aiProviderRuntime) snapshot() (time.Duration, float64) {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	failureRate := 0.0
	if runtime.calls > 0 {
		failureRate = float64(runtime.failures) / float64(runtime.calls)
	}
	return runtime.ewmaLatency, failureRate
}

func (runtime *aiProviderRuntime) pressureSnapshot() (time.Duration, uint64, uint64) {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.ewmaLatency, runtime.calls, runtime.failures
}

func (runtime *aiProviderRuntime) gateFor(model string) *modelConcurrencyGate {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		model = "unknown"
	}
	runtime.gateMu.Lock()
	defer runtime.gateMu.Unlock()
	if gate := runtime.gates[model]; gate != nil {
		return gate
	}
	gate := newModelConcurrencyGate(runtime.info.MaxConcurrency, runtime.fair)
	runtime.gates[model] = gate
	return gate
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
