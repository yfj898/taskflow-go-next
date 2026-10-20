package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type aiHedgeAttemptResult struct {
	candidate     aiCandidate
	providerName  string
	modelName     string
	response      AIResponse
	usage         AIUsage
	err           error
	result        string
	queueWait     time.Duration
	latency       time.Duration
	selectedModel string
}

type aiHedgePairResult struct {
	execution      AIRouteExecution
	success        bool
	attemptedCount int
	lastErr        error
	lastProvider   string
}

func (r *AIProviderRouter) hedgeEligible(req AIRequest, sloClass, pressureState string, candidates []aiCandidate) bool {
	if !r.config.Hedge.Enabled || req.DisableHedge || sloClass != AISLORealtime || pressureState != AIPressureNormal || len(candidates) < 2 {
		return false
	}
	primary := candidates[0]
	secondary := candidates[1]
	primaryModel := firstNonEmpty(primary.runtime.info.Model, req.Model, "unknown")
	secondaryModel := firstNonEmpty(secondary.runtime.info.Model, req.Model, "unknown")
	if primary.runtime.provider.Name() == secondary.runtime.provider.Name() && primaryModel == secondaryModel {
		return false
	}
	if r.config.Hedge.MaxExtraCostUSD > 0 {
		if !secondary.costKnown || secondary.estimatedCost > r.config.Hedge.MaxExtraCostUSD {
			return false
		}
	}
	if req.MaxCostUSD > 0 {
		if !primary.costKnown || !secondary.costKnown || primary.estimatedCost+secondary.estimatedCost > req.MaxCostUSD {
			return false
		}
	}
	return true
}

func (r *AIProviderRouter) executeHedgedPair(
	ctx context.Context,
	taskType string,
	req AIRequest,
	strategy string,
	sloClass string,
	sloPolicy AISLOPolicy,
	candidates []aiCandidate,
	audit AIRouteDecisionAudit,
	startedTotal time.Time,
) aiHedgePairResult {
	if len(candidates) < 2 {
		return aiHedgePairResult{execution: AIRouteExecution{Audit: audit}}
	}

	hedgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan aiHedgeAttemptResult, 2)
	launch := func(candidate aiCandidate, reason string) {
		go func() {
			results <- r.runHedgeAttempt(hedgeCtx, taskType, req, strategy, sloClass, sloPolicy, candidate, reason)
		}()
	}

	launch(candidates[0], "selected")
	timer := time.NewTimer(r.config.Hedge.Delay)
	defer timer.Stop()

	select {
	case first := <-results:
		audit.Attempts = append(audit.Attempts, hedgeAuditAttempt(first, first.result))
		if first.err == nil {
			appmetrics.AIHedgeRequests.WithLabelValues("not_triggered_primary_fast").Inc()
			return r.finishSingleHedgeAttempt(first, audit, startedTotal, 1)
		}
		return aiHedgePairResult{
			execution:      AIRouteExecution{Audit: audit},
			attemptedCount: 1,
			lastErr:        first.err,
			lastProvider:   first.providerName,
		}
	case <-timer.C:
		audit.HedgeTriggered = true
		audit.HedgeDelayMS = float64(r.config.Hedge.Delay) / float64(time.Millisecond)
		audit.FallbackCount++
		appmetrics.AIHedgeDelay.Observe(r.config.Hedge.Delay.Seconds())
		launch(candidates[1], "hedge_secondary")
	}

	collected := make([]aiHedgeAttemptResult, 0, 2)
	var winner *aiHedgeAttemptResult
	for len(collected) < 2 && winner == nil {
		select {
		case result := <-results:
			collected = append(collected, result)
			if result.err == nil {
				copyResult := result
				winner = &copyResult
				cancel()
			}
		case <-ctx.Done():
			return aiHedgePairResult{
				execution:      AIRouteExecution{Audit: audit},
				attemptedCount: 2,
				lastErr:        ctx.Err(),
			}
		}
	}

	if winner != nil && len(collected) < 2 {
		cancelTimer := time.NewTimer(r.config.Hedge.CancelWait)
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-cancelTimer.C:
			appmetrics.AIHedgeRequests.WithLabelValues("loser_cancel_pending").Inc()
		}
		if !cancelTimer.Stop() {
			select {
			case <-cancelTimer.C:
			default:
			}
		}
	}

	if winner == nil {
		var lastErr error
		var lastProvider string
		for _, result := range collected {
			audit.Attempts = append(audit.Attempts, hedgeAuditAttempt(result, result.result))
			if result.err != nil {
				lastErr = result.err
				lastProvider = result.providerName
			}
		}
		appmetrics.AIHedgeRequests.WithLabelValues("both_failed").Inc()
		return aiHedgePairResult{
			execution:      AIRouteExecution{Audit: audit},
			attemptedCount: 2,
			lastErr:        lastErr,
			lastProvider:   lastProvider,
		}
	}

	winnerProvider := winner.providerName
	winnerCost := winner.usage.CostUSD
	totalUsage := AIUsage{}
	for _, result := range collected {
		label := result.result
		if result.err == nil {
			totalUsage.InputTokens += result.usage.InputTokens
			totalUsage.OutputTokens += result.usage.OutputTokens
			totalUsage.TotalTokens += result.usage.TotalTokens
			totalUsage.CostUSD += result.usage.CostUSD
			if result.providerName == winnerProvider && result.selectedModel == winner.selectedModel {
				label = "hedge_winner"
			} else {
				label = "hedge_loser_success"
			}
		} else if errors.Is(result.err, context.Canceled) {
			label = "hedge_canceled"
		}
		audit.Attempts = append(audit.Attempts, hedgeAuditAttempt(result, label))
	}

	extraCost := totalUsage.CostUSD - winnerCost
	if extraCost < 0 {
		extraCost = 0
	}
	if extraCost > 0 {
		appmetrics.AIHedgeExtraCostUSD.Add(extraCost)
	}
	audit.HedgeWinner = winnerProvider
	audit.HedgeExtraCostUSD = extraCost
	audit.SelectedProvider = winnerProvider
	audit.SelectedModel = winner.selectedModel
	audit.EstimatedCostUSD = winner.candidate.estimatedCost
	audit.ActualCostUSD = totalUsage.CostUSD
	audit.InputTokens = totalUsage.InputTokens
	audit.OutputTokens = totalUsage.OutputTokens
	audit.TotalTokens = totalUsage.TotalTokens
	audit.TotalLatencyMS = float64(time.Since(startedTotal)) / float64(time.Millisecond)
	appmetrics.AISLORequests.WithLabelValues(sloClass, "success").Inc()
	outcome := "winner_primary"
	if winnerProvider == candidates[1].runtime.provider.Name() {
		outcome = "winner_secondary"
	}
	appmetrics.AIHedgeRequests.WithLabelValues(outcome).Inc()
	return aiHedgePairResult{
		execution: AIRouteExecution{Result: winner.response.Result, Audit: audit},
		success:   true,
	}
}

func (r *AIProviderRouter) finishSingleHedgeAttempt(result aiHedgeAttemptResult, audit AIRouteDecisionAudit, startedTotal time.Time, attempted int) aiHedgePairResult {
	audit.SelectedProvider = result.providerName
	audit.SelectedModel = result.selectedModel
	audit.EstimatedCostUSD = result.candidate.estimatedCost
	audit.ActualCostUSD = result.usage.CostUSD
	audit.InputTokens = result.usage.InputTokens
	audit.OutputTokens = result.usage.OutputTokens
	audit.TotalTokens = result.usage.TotalTokens
	audit.TotalLatencyMS = float64(time.Since(startedTotal)) / float64(time.Millisecond)
	appmetrics.AISLORequests.WithLabelValues(AISLORealtime, "success").Inc()
	return aiHedgePairResult{
		execution:      AIRouteExecution{Result: result.response.Result, Audit: audit},
		success:        true,
		attemptedCount: attempted,
	}
}

func hedgeAuditAttempt(result aiHedgeAttemptResult, label string) AIRouteAttemptAudit {
	attempt := AIRouteAttemptAudit{
		Provider:    result.providerName,
		Model:       result.selectedModel,
		Result:      label,
		LatencyMS:   float64(result.latency) / float64(time.Millisecond),
		QueueWaitMS: float64(result.queueWait) / float64(time.Millisecond),
	}
	if result.err != nil {
		attempt.Error = result.err.Error()
	}
	return attempt
}

func (r *AIProviderRouter) runHedgeAttempt(
	ctx context.Context,
	taskType string,
	req AIRequest,
	strategy string,
	sloClass string,
	sloPolicy AISLOPolicy,
	candidate aiCandidate,
	reason string,
) aiHedgeAttemptResult {
	runtime := candidate.runtime
	providerName := runtime.provider.Name()
	modelName := firstNonEmpty(runtime.info.Model, req.Model, "unknown")
	result := aiHedgeAttemptResult{candidate: candidate, providerName: providerName, modelName: modelName, selectedModel: modelName}

	if err := runtime.breaker.Allow(); err != nil {
		appmetrics.AIProviderCircuitSkips.WithLabelValues(providerName).Inc()
		result.err = Retryable(err)
		result.result = "circuit_open"
		return result
	}

	gate := runtime.gateFor(modelName)
	active, queued, _ := gate.snapshot()
	appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
	appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
	queueWait, releaseSlot, err := gate.acquireObserved(
		ctx,
		sloPolicy.Priority,
		req.TenantID,
		sloPolicy.QueueWait,
		func(active, queued, _ int) {
			appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
			appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
		},
	)
	result.queueWait = queueWait
	appmetrics.AIModelQueueWait.WithLabelValues(providerName, modelName, sloClass).Observe(queueWait.Seconds())
	active, queued, _ = gate.snapshot()
	appmetrics.AIModelConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(active))
	appmetrics.AIModelConcurrencyQueued.WithLabelValues(providerName, modelName).Set(float64(queued))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			result.err = sloDeadlineError(sloClass, providerName, modelName)
			result.result = "slo_deadline"
		} else if errors.Is(err, context.Canceled) {
			result.err = context.Canceled
			result.result = "hedge_canceled"
		} else {
			result.err = backpressureError(sloClass, providerName, modelName)
			result.result = "backpressure"
		}
		return result
	}
	defer releaseSlot()

	remainingWait := sloPolicy.QueueWait - result.queueWait
	if remainingWait < 0 {
		remainingWait = 0
	}
	tenantQuota, _ := r.tenantQuotaConfig(ctx)
	tenantWait, tenantLease, tenantOutcome, tenantErr := r.acquireTenantQuota(
		ctx,
		tenantQuota,
		providerName,
		modelName,
		req.TenantID,
		sloClass,
		remainingWait,
	)
	result.queueWait += tenantWait
	if tenantOutcome != "" {
		appmetrics.AITenantDistributedConcurrency.WithLabelValues(providerName, modelName, sloClass, tenantOutcome).Inc()
	}
	if tenantErr != nil {
		result.err = tenantErr
		if tenantOutcome == "canceled" {
			result.result = "hedge_canceled"
		} else {
			result.result = tenantOutcome
		}
		return result
	}
	if tenantLease != nil {
		defer tenantLease.Release()
	}

	var distributedLease DistributedConcurrencyLease
	if r.config.DistributedCoordinator != nil && runtime.info.GlobalMaxConcurrency > 0 {
		remainingWait := sloPolicy.QueueWait - result.queueWait
		if remainingWait < 0 {
			remainingWait = 0
		}
		globalWait, globalActive, lease, globalErr := r.config.DistributedCoordinator.Acquire(
			ctx,
			providerName,
			modelName,
			runtime.info.GlobalMaxConcurrency,
			r.config.DistributedLeaseTTL,
			remainingWait,
		)
		result.queueWait += globalWait
		appmetrics.AIGlobalConcurrencyActive.WithLabelValues(providerName, modelName).Set(float64(globalActive))
		if globalErr != nil {
			if errors.Is(globalErr, context.Canceled) {
				result.err = context.Canceled
				result.result = "hedge_canceled"
			} else if errors.Is(globalErr, ErrAIGlobalConcurrency) {
				result.err = globalConcurrencyError(sloClass, providerName, modelName)
				result.result = "global_backpressure"
			} else {
				result.err = Retryable(fmt.Errorf("distributed AI concurrency coordinator: %w", globalErr))
				result.result = "coordinator_error"
			}
			return result
		}
		distributedLease = lease
		defer distributedLease.Release()
		appmetrics.AIGlobalConcurrency.WithLabelValues(providerName, modelName, sloClass, "acquired").Inc()
	}

	appmetrics.AIRouteSelections.WithLabelValues(providerName, strategy, reason).Inc()
	started := time.Now()
	providerCtx, span := otel.Tracer("taskflow/ai").Start(
		ctx,
		"ai.provider.generate",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("ai.provider", providerName),
			attribute.String("ai.routing.strategy", strategy),
			attribute.String("ai.slo_class", sloClass),
			attribute.Bool("ai.hedged", reason == "hedge_secondary"),
			attribute.String("task.type", taskType),
		),
	)
	resp, callErr := runtime.provider.Generate(providerCtx, req)
	result.latency = time.Since(started)
	result.response = resp
	appmetrics.AIProviderDuration.WithLabelValues(providerName).Observe(result.latency.Seconds())

	if callErr == nil {
		runtime.breaker.Success()
		runtime.observe(result.latency, false, r.config.EWMAAlpha)
		usage := normalizeAIUsage(resp.Usage)
		if usage.CostUSD == 0 {
			usage.CostUSD = actualProviderCost(runtime.info, usage)
		}
		result.usage = usage
		result.selectedModel = firstNonEmpty(resp.Model, runtime.info.Model, req.Model, "unknown")
		result.result = "success"
		appmetrics.AIProviderCalls.WithLabelValues(providerName, "success").Inc()
		recordAIUsage(providerName, usage)
		span.SetAttributes(
			attribute.String("ai.model", result.selectedModel),
			attribute.Int("ai.total_tokens", usage.TotalTokens),
			attribute.Float64("ai.cost_usd", usage.CostUSD),
		)
		span.End()
		return result
	}

	result.err = callErr
	if errors.Is(callErr, context.Canceled) && ctx.Err() != nil {
		result.result = "hedge_canceled"
		runtime.breaker.Success()
		appmetrics.AIProviderCalls.WithLabelValues(providerName, "hedge_canceled").Inc()
		span.SetStatus(otelcodes.Error, "hedge canceled")
		span.End()
		return result
	}
	if errors.Is(callErr, context.DeadlineExceeded) && ctx.Err() != nil {
		result.err = sloDeadlineError(sloClass, providerName, modelName)
		result.result = "slo_deadline"
		runtime.breaker.Success()
	} else if !IsRetryable(callErr) {
		result.result = "permanent_error"
		runtime.breaker.Success()
	} else {
		result.result = "retryable_error"
		runtime.breaker.Failure()
		runtime.observe(result.latency, true, r.config.EWMAAlpha)
	}
	appmetrics.AIProviderCalls.WithLabelValues(providerName, result.result).Inc()
	span.RecordError(result.err)
	span.SetStatus(otelcodes.Error, result.err.Error())
	span.End()
	return result
}
