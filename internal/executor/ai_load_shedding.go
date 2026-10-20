package executor

import (
	"errors"
	"fmt"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

const (
	AIPressureNormal     = "normal"
	AIPressureDegraded   = "degraded"
	AIPressureOverloaded = "overloaded"
)

var ErrAILoadShed = errors.New("AI request shed because executor is overloaded")

type AILoadSheddingConfig struct {
	Enabled               bool
	DegradedQueueDepth    int
	OverloadedQueueDepth  int
	DegradedFailureRate   float64
	OverloadedFailureRate float64
	DegradedLatency       time.Duration
	OverloadedLatency     time.Duration
}

func normalizeLoadSheddingConfig(config AILoadSheddingConfig) AILoadSheddingConfig {
	if config.DegradedQueueDepth <= 0 {
		config.DegradedQueueDepth = 4
	}
	if config.OverloadedQueueDepth <= config.DegradedQueueDepth {
		config.OverloadedQueueDepth = config.DegradedQueueDepth * 4
	}
	if config.DegradedFailureRate <= 0 || config.DegradedFailureRate >= 1 {
		config.DegradedFailureRate = 0.20
	}
	if config.OverloadedFailureRate <= config.DegradedFailureRate || config.OverloadedFailureRate > 1 {
		config.OverloadedFailureRate = 0.50
	}
	if config.DegradedLatency <= 0 {
		config.DegradedLatency = 2 * time.Second
	}
	if config.OverloadedLatency <= config.DegradedLatency {
		config.OverloadedLatency = 5 * time.Second
	}
	return config
}

func (r *AIProviderRouter) pressureState() string {
	if r == nil || !r.config.LoadShedding.Enabled {
		recordPressureState(AIPressureNormal, 0)
		return AIPressureNormal
	}
	queued := 0
	maxLatency := time.Duration(0)
	totalCalls := uint64(0)
	totalFailures := uint64(0)
	for _, runtime := range r.providers {
		runtime.gateMu.Lock()
		for _, gate := range runtime.gates {
			_, q, _ := gate.snapshot()
			queued += q
		}
		runtime.gateMu.Unlock()
		latency, calls, failures := runtime.pressureSnapshot()
		if latency > maxLatency {
			maxLatency = latency
		}
		totalCalls += calls
		totalFailures += failures
	}
	failureRate := 0.0
	if totalCalls >= 5 {
		failureRate = float64(totalFailures) / float64(totalCalls)
	}
	state := AIPressureNormal
	config := r.config.LoadShedding
	if queued >= config.OverloadedQueueDepth || failureRate >= config.OverloadedFailureRate || maxLatency >= config.OverloadedLatency {
		state = AIPressureOverloaded
	} else if queued >= config.DegradedQueueDepth || failureRate >= config.DegradedFailureRate || maxLatency >= config.DegradedLatency {
		state = AIPressureDegraded
	}
	recordPressureState(state, queued)
	return state
}

func recordPressureState(state string, queued int) {
	appmetrics.AIExecutorQueueDepth.Set(float64(queued))
	for _, value := range []string{AIPressureNormal, AIPressureDegraded, AIPressureOverloaded} {
		metric := 0.0
		if state == value {
			metric = 1
		}
		appmetrics.AISystemPressureState.WithLabelValues(value).Set(metric)
	}
}

func applyLoadShedding(state, sloClass string, policy AISLOPolicy, realtime AISLOPolicy) (AISLOPolicy, error) {
	if state == AIPressureOverloaded {
		switch sloClass {
		case AISLOBatch:
			appmetrics.AILoadShedding.WithLabelValues(sloClass, state, "reject").Inc()
			return policy, Retryable(fmt.Errorf("%w: pressure=%s slo=%s", ErrAILoadShed, state, sloClass))
		case AISLOStandard:
			if realtime.QueueWait > 0 && realtime.QueueWait < policy.QueueWait {
				policy.QueueWait = realtime.QueueWait
			}
			appmetrics.AILoadShedding.WithLabelValues(sloClass, state, "short_queue").Inc()
		}
	} else if state == AIPressureDegraded && sloClass == AISLOBatch {
		if realtime.QueueWait > 0 && realtime.QueueWait < policy.QueueWait {
			policy.QueueWait = realtime.QueueWait
		}
		appmetrics.AILoadShedding.WithLabelValues(sloClass, state, "short_queue").Inc()
	}
	return policy, nil
}
