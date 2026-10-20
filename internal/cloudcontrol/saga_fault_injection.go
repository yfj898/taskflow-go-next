package cloudcontrol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

// SagaFaultInjectionConfig exists only to make crash/outcome-unknown recovery
// reproducible in local E2E environments. It is disabled unless the explicit
// CLOUD_SAGA_FAULT_INJECTION_ENABLED switch is true.
type SagaFaultInjectionConfig struct {
	Step              string
	AfterSuccessDelay time.Duration
	AfterSuccessError bool
}

type sagaFaultInjectingProvider struct {
	delegate SagaResourceProvider
	config   SagaFaultInjectionConfig
	fired    atomic.Bool
}

func WrapSagaProviderWithFaultInjectionFromEnvironment(provider SagaResourceProvider) (SagaResourceProvider, error) {
	if provider == nil {
		return nil, nil
	}
	rawEnabled := strings.TrimSpace(os.Getenv("CLOUD_SAGA_FAULT_INJECTION_ENABLED"))
	if rawEnabled == "" {
		return provider, nil
	}
	enabled, err := strconv.ParseBool(rawEnabled)
	if err != nil {
		return nil, fmt.Errorf("invalid CLOUD_SAGA_FAULT_INJECTION_ENABLED: %w", err)
	}
	if !enabled {
		return provider, nil
	}

	step := strings.ToLower(strings.TrimSpace(os.Getenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP")))
	switch step {
	case CloudWorkflowFaultStepNetwork, CloudWorkflowFaultStepStorage, CloudWorkflowFaultStepCompute:
	default:
		return nil, fmt.Errorf("invalid CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP %q", step)
	}

	delay := time.Duration(0)
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_DELAY_MS")); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms < 0 {
			return nil, fmt.Errorf("invalid CLOUD_SAGA_FAULT_AFTER_SUCCESS_DELAY_MS %q", raw)
		}
		delay = time.Duration(ms) * time.Millisecond
	}
	afterSuccessError := false
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR")); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR %q", raw)
		}
		afterSuccessError = parsed
	}
	if delay == 0 && !afterSuccessError {
		return nil, errors.New("Saga fault injection enabled without a delay or synthetic error")
	}
	return &sagaFaultInjectingProvider{
		delegate: provider,
		config: SagaFaultInjectionConfig{
			Step:              step,
			AfterSuccessDelay: delay,
			AfterSuccessError: afterSuccessError,
		},
	}, nil
}

const (
	CloudWorkflowFaultStepNetwork = "network"
	CloudWorkflowFaultStepStorage = "storage"
	CloudWorkflowFaultStepCompute = "compute"
)

func (p *sagaFaultInjectingProvider) EnsureNetwork(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	result, err := p.delegate.EnsureNetwork(ctx, resource, bundle)
	return p.afterSuccess(ctx, CloudWorkflowFaultStepNetwork, result, err)
}

func (p *sagaFaultInjectingProvider) DeleteNetwork(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	return p.delegate.DeleteNetwork(ctx, resource, bundle)
}

func (p *sagaFaultInjectingProvider) EnsureStorage(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	result, err := p.delegate.EnsureStorage(ctx, resource, bundle)
	return p.afterSuccess(ctx, CloudWorkflowFaultStepStorage, result, err)
}

func (p *sagaFaultInjectingProvider) DeleteStorage(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	return p.delegate.DeleteStorage(ctx, resource, bundle)
}

func (p *sagaFaultInjectingProvider) EnsureCompute(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	result, err := p.delegate.EnsureCompute(ctx, resource, bundle)
	return p.afterSuccess(ctx, CloudWorkflowFaultStepCompute, result, err)
}

func (p *sagaFaultInjectingProvider) DeleteCompute(ctx context.Context, resource domain.CloudResource, bundle KubernetesBundleSpec) (SagaStepResult, error) {
	return p.delegate.DeleteCompute(ctx, resource, bundle)
}

func (p *sagaFaultInjectingProvider) afterSuccess(ctx context.Context, step string, result SagaStepResult, err error) (SagaStepResult, error) {
	if err != nil || step != p.config.Step || !p.fired.CompareAndSwap(false, true) {
		return result, err
	}
	if p.config.AfterSuccessDelay > 0 {
		timer := time.NewTimer(p.config.AfterSuccessDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return SagaStepResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	if p.config.AfterSuccessError {
		return SagaStepResult{}, fmt.Errorf("injected outcome-unknown after successful %s side effect", step)
	}
	return result, nil
}

var _ SagaResourceProvider = (*sagaFaultInjectingProvider)(nil)
