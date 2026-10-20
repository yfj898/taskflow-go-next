package cloudcontrol

import (
	"context"
	"strings"
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type faultInjectionFakeProvider struct {
	networkCalls int
}

func (p *faultInjectionFakeProvider) EnsureNetwork(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	p.networkCalls++
	return SagaStepResult{Ready: true, ExternalID: "svc-1"}, nil
}
func (*faultInjectionFakeProvider) DeleteNetwork(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	return SagaStepResult{Ready: true}, nil
}
func (*faultInjectionFakeProvider) EnsureStorage(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	return SagaStepResult{Ready: true}, nil
}
func (*faultInjectionFakeProvider) DeleteStorage(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	return SagaStepResult{Ready: true}, nil
}
func (*faultInjectionFakeProvider) EnsureCompute(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	return SagaStepResult{Ready: true}, nil
}
func (*faultInjectionFakeProvider) DeleteCompute(context.Context, domain.CloudResource, KubernetesBundleSpec) (SagaStepResult, error) {
	return SagaStepResult{Ready: true}, nil
}

func TestSagaFaultInjectionAfterSuccessIsOneShot(t *testing.T) {
	t.Setenv("CLOUD_SAGA_FAULT_INJECTION_ENABLED", "true")
	t.Setenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP", "network")
	t.Setenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR", "true")
	base := &faultInjectionFakeProvider{}
	provider, err := WrapSagaProviderWithFaultInjectionFromEnvironment(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.EnsureNetwork(context.Background(), domain.CloudResource{}, KubernetesBundleSpec{}); err == nil || !strings.Contains(err.Error(), "outcome-unknown") {
		t.Fatalf("first network ensure error=%v want injected outcome-unknown", err)
	}
	result, err := provider.EnsureNetwork(context.Background(), domain.CloudResource{}, KubernetesBundleSpec{})
	if err != nil {
		t.Fatalf("second network ensure: %v", err)
	}
	if !result.Ready || base.networkCalls != 2 {
		t.Fatalf("second result=%+v calls=%d", result, base.networkCalls)
	}
}

func TestSagaFaultInjectionDisabledReturnsDelegate(t *testing.T) {
	t.Setenv("CLOUD_SAGA_FAULT_INJECTION_ENABLED", "false")
	base := &faultInjectionFakeProvider{}
	provider, err := WrapSagaProviderWithFaultInjectionFromEnvironment(base)
	if err != nil {
		t.Fatal(err)
	}
	if provider != base {
		t.Fatal("disabled fault injection should return the original provider")
	}
}

func TestSagaFaultInjectionUnsetReturnsDelegate(t *testing.T) {
	base := &faultInjectionFakeProvider{}
	provider, err := WrapSagaProviderWithFaultInjectionFromEnvironment(base)
	if err != nil {
		t.Fatal(err)
	}
	if provider != base {
		t.Fatal("unset fault injection should return the original provider")
	}
}

func TestSagaFaultInjectionRejectsEmptyAction(t *testing.T) {
	t.Setenv("CLOUD_SAGA_FAULT_INJECTION_ENABLED", "true")
	t.Setenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP", "network")
	t.Setenv("CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR", "false")
	_, err := WrapSagaProviderWithFaultInjectionFromEnvironment(&faultInjectionFakeProvider{})
	if err == nil {
		t.Fatalf("expected configuration error, got %v", err)
	}
}
