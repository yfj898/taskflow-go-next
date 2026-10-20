package executor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestAIRuntimeTenantQuotaOverridesStaticDisabledConfig(t *testing.T) {
	coordinator := newSharedTenantQuotaCoordinator()
	block := make(chan struct{})
	primaryOne := &phase6Provider{
		name:    "shared-primary",
		info:    AIProviderInfo{Model: "model-a", MaxConcurrency: 4},
		block:   block,
		started: make(chan struct{}),
	}
	primaryTwo := &phase6Provider{
		name: "shared-primary",
		info: AIProviderInfo{Model: "model-a", MaxConcurrency: 4},
	}
	fallback := &phase6Provider{
		name: "fallback",
		info: AIProviderInfo{Model: "model-a", MaxConcurrency: 4},
	}
	config := AIRoutingConfig{
		SLO:                 phase6SLOConfig(40 * time.Millisecond),
		TenantCoordinator:   coordinator,
		TenantQuota:         AITenantDistributedQuotaConfig{Enabled: false},
		DistributedLeaseTTL: time.Second,
	}
	routerOne := NewAIProviderRouter(config, primaryOne)
	routerTwo := NewAIProviderRouter(config, primaryTwo, fallback)
	policy := AIRoutingPolicySnapshot{
		Version:                 11,
		Strategy:                AIRoutingBalanced,
		DefaultOutputTokens:     256,
		TenantQuotaManaged:      true,
		TenantQuotaEnabled:      true,
		TenantQuotaDefaultLimit: 1,
	}
	ctx := WithAIRoutingPolicy(context.Background(), policy)

	holderDone := make(chan error, 1)
	go func() {
		_, err := routerOne.Execute(ctx, AITaskType, AIRequest{Prompt: "holder", TenantID: "tenant-a", SLOClass: AISLOStandard})
		holderDone <- err
	}()
	select {
	case <-primaryOne.started:
	case <-time.After(time.Second):
		t.Fatal("runtime quota holder did not enter provider")
	}

	execution, err := routerTwo.ExecuteDetailed(ctx, AITaskType, AIRequest{Prompt: "overflow", TenantID: "tenant-a", SLOClass: AISLOStandard})
	if err != nil {
		t.Fatalf("runtime quota fallback failed: %v", err)
	}
	if execution.Audit.SelectedProvider != "fallback" {
		t.Fatalf("selected provider=%q, want fallback", execution.Audit.SelectedProvider)
	}
	if execution.Audit.TenantQuotaSource != "runtime" || !execution.Audit.TenantDistributedQuota || execution.Audit.TenantConcurrencyLimit != 1 {
		t.Fatalf("unexpected runtime quota audit: %+v", execution.Audit)
	}

	close(block)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder failed: %v", err)
	}
}

func TestAIRuntimeTenantQuotaUnmanagedFallsBackToStaticConfig(t *testing.T) {
	router := NewAIProviderRouter(AIRoutingConfig{
		TenantQuota: AITenantDistributedQuotaConfig{Enabled: true, DefaultLimit: 2},
	}, &phase6Provider{name: "primary", info: AIProviderInfo{Model: "model-a", MaxConcurrency: 4}})
	ctx := WithAIRoutingPolicy(context.Background(), AIRoutingPolicySnapshot{
		Version:             12,
		Strategy:            AIRoutingBalanced,
		DefaultOutputTokens: 256,
		TenantQuotaManaged:  false,
	})
	quota, source := router.tenantQuotaConfig(ctx)
	if source != "static" || !quota.Enabled || quota.limitFor("tenant-a") != 2 {
		t.Fatalf("quota=%+v source=%q, want static limit 2", quota, source)
	}
}

func TestNormalizeAITenantConcurrencyOverridesStrict(t *testing.T) {
	canonical, overrides, err := NormalizeAITenantConcurrencyOverrides(" silver = 2, gold=4 ")
	if err != nil {
		t.Fatalf("normalize valid overrides: %v", err)
	}
	if canonical != "gold=4,silver=2" || overrides["gold"] != 4 || overrides["silver"] != 2 {
		t.Fatalf("canonical=%q overrides=%v", canonical, overrides)
	}
	for _, value := range []string{"bad", "gold=0", "gold=nope"} {
		if _, _, err := NormalizeAITenantConcurrencyOverrides(value); err == nil || !strings.Contains(err.Error(), "tenant quota") {
			t.Fatalf("value=%q error=%v, want validation error", value, err)
		}
	}
}
