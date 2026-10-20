package executor

import (
	"context"
	"sync"
	"testing"
	"time"
)

type sharedTenantQuotaCoordinator struct {
	mu     sync.Mutex
	active map[string]int
}

func newSharedTenantQuotaCoordinator() *sharedTenantQuotaCoordinator {
	return &sharedTenantQuotaCoordinator{active: make(map[string]int)}
}

func (c *sharedTenantQuotaCoordinator) AcquireTenant(ctx context.Context, provider, model, tenantID string, limit int, _ time.Duration, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error) {
	started := time.Now()
	key := provider + ":" + model + ":" + normalizeTenantID(tenantID)
	deadline := started.Add(maxWait)
	for {
		c.mu.Lock()
		active := c.active[key]
		if active < limit {
			c.active[key] = active + 1
			c.mu.Unlock()
			return time.Since(started), active + 1, &sharedTenantQuotaLease{coordinator: c, key: key}, nil
		}
		c.mu.Unlock()
		if maxWait == 0 || !time.Now().Before(deadline) {
			return time.Since(started), active, nil, ErrAITenantConcurrency
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return time.Since(started), active, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type sharedTenantQuotaLease struct {
	coordinator *sharedTenantQuotaCoordinator
	key         string
	once        sync.Once
}

func (l *sharedTenantQuotaLease) Release() {
	l.once.Do(func() {
		l.coordinator.mu.Lock()
		if l.coordinator.active[l.key] > 0 {
			l.coordinator.active[l.key]--
		}
		l.coordinator.mu.Unlock()
	})
}

func phase10RoutingConfig(coordinator DistributedTenantConcurrencyCoordinator) AIRoutingConfig {
	return AIRoutingConfig{
		SLO:               phase6SLOConfig(40 * time.Millisecond),
		TenantCoordinator: coordinator,
		TenantQuota: AITenantDistributedQuotaConfig{
			Enabled:      true,
			DefaultLimit: 1,
		},
		DistributedLeaseTTL: time.Second,
	}
}

func TestAIDistributedTenantQuotaSharedAcrossRouters(t *testing.T) {
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
	routerOne := NewAIProviderRouter(phase10RoutingConfig(coordinator), primaryOne)
	routerTwo := NewAIProviderRouter(phase10RoutingConfig(coordinator), primaryTwo, fallback)

	holderDone := make(chan error, 1)
	go func() {
		_, err := routerOne.Execute(context.Background(), AITaskType, AIRequest{
			Prompt:   "holder",
			TenantID: "tenant-a",
			SLOClass: AISLOStandard,
		})
		holderDone <- err
	}()
	select {
	case <-primaryOne.started:
	case <-time.After(time.Second):
		t.Fatal("first router did not enter provider")
	}

	execution, err := routerTwo.ExecuteDetailed(context.Background(), AITaskType, AIRequest{
		Prompt:   "overflow",
		TenantID: "tenant-a",
		SLOClass: AISLOStandard,
	})
	if err != nil {
		t.Fatalf("same-tenant fallback failed: %v", err)
	}
	if execution.Audit.SelectedProvider != "fallback" {
		t.Fatalf("selected provider=%q, want fallback", execution.Audit.SelectedProvider)
	}
	if len(execution.Audit.Attempts) < 2 || execution.Audit.Attempts[0].Result != "tenant_backpressure" {
		t.Fatalf("unexpected attempts: %+v", execution.Audit.Attempts)
	}
	if !execution.Audit.TenantDistributedQuota || execution.Audit.TenantConcurrencyLimit != 1 {
		t.Fatalf("unexpected tenant quota audit: %+v", execution.Audit)
	}

	close(block)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder failed: %v", err)
	}
}

func TestAIDistributedTenantQuotaKeepsTenantsIndependent(t *testing.T) {
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
	routerOne := NewAIProviderRouter(phase10RoutingConfig(coordinator), primaryOne)
	routerTwo := NewAIProviderRouter(phase10RoutingConfig(coordinator), primaryTwo)

	holderDone := make(chan error, 1)
	go func() {
		_, err := routerOne.Execute(context.Background(), AITaskType, AIRequest{
			Prompt:   "holder",
			TenantID: "tenant-a",
			SLOClass: AISLOStandard,
		})
		holderDone <- err
	}()
	<-primaryOne.started

	result, err := routerTwo.Execute(context.Background(), AITaskType, AIRequest{
		Prompt:   "tenant-b",
		TenantID: "tenant-b",
		SLOClass: AISLOStandard,
	})
	if err != nil {
		t.Fatalf("different tenant was incorrectly throttled: %v", err)
	}
	if result != "shared-primary:tenant-b" {
		t.Fatalf("result=%q, want shared-primary:tenant-b", result)
	}

	close(block)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder failed: %v", err)
	}
}

func TestAITenantQuotaOverridesAndDefaultTenant(t *testing.T) {
	config := normalizeTenantDistributedQuotaConfig(AITenantDistributedQuotaConfig{
		Enabled:      true,
		DefaultLimit: 1,
		Overrides:    ParseAITenantConcurrencyOverrides("gold=4, silver=2, bad=0, malformed"),
	})
	if got := config.limitFor("gold"); got != 4 {
		t.Fatalf("gold limit=%d, want 4", got)
	}
	if got := config.limitFor("silver"); got != 2 {
		t.Fatalf("silver limit=%d, want 2", got)
	}
	if got := config.limitFor("bronze"); got != 1 {
		t.Fatalf("bronze limit=%d, want default 1", got)
	}
	if tenantConcurrencyKeyPart("") != tenantConcurrencyKeyPart("__default__") {
		t.Fatal("empty tenant must share the internal default tenant quota")
	}
	if tenantConcurrencyKeyPart("tenant-a") == "tenant-a" {
		t.Fatal("Redis tenant quota key must not expose the raw tenant id")
	}
}
