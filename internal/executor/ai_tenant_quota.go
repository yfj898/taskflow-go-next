package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type AITenantDistributedQuotaConfig struct {
	Enabled      bool
	DefaultLimit int
	Overrides    map[string]int
}

func normalizeTenantDistributedQuotaConfig(config AITenantDistributedQuotaConfig) AITenantDistributedQuotaConfig {
	if config.DefaultLimit < 0 {
		config.DefaultLimit = 0
	}
	if len(config.Overrides) == 0 {
		config.Overrides = nil
		return config
	}

	normalized := make(map[string]int, len(config.Overrides))
	for tenantID, limit := range config.Overrides {
		if limit <= 0 {
			continue
		}
		normalized[normalizeTenantID(tenantID)] = limit
	}
	if len(normalized) == 0 {
		config.Overrides = nil
	} else {
		config.Overrides = normalized
	}
	return config
}

func (config AITenantDistributedQuotaConfig) limitFor(tenantID string) int {
	if !config.Enabled {
		return 0
	}
	tenantID = normalizeTenantID(tenantID)
	if limit, ok := config.Overrides[tenantID]; ok && limit > 0 {
		return limit
	}
	if config.DefaultLimit > 0 {
		return config.DefaultLimit
	}
	return 0
}

// ParseAITenantConcurrencyOverrides parses a comma-separated list such as
// "tenant-a=1,tenant-b=3". Invalid or non-positive entries are ignored so a
// malformed optional override cannot disable the default quota for all other
// tenants.
func ParseAITenantConcurrencyOverrides(value string) map[string]int {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	overrides := make(map[string]int)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			continue
		}
		tenantID := strings.TrimSpace(parts[0])
		limit, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if tenantID == "" || err != nil || limit <= 0 {
			continue
		}
		overrides[normalizeTenantID(tenantID)] = limit
	}
	if len(overrides) == 0 {
		return nil
	}
	return overrides
}

// NormalizeAITenantConcurrencyOverrides validates and canonicalizes a runtime
// governance value. Unlike the environment parser above, runtime writes are
// rejected when any entry is malformed so operators cannot believe a quota was
// applied when it was silently ignored.
func NormalizeAITenantConcurrencyOverrides(value string) (string, map[string]int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil, nil
	}
	overrides := make(map[string]int)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return "", nil, fmt.Errorf("invalid tenant quota override %q", item)
		}
		tenantID := normalizeTenantID(parts[0])
		limit, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || limit <= 0 {
			return "", nil, fmt.Errorf("invalid tenant quota limit in %q", item)
		}
		overrides[tenantID] = limit
	}
	keys := make([]string, 0, len(overrides))
	for tenantID := range overrides {
		keys = append(keys, tenantID)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, tenantID := range keys {
		parts = append(parts, tenantID+"="+strconv.Itoa(overrides[tenantID]))
	}
	return strings.Join(parts, ","), overrides, nil
}

func (r *AIProviderRouter) tenantQuotaConfig(ctx context.Context) (AITenantDistributedQuotaConfig, string) {
	if policy, ok := AIRoutingPolicyFromContext(ctx); ok && policy.TenantQuotaManaged {
		return normalizeTenantDistributedQuotaConfig(AITenantDistributedQuotaConfig{
			Enabled:      policy.TenantQuotaEnabled,
			DefaultLimit: policy.TenantQuotaDefaultLimit,
			Overrides:    ParseAITenantConcurrencyOverrides(policy.TenantQuotaOverrides),
		}), "runtime"
	}
	return r.config.TenantQuota, "static"
}

func (r *AIProviderRouter) acquireTenantQuota(ctx context.Context, quota AITenantDistributedQuotaConfig, provider, model, tenantID, sloClass string, maxWait time.Duration) (time.Duration, DistributedConcurrencyLease, string, error) {
	limit := quota.limitFor(tenantID)
	if limit <= 0 {
		return 0, nil, "", nil
	}
	if r.config.TenantCoordinator == nil {
		return 0, nil, "coordinator_error", Retryable(errors.New("distributed tenant concurrency coordinator is not configured"))
	}
	wait, _, lease, err := r.config.TenantCoordinator.AcquireTenant(
		ctx,
		provider,
		model,
		tenantID,
		limit,
		r.config.DistributedLeaseTTL,
		maxWait,
	)
	if err == nil {
		return wait, lease, "acquired", nil
	}
	if errors.Is(err, ErrAITenantConcurrency) {
		return wait, nil, "tenant_backpressure", tenantConcurrencyError(sloClass, provider, model)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return wait, nil, "slo_deadline", sloDeadlineError(sloClass, provider, model)
	}
	if errors.Is(err, context.Canceled) {
		return wait, nil, "canceled", context.Canceled
	}
	return wait, nil, "coordinator_error", Retryable(fmt.Errorf("distributed tenant concurrency coordinator: %w", err))
}
