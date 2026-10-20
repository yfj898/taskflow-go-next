package repository

import (
	"context"
	"errors"
	"strings"

	"gorm.io/gorm"
)

type cloudTenantScopeKey struct{}

var ErrCloudTenantAccessDenied = errors.New("cloud tenant access denied")

// WithCloudTenantGrants binds authenticated tenant identities to a request.
// Call this only with grants from a verified identity provider, never directly
// from an untrusted HTTP header, path, query or request body. Background
// reconciler contexts remain unscoped so they can operate across all tenants.
func WithCloudTenantGrants(ctx context.Context, grants []string) context.Context {
	copyGrants := make([]string, 0, len(grants))
	seen := make(map[string]struct{}, len(grants))
	for _, tenant := range grants {
		tenant = strings.TrimSpace(tenant)
		if tenant == "" {
			continue
		}
		if _, exists := seen[tenant]; exists {
			continue
		}
		seen[tenant] = struct{}{}
		copyGrants = append(copyGrants, tenant)
	}
	return context.WithValue(ctx, cloudTenantScopeKey{}, copyGrants)
}

func cloudTenantGrants(ctx context.Context) ([]string, bool) {
	grants, scoped := ctx.Value(cloudTenantScopeKey{}).([]string)
	return grants, scoped
}

func cloudTenantPermitted(ctx context.Context, tenant string) bool {
	grants, scoped := cloudTenantGrants(ctx)
	if !scoped {
		return true
	}
	for _, grant := range grants {
		if grant == tenant && tenant != "" {
			return true
		}
	}
	return false
}

// tenantScopedQuery forces ownership into SQL, so a request made with a
// foreign resource/workflow ID never loads or locks another tenant's row.
func tenantScopedQuery(ctx context.Context, query *gorm.DB) *gorm.DB {
	grants, scoped := cloudTenantGrants(ctx)
	if !scoped {
		return query
	}
	if len(grants) == 0 {
		return query.Where("1 = 0")
	}
	return query.Where("tenant_id IN ?", grants)
}
