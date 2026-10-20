package handler

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
)

// cloudTenantIdentityGuard ensures a verified tenant grant is present whenever
// tenant isolation is enabled. All cloud routes additionally check the tenant
// of the requested object or operation before reading or mutating it.
func cloudTenantIdentityGuard(auth ControlPlaneAuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !auth.CloudTenantAuthzEnabled {
			c.Next()
			return
		}
		slot, _ := c.Get(controlPlaneCredentialSlotKey)
		tenants, ok := cloudAuthorizedTenants(c)
		if slot != "oidc" || !ok || len(tenants) == 0 {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "cloud tenant authorization required"})
			return
		}
		grants := make([]string, 0, len(tenants))
		for tenant := range tenants {
			grants = append(grants, tenant)
		}
		c.Request = c.Request.WithContext(repository.WithCloudTenantGrants(c.Request.Context(), grants))
		c.Next()
	}
}

func cloudAuthorizedTenants(c *gin.Context) (map[string]struct{}, bool) {
	raw, exists := c.Get(controlPlaneTenantsKey)
	if !exists {
		return nil, false
	}
	claims, ok := raw.([]string)
	if !ok {
		return nil, false
	}
	tenants := make(map[string]struct{}, len(claims))
	for _, tenant := range claims {
		if strings.TrimSpace(tenant) != "" {
			tenants[tenant] = struct{}{}
		}
	}
	return tenants, true
}

// cloudTenantAllowed is deliberately deny-by-default and does not treat an
// administrator role as an automatic cross-tenant access grant.
func cloudTenantAllowed(c *gin.Context, auth ControlPlaneAuthConfig, tenant string) bool {
	if !auth.CloudTenantAuthzEnabled {
		return true
	}
	slot, _ := c.Get(controlPlaneCredentialSlotKey)
	tenants, ok := cloudAuthorizedTenants(c)
	_, allowed := tenants[strings.TrimSpace(tenant)]
	if slot != "oidc" || !ok || !allowed || tenant == "" {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
		return false
	}
	return true
}

// List APIs cannot default to an unfiltered cross-tenant query. For a token
// containing multiple tenants, callers must select one explicitly.
func cloudTenantForList(c *gin.Context, auth ControlPlaneAuthConfig, requested string) (string, bool) {
	requested = strings.TrimSpace(requested)
	if !auth.CloudTenantAuthzEnabled {
		return requested, true
	}
	if requested != "" {
		return requested, cloudTenantAllowed(c, auth, requested)
	}
	tenants, ok := cloudAuthorizedTenants(c)
	if !ok || len(tenants) == 0 {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
		return "", false
	}
	if len(tenants) != 1 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "tenant_id is required for multi-tenant identities"})
		return "", false
	}
	for tenant := range tenants {
		return tenant, true
	}
	return "", false
}

func cloudTenantForResource(c *gin.Context, auth ControlPlaneAuthConfig, resources *service.CloudResourceService, resourceID uint64) bool {
	if !auth.CloudTenantAuthzEnabled {
		return true
	}
	resource, err := resources.Get(c.Request.Context(), resourceID)
	if err != nil {
		writeCloudResourceError(c, err)
		return false
	}
	return cloudTenantAllowed(c, auth, resource.TenantID)
}

func cloudTenantForWorkflowResource(c *gin.Context, auth ControlPlaneAuthConfig, workflows *service.CloudWorkflowService, resourceID uint64) bool {
	if !auth.CloudTenantAuthzEnabled {
		return true
	}
	tenant, err := workflows.ResourceTenant(c.Request.Context(), resourceID)
	if err != nil {
		writeCloudResourceError(c, err)
		return false
	}
	return cloudTenantAllowed(c, auth, tenant)
}
