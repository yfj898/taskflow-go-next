package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
)

func RegisterCloudControlRoutes(
	r *gin.Engine,
	cloudResources *service.CloudResourceService,
	auth ControlPlaneAuthConfig,
	audits *service.ControlPlaneAuditService,
	auditConfig ControlPlaneAuditConfig,
) {
	if r == nil || cloudResources == nil {
		return
	}
	group := r.Group("/api/v1/cloud")
	group.Use(
		controlPlaneBearerAuth(auth),
		controlPlaneOIDCRBAC(auth),
		cloudTenantIdentityGuard(auth),
		controlPlaneAudit(audits, auditConfig),
	)

	group.PUT("/tenants/:tenant/quota", func(c *gin.Context) {
		if !cloudTenantAllowed(c, auth, c.Param("tenant")) {
			return
		}
		var in service.SetCloudTenantQuotaInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		quota, err := cloudResources.SetTenantQuota(c.Request.Context(), c.Param("tenant"), in)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, quota)
	})

	group.GET("/tenants/:tenant/quota", func(c *gin.Context) {
		if !cloudTenantAllowed(c, auth, c.Param("tenant")) {
			return
		}
		quota, err := cloudResources.GetTenantQuota(c.Request.Context(), c.Param("tenant"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, quota)
	})

	group.POST("/resources", func(c *gin.Context) {
		var in service.CreateCloudResourceInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		if !cloudTenantAllowed(c, auth, in.TenantID) {
			return
		}
		result, err := cloudResources.Create(c.Request.Context(), in, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		// An idempotency key could have been used by another tenant. Never
		// return a cached result unless its persisted owner matches the grant.
		if !cloudTenantAllowed(c, auth, result.Resource.TenantID) {
			return
		}
		c.Header("Idempotency-Key", result.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, result)
	})

	group.GET("/resources", func(c *gin.Context) {
		tenant, ok := cloudTenantForList(c, auth, c.Query("tenant_id"))
		if !ok {
			return
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
		resources, err := cloudResources.List(c.Request.Context(), tenant, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "list cloud resources failed"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": resources})
	})

	group.GET("/resources/:id", func(c *gin.Context) {
		id, ok := cloudResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForResource(c, auth, cloudResources, id) {
			return
		}
		resource, err := cloudResources.Get(c.Request.Context(), id)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, resource)
	})

	group.POST("/resources/:id/resize", func(c *gin.Context) {
		id, ok := cloudResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForResource(c, auth, cloudResources, id) {
			return
		}
		var in service.ResizeCloudResourceInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		result, err := cloudResources.Resize(c.Request.Context(), id, in, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if !cloudTenantAllowed(c, auth, result.Resource.TenantID) {
			return
		}
		c.Header("Idempotency-Key", result.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, result)
	})

	group.DELETE("/resources/:id", func(c *gin.Context) {
		id, ok := cloudResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForResource(c, auth, cloudResources, id) {
			return
		}
		result, err := cloudResources.Delete(c.Request.Context(), id, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if !cloudTenantAllowed(c, auth, result.Resource.TenantID) {
			return
		}
		c.Header("Idempotency-Key", result.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, result)
	})

	group.GET("/resources/:id/orders", func(c *gin.Context) {
		id, ok := cloudResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForResource(c, auth, cloudResources, id) {
			return
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
		orders, err := cloudResources.ListOrders(c.Request.Context(), id, limit, offset)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": orders})
	})

	group.POST("/resources/:id/reconcile", func(c *gin.Context) {
		id, ok := cloudResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForResource(c, auth, cloudResources, id) {
			return
		}
		if err := cloudResources.ReconcileOne(c.Request.Context(), id); err != nil {
			writeCloudResourceError(c, err)
			return
		}
		resource, err := cloudResources.Get(c.Request.Context(), id)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, resource)
	})
}

func cloudResourceID(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cloud resource id"})
		return 0, false
	}
	return id, true
}

func writeCloudResourceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repository.ErrCloudTenantAccessDenied):
		c.JSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
	case errors.Is(err, repository.ErrCloudResourceNotFound), errors.Is(err, repository.ErrCloudQuotaNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, repository.ErrCloudWorkflowNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, repository.ErrCloudResourceDeleted),
		errors.Is(err, repository.ErrCloudResourceSagaManaged),
		errors.Is(err, repository.ErrCloudQuotaExceeded),
		errors.Is(err, repository.ErrCloudQuotaBelowUsage),
		errors.Is(err, repository.ErrCloudWorkflowActive),
		errors.Is(err, repository.ErrCloudWorkflowStateChanged),
		errors.Is(err, repository.ErrCloudWorkflowRequiresActive),
		errors.Is(err, service.ErrIdempotencyConflict),
		errors.Is(err, service.ErrCloudReconcileBusy),
		errors.Is(err, service.ErrCloudWorkflowBusy),
		errors.Is(err, repository.ErrCloudWorkflowNotRunnable):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, service.ErrInvalidCloudResource), errors.Is(err, service.ErrInvalidCloudQuota), errors.Is(err, service.ErrInvalidCloudWorkflow):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, service.ErrCloudProviderDisabled):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cloud resource operation failed"})
	}
}
