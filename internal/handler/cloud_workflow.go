package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/service"
)

func RegisterCloudWorkflowRoutes(
	r *gin.Engine,
	workflows *service.CloudWorkflowService,
	auth ControlPlaneAuthConfig,
	audits *service.ControlPlaneAuditService,
	auditConfig ControlPlaneAuditConfig,
) {
	if r == nil || workflows == nil {
		return
	}
	group := r.Group("/api/v1/cloud/workflows")
	group.Use(
		controlPlaneBearerAuth(auth),
		controlPlaneOIDCRBAC(auth),
		cloudTenantIdentityGuard(auth),
		controlPlaneAudit(audits, auditConfig),
	)

	group.POST("/provision", func(c *gin.Context) {
		var in service.CreateProvisionWorkflowInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		if !cloudTenantAllowed(c, auth, in.TenantID) {
			return
		}
		state, err := workflows.CreateProvisionWorkflow(c.Request.Context(), in, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if auth.CloudTenantAuthzEnabled {
			if state.Resource == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
				return
			}
			if !cloudTenantAllowed(c, auth, state.Resource.TenantID) {
				return
			}
		}
		c.Header("Idempotency-Key", state.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, state)
	})

	group.GET("/:id", func(c *gin.Context) {
		id, ok := cloudWorkflowID(c)
		if !ok {
			return
		}
		state, err := workflows.Get(c.Request.Context(), id)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if auth.CloudTenantAuthzEnabled {
			if state.Resource == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
				return
			}
			if !cloudTenantAllowed(c, auth, state.Resource.TenantID) {
				return
			}
		}
		c.JSON(http.StatusOK, state)
	})

	group.POST("/:id/reconcile", func(c *gin.Context) {
		id, ok := cloudWorkflowID(c)
		if !ok {
			return
		}
		if auth.CloudTenantAuthzEnabled {
			state, err := workflows.Get(c.Request.Context(), id)
			if err != nil {
				writeCloudResourceError(c, err)
				return
			}
			if state.Resource == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
				return
			}
			if !cloudTenantAllowed(c, auth, state.Resource.TenantID) {
				return
			}
		}
		if err := workflows.ReconcileOne(c.Request.Context(), id); err != nil {
			writeCloudResourceError(c, err)
			return
		}
		state, err := workflows.Get(c.Request.Context(), id)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, state)
	})

	resourceGroup := r.Group("/api/v1/cloud/resources")
	resourceGroup.Use(
		controlPlaneBearerAuth(auth),
		controlPlaneOIDCRBAC(auth),
		cloudTenantIdentityGuard(auth),
		controlPlaneAudit(audits, auditConfig),
	)

	resourceGroup.POST("/:id/workflows/resize", func(c *gin.Context) {
		id, ok := cloudWorkflowResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForWorkflowResource(c, auth, workflows, id) {
			return
		}
		var in service.ResizeBundleWorkflowInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		state, err := workflows.CreateResizeWorkflow(c.Request.Context(), id, in, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if auth.CloudTenantAuthzEnabled {
			if state.Resource == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
				return
			}
			if !cloudTenantAllowed(c, auth, state.Resource.TenantID) {
				return
			}
		}
		c.Header("Idempotency-Key", state.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, state)
	})

	resourceGroup.POST("/:id/workflows/delete", func(c *gin.Context) {
		id, ok := cloudWorkflowResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForWorkflowResource(c, auth, workflows, id) {
			return
		}
		state, err := workflows.CreateDeleteWorkflow(c.Request.Context(), id, c.GetHeader("Idempotency-Key"))
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		if auth.CloudTenantAuthzEnabled {
			if state.Resource == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "tenant access denied"})
				return
			}
			if !cloudTenantAllowed(c, auth, state.Resource.TenantID) {
				return
			}
		}
		c.Header("Idempotency-Key", state.Order.IdempotencyKey)
		c.JSON(http.StatusAccepted, state)
	})

	resourceGroup.GET("/:id/workflows", func(c *gin.Context) {
		id, ok := cloudWorkflowResourceID(c)
		if !ok {
			return
		}
		if !cloudTenantForWorkflowResource(c, auth, workflows, id) {
			return
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
		items, err := workflows.ListForResource(c.Request.Context(), id, limit, offset)
		if err != nil {
			writeCloudResourceError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items})
	})
}

func cloudWorkflowID(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cloud workflow id"})
		return 0, false
	}
	return id, true
}

func cloudWorkflowResourceID(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cloud resource id"})
		return 0, false
	}
	return id, true
}
