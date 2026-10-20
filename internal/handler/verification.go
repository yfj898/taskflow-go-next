package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
	"github.com/yfj898/taskflow-go/internal/verification"
)

func writeVerificationError(c *gin.Context, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, verification.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, verification.ErrDenied):
		status = http.StatusForbidden
	case errors.Is(err, repository.ErrVerificationNotFound), errors.Is(err, service.ErrVerificationArtifactNotFound):
		status = http.StatusNotFound
	case errors.Is(err, repository.ErrVerificationConflict):
		status = http.StatusConflict
	case errors.Is(err, verification.ErrUnavailable):
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, gin.H{"error": err.Error()})
}
func RegisterVerificationRoutes(r *gin.Engine, s *service.VerificationService, auth ControlPlaneAuthConfig) {
	v := r.Group("/api/v1/verification-runs")
	v.Use(func(c *gin.Context) {
		if !auth.Enabled || s == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "verification requires enabled authentication and registration"})
			return
		}
		c.Next()
	}, controlPlaneBearerAuth(auth))
	v.POST("", func(c *gin.Context) {
		var input domain.VerificationRequest
		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		view, created, err := s.Submit(c.Request.Context(), c.GetString(controlPlaneOperatorKey), input)
		if err != nil {
			writeVerificationError(c, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusAccepted
		}
		c.JSON(status, view)
	})
	v.GET("/:run_id", func(c *gin.Context) {
		view, err := s.Get(c.Request.Context(), c.GetString(controlPlaneOperatorKey), c.Param("run_id"))
		if err != nil {
			writeVerificationError(c, err)
			return
		}
		c.JSON(http.StatusOK, view)
	})
	v.GET("/:run_id/artifacts/:artifact_id", func(c *gin.Context) {
		generation, err := strconv.ParseUint(c.Query("generation"), 10, 64)
		if err != nil || generation == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "positive generation is required"})
			return
		}
		data, err := s.Artifact(c.Request.Context(), c.GetString(controlPlaneOperatorKey), c.Param("run_id"), generation, c.Param("artifact_id"))
		if err != nil {
			writeVerificationError(c, err)
			return
		}
		c.Header("Content-Disposition", "attachment; filename=verification.log")
		c.Data(http.StatusOK, "text/plain; charset=utf-8", data)
	})
}
