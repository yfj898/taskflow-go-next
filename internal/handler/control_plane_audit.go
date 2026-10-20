package handler

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/service"
)

type ControlPlaneAuditConfig struct {
	Enabled bool
}

func controlPlaneAudit(audits *service.ControlPlaneAuditService, config ControlPlaneAuditConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !config.Enabled {
			c.Next()
			return
		}
		if audits == nil {
			appmetrics.ControlPlaneAudits.WithLabelValues(c.Request.Method, "unavailable").Inc()
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "control-plane audit is unavailable"})
			return
		}

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		audit := &domain.ControlPlaneAudit{
			OperatorID:     boundedAuditValue(c.GetString(controlPlaneOperatorKey), 128),
			CredentialSlot: boundedAuditValue(c.GetString(controlPlaneCredentialSlotKey), 16),
			Method:         boundedAuditValue(c.Request.Method, 16),
			Route:          boundedAuditValue(route, 256),
			Target:         boundedAuditValue(c.Request.URL.Path, 512),
			RequestID:      boundedAuditValue(c.GetString("request_id"), 128),
			Outcome:        "started",
			CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		}
		if audit.OperatorID == "" || audit.CredentialSlot == "" {
			appmetrics.ControlPlaneAudits.WithLabelValues(c.Request.Method, "identity_missing").Inc()
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "control-plane identity is unavailable"})
			return
		}
		if err := audits.Begin(c.Request.Context(), audit); err != nil {
			appmetrics.ControlPlaneAudits.WithLabelValues(c.Request.Method, "persist_failed").Inc()
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "control-plane audit unavailable"})
			return
		}

		c.Next()

		status := c.Writer.Status()
		outcome := controlPlaneAuditOutcome(status)
		completeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := audits.Complete(completeCtx, audit.ID, status, outcome); err != nil {
			appmetrics.ControlPlaneAudits.WithLabelValues(c.Request.Method, "completion_failed").Inc()
			log.Printf("complete control-plane audit id=%d request_id=%s: %v", audit.ID, audit.RequestID, err)
			return
		}
		appmetrics.ControlPlaneAudits.WithLabelValues(c.Request.Method, outcome).Inc()
	}
}

func controlPlaneAuditOutcome(status int) string {
	switch {
	case status >= 500:
		return "server_error"
	case status >= 400:
		return "client_error"
	default:
		return "success"
	}
}

func boundedAuditValue(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
