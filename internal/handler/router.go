package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
	"gorm.io/gorm"
)

type ReadinessCheck func(context.Context) error

type SIEMReceiverControl interface {
	ReceiverStatus(context.Context) (domain.ControlPlaneSIEMReceiverStatus, error)
	SyncReceiverAntiEntropy(context.Context) (int, error)
	PromoteReceiver(context.Context, string, uint64, string) (domain.ControlPlaneSIEMReceiverPromotionResult, error)
	RequestPromotionIntent(context.Context, string, uint64, string, string) (domain.ControlPlaneSIEMPromotionIntent, error)
	ApprovePromotionIntent(context.Context, uint64, string, string) (domain.ControlPlaneSIEMPromotionIntent, error)
	ExecutePromotionIntent(context.Context, uint64, string) (domain.ControlPlaneSIEMReceiverPromotionResult, error)
	ListPromotionIntents(context.Context, int) ([]domain.ControlPlaneSIEMPromotionIntent, error)
	PromotionApprovals(context.Context, uint64) ([]domain.ControlPlaneSIEMPromotionApproval, error)
	BreakGlassPromotion(context.Context, string, uint64, string, string, string) (domain.ControlPlaneSIEMReceiverPromotionResult, error)
	ReceiverTermHistory(context.Context, int) ([]domain.ControlPlaneSIEMReceiverTermHistory, domain.ControlPlaneSIEMTermHistoryVerification, error)
	EnsureTermAnchor(context.Context) (domain.ControlPlaneSIEMTermLedgerAnchor, error)
	IncidentEvidence(context.Context, int) ([]domain.ControlPlaneSIEMIncidentEvidence, error)
	GovernanceStatus(context.Context) (domain.ControlPlaneSIEMGovernanceStatus, error)
	GovernanceDead(context.Context, string, int) ([]domain.ControlPlaneSIEMGovernanceDeadItem, error)
	GovernanceRemediations(context.Context, int) ([]domain.ControlPlaneSIEMGovernanceRemediation, error)
	GovernanceReplicaReceipts(context.Context, string, uint64, int) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error)
	RequeueGovernanceDead(context.Context, string, uint64, string, string) (domain.ControlPlaneSIEMGovernanceRemediation, error)
	RequestGovernanceRemediation(context.Context, string, uint64, string, string, string) (domain.ControlPlaneSIEMGovernanceRemediation, error)
	ApproveGovernanceRemediation(context.Context, uint64, string, string) (domain.ControlPlaneSIEMGovernanceRemediation, error)
	RejectGovernanceRemediation(context.Context, uint64, string, string) (domain.ControlPlaneSIEMGovernanceRemediation, error)
	CancelGovernanceRemediation(context.Context, uint64, string, string) (domain.ControlPlaneSIEMGovernanceRemediation, error)
}

var ErrDraining = errors.New("process draining")

func writeDeadLetterArchiveError(c *gin.Context, err error, fallback string) {
	switch {
	case errors.Is(err, repository.ErrDeadLetterNotFound), errors.Is(err, repository.ErrDeadLetterArchiveNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, repository.ErrDeadLetterReplayActive),
		errors.Is(err, repository.ErrDeadLetterArchiveActive),
		errors.Is(err, repository.ErrDeadLetterArchiveRequired),
		errors.Is(err, repository.ErrDeadLetterArchiveRestoreConflict):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, service.ErrDeadLetterArchiveDisabled), errors.Is(err, service.ErrDeadLetterArchiveSink), errors.Is(err, service.ErrDeadLetterArchiveCopies):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
	case errors.Is(err, service.ErrDeadLetterArchiveIntegrity):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": fallback})
	}
}

func NewRouter(taskService *service.TaskService, db *gorm.DB, rdb *redis.Client, rateRPS, rateBurst int, accessLog bool, extraReady ReadinessCheck) *gin.Engine {
	return NewRouterWithGovernance(taskService, db, rdb, rateRPS, rateBurst, accessLog, extraReady, nil)
}

func NewRouterWithGovernance(
	taskService *service.TaskService,
	db *gorm.DB,
	rdb *redis.Client,
	rateRPS, rateBurst int,
	accessLog bool,
	extraReady ReadinessCheck,
	aiGovernance *service.AIGovernanceService,
) *gin.Engine {
	return NewRouterWithGovernanceAndDeadLetters(taskService, db, rdb, rateRPS, rateBurst, accessLog, extraReady, aiGovernance, nil)
}

func NewRouterWithGovernanceAndDeadLetters(
	taskService *service.TaskService,
	db *gorm.DB,
	rdb *redis.Client,
	rateRPS, rateBurst int,
	accessLog bool,
	extraReady ReadinessCheck,
	aiGovernance *service.AIGovernanceService,
	deadLetters *service.DeadLetterService,
) *gin.Engine {
	return NewRouterWithSecurity(
		taskService,
		db,
		rdb,
		rateRPS,
		rateBurst,
		accessLog,
		extraReady,
		aiGovernance,
		deadLetters,
		ControlPlaneAuthConfig{},
		nil,
		ControlPlaneAuditConfig{},
		nil,
	)
}

func NewRouterWithSecurity(
	taskService *service.TaskService,
	db *gorm.DB,
	rdb *redis.Client,
	rateRPS, rateBurst int,
	accessLog bool,
	extraReady ReadinessCheck,
	aiGovernance *service.AIGovernanceService,
	deadLetters *service.DeadLetterService,
	controlPlaneAuth ControlPlaneAuthConfig,
	controlPlaneAudits *service.ControlPlaneAuditService,
	controlPlaneAuditConfig ControlPlaneAuditConfig,
	siemReceivers SIEMReceiverControl,
) *gin.Engine {
	r := gin.New()
	r.Use(requestID(), traceHTTP())
	if accessLog {
		r.Use(gin.Logger())
	}
	r.Use(gin.Recovery(), observeHTTP(), rateLimit(rateRPS, rateBurst))

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/health/ready", func(c *gin.Context) {
		sqlDB, err := db.DB()
		if err != nil || sqlDB.PingContext(c.Request.Context()) != nil || rdb.Ping(c.Request.Context()).Err() != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		if extraReady != nil {
			err := extraReady(c.Request.Context())
			if err == nil {
				c.JSON(http.StatusOK, gin.H{"status": "ready"})
				return
			}
			if errors.Is(err, ErrDraining) {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "reason": "draining"})
				return
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "dependency": "executor"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	v1 := r.Group("/api/v1")
	controlV1 := v1.Group("")
	controlV1.Use(
		controlPlaneBearerAuth(controlPlaneAuth),
		controlPlaneOIDCRBAC(controlPlaneAuth),
		controlPlaneAudit(controlPlaneAudits, controlPlaneAuditConfig),
	)

	v1.POST("/tasks", func(c *gin.Context) {
		var in service.CreateTaskInput
		if err := c.ShouldBindJSON(&in); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}
		task, err := taskService.Create(c.Request.Context(), in)
		if err != nil {
			if errors.Is(err, service.ErrInvalidTask) || errors.Is(err, service.ErrReservedTaskType) {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "create task failed"})
			return
		}
		c.JSON(http.StatusCreated, task)
	})

	v1.GET("/tasks/:id", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
			return
		}
		task, err := taskService.Get(c.Request.Context(), id)
		if err != nil {
			if errors.Is(err, repository.ErrTaskNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "query task failed"})
			return
		}
		c.JSON(http.StatusOK, task)
	})

	v1.GET("/tasks", func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
		tasks, err := taskService.List(c.Request.Context(), limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "list tasks failed"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": tasks})
	})

	if deadLetters != nil {
		controlV1.GET("/queue/dead-letters", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
			offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
			messages, err := deadLetters.List(c.Request.Context(), limit, offset)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list dead letters failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": messages})
		})

		controlV1.GET("/queue/dead-letters/:id", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dead letter id"})
				return
			}
			message, err := deadLetters.Get(c.Request.Context(), id)
			if err != nil {
				if errors.Is(err, repository.ErrDeadLetterNotFound) {
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "query dead letter failed"})
				return
			}
			c.JSON(http.StatusOK, message)
		})

		controlV1.GET("/queue/dead-letters/:id/replay-audits", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dead letter id"})
				return
			}
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
			offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
			audits, err := deadLetters.ListReplayAudits(c.Request.Context(), id, limit, offset)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list dead letter replay audits failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": audits})
		})

		controlV1.POST("/queue/dead-letters/:id/archive", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dead letter id"})
				return
			}
			archive, err := deadLetters.Archive(c.Request.Context(), id)
			if err != nil {
				writeDeadLetterArchiveError(c, err, "archive dead letter failed")
				return
			}
			c.JSON(http.StatusOK, archive)
		})

		controlV1.GET("/queue/dead-letter-archives", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
			offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
			archives, err := deadLetters.ListArchives(c.Request.Context(), limit, offset)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list dead letter archives failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": archives})
		})

		controlV1.GET("/queue/dead-letter-archives/:id", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid archive id"})
				return
			}
			archive, err := deadLetters.GetArchive(c.Request.Context(), id)
			if err != nil {
				writeDeadLetterArchiveError(c, err, "query dead letter archive failed")
				return
			}
			c.JSON(http.StatusOK, archive)
		})

		controlV1.GET("/queue/dead-letter-archives/:id/export", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid archive id"})
				return
			}
			data, archive, err := deadLetters.ExportArchive(c.Request.Context(), id)
			if err != nil {
				writeDeadLetterArchiveError(c, err, "export dead letter archive failed")
				return
			}
			c.Header("X-Archive-SHA256", archive.SHA256)
			c.Header("X-Archive-Format-Version", strconv.Itoa(archive.FormatVersion))
			c.Data(http.StatusOK, "application/json", data)
		})

		controlV1.POST("/queue/dead-letter-archives/:id/restore", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid archive id"})
				return
			}
			message, err := deadLetters.RestoreArchive(c.Request.Context(), id)
			if err != nil {
				writeDeadLetterArchiveError(c, err, "restore dead letter archive failed")
				return
			}
			c.JSON(http.StatusCreated, message)
		})

		controlV1.POST("/queue/dead-letter-archives/:id/drill", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid archive id"})
				return
			}
			if err := deadLetters.DrillArchive(c.Request.Context(), id); err != nil {
				writeDeadLetterArchiveError(c, err, "drill dead letter archive failed")
				return
			}
			c.Status(http.StatusNoContent)
		})

		controlV1.DELETE("/queue/dead-letters/:id", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dead letter id"})
				return
			}
			if err := deadLetters.Purge(c.Request.Context(), id); err != nil {
				writeDeadLetterArchiveError(c, err, "purge dead letter failed")
				return
			}
			c.Status(http.StatusNoContent)
		})

		controlV1.POST("/queue/dead-letters/:id/replay", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dead letter id"})
				return
			}
			var in service.ReplayDeadLetterInput
			if c.Request.ContentLength > 0 {
				if err := c.ShouldBindJSON(&in); err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
					return
				}
			}
			message, err := deadLetters.Replay(c.Request.Context(), id, in)
			if err != nil {
				switch {
				case errors.Is(err, repository.ErrDeadLetterNotFound):
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				case errors.Is(err, service.ErrDeadLetterReplayUnavailable):
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
				case errors.Is(err, service.ErrDeadLetterReplayStale):
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				default:
					c.JSON(http.StatusInternalServerError, gin.H{"error": "replay dead letter failed"})
				}
				return
			}
			c.JSON(http.StatusOK, message)
		})
	}

	if aiGovernance != nil {
		controlV1.GET("/ai/routing-policy", func(c *gin.Context) {
			c.JSON(http.StatusOK, aiGovernance.Policy())
		})

		controlV1.PUT("/ai/routing-policy", func(c *gin.Context) {
			var in service.UpdateAIRoutingPolicyInput
			if err := c.ShouldBindJSON(&in); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
				return
			}
			policy, err := aiGovernance.UpdatePolicy(c.Request.Context(), in)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, policy)
		})

		controlV1.GET("/ai/tenant-quota-policy", func(c *gin.Context) {
			c.JSON(http.StatusOK, aiGovernance.Policy())
		})

		controlV1.PUT("/ai/tenant-quota-policy", func(c *gin.Context) {
			var in service.UpdateAITenantQuotaPolicyInput
			if err := c.ShouldBindJSON(&in); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
				return
			}
			policy, err := aiGovernance.UpdateTenantQuotaPolicy(c.Request.Context(), in)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, policy)
		})

		controlV1.DELETE("/ai/tenant-quota-policy", func(c *gin.Context) {
			policy, err := aiGovernance.ResetTenantQuotaPolicy(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "reset AI tenant quota policy failed"})
				return
			}
			c.JSON(http.StatusOK, policy)
		})

		controlV1.GET("/ai/budgets", func(c *gin.Context) {
			accounts, err := aiGovernance.ListBudgets(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list AI budgets failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": accounts})
		})

		controlV1.GET("/ai/budgets/:scope/:key", func(c *gin.Context) {
			account, err := aiGovernance.GetBudget(c.Request.Context(), c.Param("scope"), c.Param("key"))
			if err != nil {
				if errors.Is(err, repository.ErrAIBudgetAccountNotFound) {
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, account)
		})

		controlV1.PUT("/ai/budgets/:scope/:key", func(c *gin.Context) {
			var in service.UpdateAIBudgetInput
			if err := c.ShouldBindJSON(&in); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
				return
			}
			account, err := aiGovernance.UpdateBudget(c.Request.Context(), c.Param("scope"), c.Param("key"), in)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, account)
		})

		controlV1.GET("/tasks/:id/ai-route-audits", func(c *gin.Context) {
			taskID, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
				return
			}
			audits, err := aiGovernance.ListAudits(c.Request.Context(), taskID)
			if err != nil {
				if errors.Is(err, repository.ErrTaskNotFound) {
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list AI route audits failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": audits})
		})

		controlV1.POST("/tasks/:id/ai-route-audits/:audit_id/replay", func(c *gin.Context) {
			taskID, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
				return
			}
			auditID, err := strconv.ParseUint(c.Param("audit_id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid audit id"})
				return
			}
			var in struct {
				Strategy string `json:"strategy"`
			}
			if c.Request.ContentLength > 0 {
				if err := c.ShouldBindJSON(&in); err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
					return
				}
			}
			replay, err := aiGovernance.ReplayAudit(c.Request.Context(), taskID, auditID, in.Strategy)
			if err != nil {
				if errors.Is(err, repository.ErrAIRouteAuditNotFound) {
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, replay)
		})
	}

	if controlPlaneAudits != nil {
		controlV1.GET("/control-plane/audits", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
			offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
			audits, err := controlPlaneAudits.List(c.Request.Context(), limit, offset)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list control-plane audits failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": audits})
		})

		controlV1.GET("/control-plane/audits/verify", func(c *gin.Context) {
			report, err := controlPlaneAudits.Verify(c.Request.Context())
			if err != nil {
				appmetrics.ControlPlaneAuditIntegrity.Set(0)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "verify control-plane audit chain failed"})
				return
			}
			if !report.Valid {
				appmetrics.ControlPlaneAuditIntegrity.Set(0)
				c.JSON(http.StatusConflict, report)
				return
			}
			appmetrics.ControlPlaneAuditIntegrity.Set(1)
			c.JSON(http.StatusOK, report)
		})

		controlV1.GET("/control-plane/audits/export", func(c *gin.Context) {
			after, err := strconv.ParseUint(c.DefaultQuery("after_sequence", "0"), 10, 64)
			if err != nil {
				appmetrics.ControlPlaneAuditExports.WithLabelValues("invalid_request").Inc()
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_sequence"})
				return
			}
			limit, err := strconv.Atoi(c.DefaultQuery("limit", "500"))
			if err != nil || limit <= 0 {
				appmetrics.ControlPlaneAuditExports.WithLabelValues("invalid_request").Inc()
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
				return
			}
			audits, checkpoint, err := controlPlaneAudits.Export(c.Request.Context(), after, limit)
			if err != nil {
				appmetrics.ControlPlaneAuditExports.WithLabelValues("error").Inc()
				c.JSON(http.StatusInternalServerError, gin.H{"error": "export control-plane audits failed"})
				return
			}
			if after < checkpoint.RetentionAnchorSequence {
				appmetrics.ControlPlaneAuditExports.WithLabelValues("retention_gap").Inc()
				c.JSON(http.StatusGone, gin.H{
					"error":                     "requested audit sequence is older than the retention anchor",
					"retention_anchor_sequence": checkpoint.RetentionAnchorSequence,
					"retention_anchor_hash":     checkpoint.RetentionAnchorHash,
				})
				return
			}
			c.Header("Content-Type", "application/x-ndjson")
			c.Header("X-TaskFlow-Audit-Head-Hash", checkpoint.HeadHash)
			c.Header("X-TaskFlow-Audit-Last-Sequence", strconv.FormatUint(checkpoint.LastSequence, 10))
			c.Header("X-TaskFlow-Audit-Retention-Anchor-Sequence", strconv.FormatUint(checkpoint.RetentionAnchorSequence, 10))
			c.Status(http.StatusOK)
			encoder := json.NewEncoder(c.Writer)
			for _, audit := range audits {
				if err := encoder.Encode(gin.H{"type": "audit", "audit": audit}); err != nil {
					appmetrics.ControlPlaneAuditExports.WithLabelValues("write_error").Inc()
					return
				}
			}
			if err := encoder.Encode(gin.H{"type": "checkpoint", "checkpoint": checkpoint}); err != nil {
				appmetrics.ControlPlaneAuditExports.WithLabelValues("write_error").Inc()
				return
			}
			appmetrics.ControlPlaneAuditExports.WithLabelValues("success").Inc()
		})

		controlV1.GET("/control-plane/siem/status", func(c *gin.Context) {
			stats, err := controlPlaneAudits.DeliveryStats(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "read SIEM delivery status failed"})
				return
			}
			c.JSON(http.StatusOK, stats)
		})

		if siemReceivers != nil {
			controlV1.GET("/control-plane/siem/receivers/status", func(c *gin.Context) {
				status, err := siemReceivers.ReceiverStatus(c.Request.Context())
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, status)
			})

			controlV1.POST("/control-plane/siem/receivers/sync", func(c *gin.Context) {
				replayed, err := siemReceivers.SyncReceiverAntiEntropy(c.Request.Context())
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"status": "converged", "replayed": replayed})
			})

			controlV1.GET("/control-plane/siem/receivers/intents", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
				items, err := siemReceivers.ListPromotionIntents(c.Request.Context(), limit)
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.POST("/control-plane/siem/receivers/intents", func(c *gin.Context) {
				var request struct {
					TargetReceiver string `json:"target_receiver"`
					ExpectedTerm   uint64 `json:"expected_term"`
					Reason         string `json:"reason"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.TargetReceiver == "" || request.ExpectedTerm == 0 || request.Reason == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "target_receiver, positive expected_term and reason are required"})
					return
				}
				intent, err := siemReceivers.RequestPromotionIntent(c.Request.Context(), request.TargetReceiver, request.ExpectedTerm, request.Reason, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusCreated, intent)
			})

			controlV1.POST("/control-plane/siem/receivers/intents/:id/approve", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid promotion intent id"})
					return
				}
				var request struct {
					Note string `json:"note"`
				}
				if c.Request.ContentLength > 0 {
					if err := c.ShouldBindJSON(&request); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
						return
					}
				}
				intent, err := siemReceivers.ApprovePromotionIntent(c.Request.Context(), id, request.Note, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, intent)
			})

			controlV1.GET("/control-plane/siem/receivers/intents/:id/approvals", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid promotion intent id"})
					return
				}
				items, err := siemReceivers.PromotionApprovals(c.Request.Context(), id)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.POST("/control-plane/siem/receivers/intents/:id/execute", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid promotion intent id"})
					return
				}
				result, err := siemReceivers.ExecutePromotionIntent(c.Request.Context(), id, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, result)
			})

			controlV1.POST("/control-plane/siem/receivers/break-glass", func(c *gin.Context) {
				var request struct {
					TargetReceiver string `json:"target_receiver"`
					ExpectedTerm   uint64 `json:"expected_term"`
					IncidentID     string `json:"incident_id"`
					Reason         string `json:"reason"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.TargetReceiver == "" || request.ExpectedTerm == 0 || request.IncidentID == "" || request.Reason == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "target_receiver, positive expected_term, incident_id and reason are required"})
					return
				}
				result, err := siemReceivers.BreakGlassPromotion(c.Request.Context(), request.TargetReceiver, request.ExpectedTerm, request.IncidentID, request.Reason, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, result)
			})

			controlV1.GET("/control-plane/siem/receivers/term-history", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
				items, verification, err := siemReceivers.ReceiverTermHistory(c.Request.Context(), limit)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
				status := http.StatusOK
				if !verification.Valid {
					status = http.StatusConflict
				}
				c.JSON(status, gin.H{"items": items, "verification": verification})
			})

			controlV1.POST("/control-plane/siem/receivers/term-history/anchor", func(c *gin.Context) {
				anchor, err := siemReceivers.EnsureTermAnchor(c.Request.Context())
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, anchor)
			})

			controlV1.GET("/control-plane/siem/incidents", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
				items, err := siemReceivers.IncidentEvidence(c.Request.Context(), limit)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.GET("/control-plane/siem/governance/status", func(c *gin.Context) {
				status, err := siemReceivers.GovernanceStatus(c.Request.Context())
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, status)
			})

			controlV1.GET("/control-plane/siem/governance/dead", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
				items, err := siemReceivers.GovernanceDead(c.Request.Context(), c.DefaultQuery("kind", "all"), limit)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.GET("/control-plane/siem/governance/remediations", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
				items, err := siemReceivers.GovernanceRemediations(c.Request.Context(), limit)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.GET("/control-plane/siem/governance/replica-receipts", func(c *gin.Context) {
				limit, _ := strconv.Atoi(c.DefaultQuery("limit", "200"))
				targetID, _ := strconv.ParseUint(c.DefaultQuery("target_id", "0"), 10, 64)
				items, err := siemReceivers.GovernanceReplicaReceipts(c.Request.Context(), c.DefaultQuery("kind", "all"), targetID, limit)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, gin.H{"items": items})
			})

			controlV1.POST("/control-plane/siem/governance/dead/:kind/:id/remediations", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid governance dead id"})
					return
				}
				var request struct {
					OperationKey string `json:"operation_key"`
					Reason       string `json:"reason"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.OperationKey == "" || request.Reason == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "operation_key and remediation reason are required"})
					return
				}
				remediation, err := siemReceivers.RequestGovernanceRemediation(
					c.Request.Context(), c.Param("kind"), id, request.OperationKey, request.Reason, c.GetString(controlPlaneOperatorKey),
				)
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusAccepted, remediation)
			})

			controlV1.POST("/control-plane/siem/governance/remediations/:id/approve", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid governance remediation id"})
					return
				}
				var request struct {
					Note string `json:"note"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.Note == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "approval note is required"})
					return
				}
				remediation, err := siemReceivers.ApproveGovernanceRemediation(
					c.Request.Context(), id, request.Note, c.GetString(controlPlaneOperatorKey),
				)
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusAccepted, remediation)
			})

			controlV1.POST("/control-plane/siem/governance/remediations/:id/reject", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid governance remediation id"})
					return
				}
				var request struct {
					Note string `json:"note"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.Note == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "rejection note is required"})
					return
				}
				remediation, err := siemReceivers.RejectGovernanceRemediation(c.Request.Context(), id, request.Note, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, remediation)
			})

			controlV1.POST("/control-plane/siem/governance/remediations/:id/cancel", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid governance remediation id"})
					return
				}
				var request struct {
					Note string `json:"note"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.Note == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "cancellation note is required"})
					return
				}
				remediation, err := siemReceivers.CancelGovernanceRemediation(c.Request.Context(), id, request.Note, c.GetString(controlPlaneOperatorKey))
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, remediation)
			})

			controlV1.POST("/control-plane/siem/governance/dead/:kind/:id/requeue", func(c *gin.Context) {
				id, err := strconv.ParseUint(c.Param("id"), 10, 64)
				if err != nil || id == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid governance dead id"})
					return
				}
				var request struct {
					Reason string `json:"reason"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.Reason == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "remediation reason is required"})
					return
				}
				remediation, err := siemReceivers.RequeueGovernanceDead(
					c.Request.Context(), c.Param("kind"), id, request.Reason, c.GetString(controlPlaneOperatorKey),
				)
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusAccepted, remediation)
			})

			controlV1.POST("/control-plane/siem/receivers/promote", func(c *gin.Context) {
				var request struct {
					TargetReceiver string `json:"target_receiver"`
					ExpectedTerm   uint64 `json:"expected_term"`
				}
				if err := c.ShouldBindJSON(&request); err != nil || request.TargetReceiver == "" || request.ExpectedTerm == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "target_receiver and positive expected_term are required"})
					return
				}
				result, err := siemReceivers.PromoteReceiver(
					c.Request.Context(),
					request.TargetReceiver,
					request.ExpectedTerm,
					c.GetString(controlPlaneOperatorKey),
				)
				if err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, result)
			})
		}

		controlV1.GET("/control-plane/siem/deliveries", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
			deliveries, err := controlPlaneAudits.ListDeliveries(c.Request.Context(), c.Query("state"), limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list SIEM deliveries failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": deliveries})
		})

		controlV1.GET("/control-plane/siem/receipts", func(c *gin.Context) {
			after, err := strconv.ParseUint(c.DefaultQuery("after_sequence", "0"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_sequence"})
				return
			}
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
			receipts, err := controlPlaneAudits.ListDeliveryReceipts(c.Request.Context(), after, limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list SIEM delivery receipts failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": receipts})
		})

		controlV1.GET("/control-plane/siem/dlq", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
			rows, err := controlPlaneAudits.ListDeliveryDLQ(c.Request.Context(), limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "list SIEM delivery DLQ failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": rows})
		})

		controlV1.POST("/control-plane/siem/dlq/:id/replay", func(c *gin.Context) {
			id, err := strconv.ParseUint(c.Param("id"), 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SIEM delivery DLQ id"})
				return
			}
			if err := controlPlaneAudits.ReplayDeliveryDLQ(c.Request.Context(), id); err != nil {
				if errors.Is(err, repository.ErrControlPlaneDeliveryDLQNotFound) {
					c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusAccepted, gin.H{"status": "queued"})
		})
	}

	return r
}
