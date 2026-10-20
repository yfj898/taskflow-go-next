package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrTaskNotFound = errors.New("task not found")
var ErrOutboxNotFound = errors.New("outbox event not found")

type TaskRepository struct {
	db                       *gorm.DB
	controlPlaneSIEMDelivery bool
	managedWorkflowSchema    bool

	prepareOnce      sync.Once
	prepareErr       error
	insertTaskStmt   *sql.Stmt
	insertOutboxStmt *sql.Stmt
}

func NewTaskRepository(db *gorm.DB) *TaskRepository { return &TaskRepository{db: db} }

func (r *TaskRepository) ConfigureControlPlaneSIEMDelivery(enabled bool) {
	r.controlPlaneSIEMDelivery = enabled
}

// ConfigureManagedWorkflowSchema skips implicit GORM changes to existing Saga
// tables and requires an explicit, versioned workflow schema migration.
func (r *TaskRepository) ConfigureManagedWorkflowSchema(enabled bool) {
	r.managedWorkflowSchema = enabled
}

const managedWorkflowMigrationVersion = "20261009_cloud_workflow_schema_expand_v1"

// VerifyManagedWorkflowSchema checks the migration journal plus essential
// indexes/columns. It never changes data or schema. A failed check blocks
// startup in managed mode instead of invoking AutoMigrate behind the operator.
func (r *TaskRepository) VerifyManagedWorkflowSchema() error {
	m := r.db.Migrator()
	if !m.HasTable(&domain.CloudWorkflow{}) || !m.HasTable(&domain.CloudWorkflowStep{}) {
		return errors.New("managed CloudWorkflow schema missing tables; run schema-migrate --phase=workflow-expand")
	}
	for _, name := range []string{"ux_cloud_workflow_order", "idx_cloud_workflow_runnable", "idx_cloud_workflow_next_reconcile"} {
		if !m.HasIndex(&domain.CloudWorkflow{}, name) {
			return fmt.Errorf("managed CloudWorkflow missing index %s; run schema-migrate --phase=workflow-expand", name)
		}
	}
	if !m.HasIndex(&domain.CloudWorkflowStep{}, "idx_cloud_workflow_step") || !m.HasColumn(&domain.CloudWorkflow{}, "next_reconcile_at") {
		return errors.New("managed CloudWorkflow missing required column or step index")
	}
	if m.HasIndex(&domain.CloudWorkflow{}, "ux_cloud_workflow_resource") {
		return errors.New("legacy one-workflow-per-resource unique index must be removed by workflow-expand")
	}
	var n int64
	if err := r.db.Raw("SELECT COUNT(*) FROM taskflow_schema_migrations WHERE version = ?", managedWorkflowMigrationVersion).Scan(&n).Error; err != nil || n != 1 {
		return fmt.Errorf("managed CloudWorkflow migration journal not ready: count=%d error=%v", n, err)
	}
	return nil
}

// MigrateWorkflowSchemaExplicit is an operator-only, serialized migration;
// normal application startup never executes it when managed mode is enabled.
func (r *TaskRepository) MigrateWorkflowSchemaExplicit() error {
	return r.withMigrationLock(func() error {
		if err := r.db.AutoMigrate(&domain.CloudWorkflow{}, &domain.CloudWorkflowStep{}); err != nil {
			return fmt.Errorf("expand CloudWorkflow tables: %w", err)
		}
		if err := NormalizeCloudWorkflowIndexes(r.db); err != nil {
			return fmt.Errorf("normalize CloudWorkflow index: %w", err)
		}
		if err := r.db.Exec(`CREATE TABLE IF NOT EXISTS taskflow_schema_migrations (
			version VARCHAR(128) NOT NULL PRIMARY KEY,
			description VARCHAR(255) NOT NULL,
			applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		) ENGINE=InnoDB`).Error; err != nil {
			return err
		}
		if err := r.db.Exec("INSERT INTO taskflow_schema_migrations (version, description) VALUES (?, ?) ON DUPLICATE KEY UPDATE version=version", managedWorkflowMigrationVersion, "operator-managed CloudWorkflow and CloudWorkflowStep GORM schema expand").Error; err != nil {
			return err
		}
		return r.VerifyManagedWorkflowSchema()
	})
}

const insertTaskSQL = `
	INSERT INTO tasks
		(name, type, payload, status, result, retry_count, last_error,
		 trace_parent, trace_state, enqueue_generation, enqueued_generation,
		 enqueue_lease_until, next_run_at, lease_until, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const insertOutboxSQL = `
	INSERT INTO outbox_events
		(task_id, event_type, attempts, last_error, published_at, lease_until, created_at, updated_at)
	VALUES (?, ?, 0, '', NULL, NULL, ?, ?)`

func (r *TaskRepository) prepareCreateStatements() error {
	r.prepareOnce.Do(func() {
		sqlDB, err := r.db.DB()
		if err != nil {
			r.prepareErr = err
			return
		}

		r.insertTaskStmt, err = sqlDB.Prepare(insertTaskSQL)
		if err != nil {
			r.prepareErr = err
			return
		}
		r.insertOutboxStmt, err = sqlDB.Prepare(insertOutboxSQL)
		if err != nil {
			_ = r.insertTaskStmt.Close()
			r.insertTaskStmt = nil
			r.prepareErr = err
		}
	})
	return r.prepareErr
}

func (r *TaskRepository) Migrate() error {
	return r.withMigrationLock(func() error {
		// CloudOrder's idempotency uniqueness is a rolling-upgrade boundary.
		// It must be migrated by the explicit schema command after all old
		// writers have drained. AutoMigrate on an existing table can silently
		// replace the old globally unique index before that is safe.
		if !r.db.Migrator().HasTable(&domain.CloudOrder{}) {
			if err := r.db.Migrator().CreateTable(&domain.CloudOrder{}); err != nil {
				return fmt.Errorf("initialize cloud orders: %w", err)
			}
		}
		models := []any{
			&domain.Task{},
			&domain.OutboxEvent{},
			&domain.DeadLetterMessage{},
			&domain.DeadLetterReplayAudit{},
			&domain.DeadLetterArchive{},
			&domain.DeadLetterArchiveReplica{},
			&domain.AIRouteAudit{},
			&domain.AIRoutingPolicy{},
			&domain.AIBudgetAccount{},
			&domain.AIBudgetReservation{},
			&domain.ControlPlaneAudit{},
			&domain.ControlPlaneAuditChainState{},
			&domain.ControlPlaneAuditDelivery{},
			&domain.ControlPlaneAuditDeliveryReceipt{},
			&domain.ControlPlaneAuditDeliveryDLQ{},
			&domain.ControlPlaneSIEMDeliveryCoordinatorState{},
			&domain.ControlPlaneSIEMReceiverRoutingState{},
			&domain.ControlPlaneSIEMPromotionIntent{},
			&domain.ControlPlaneSIEMPromotionApproval{},
			&domain.ControlPlaneSIEMReceiverTermHistory{},
			&domain.ControlPlaneSIEMTermLedgerAnchor{},
			&domain.ControlPlaneSIEMIncidentEvidence{},
			&domain.ControlPlaneSIEMGovernanceRemediation{},
			&domain.ControlPlaneSIEMGovernanceReplicaReceipt{},
			&domain.ControlPlaneSIEMGovernanceReplicaKeyState{},
			&domain.CloudResource{},
			&domain.CloudTenantQuota{},
			&domain.CloudTenantQuotaBucket{},
		}
		if !r.managedWorkflowSchema {
			models = append(models, &domain.CloudWorkflow{}, &domain.CloudWorkflowStep{})
		}
		if err := r.db.AutoMigrate(models...); err != nil {
			return err
		}
		if r.managedWorkflowSchema {
			return r.VerifyManagedWorkflowSchema()
		}
		return NormalizeCloudWorkflowIndexes(r.db)
	})
}

func (r *TaskRepository) withMigrationLock(migrate func() error) error {
	if migrate == nil {
		return errors.New("nil migration function")
	}
	if r.db == nil || r.db.Dialector == nil || r.db.Dialector.Name() != "mysql" {
		return migrate()
	}
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 125*time.Second)
	defer cancel()
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open migration lock connection: %w", err)
	}
	defer conn.Close()

	const lockName = "taskflow:schema:migrate"
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, 120).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return errors.New("timed out acquiring migration lock")
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		var released sql.NullInt64
		_ = conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
	}()

	return migrate()
}

func (r *TaskRepository) Create(ctx context.Context, task *domain.Task) error {
	return r.db.WithContext(ctx).Create(task).Error
}

func (r *TaskRepository) CreateWithOutbox(ctx context.Context, task *domain.Task) error {
	// This is the hottest write path in the service. Keep the same atomic
	// task+outbox transaction semantics while avoiding GORM's reflection-heavy
	// Create callbacks for two inserts on every request.
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	if err := r.prepareCreateStatements(); err != nil {
		return err
	}

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now()
	result, err := tx.StmtContext(ctx, r.insertTaskStmt).ExecContext(ctx,
		task.Name,
		task.Type,
		task.Payload,
		task.Status,
		task.Result,
		task.RetryCount,
		task.LastError,
		task.TraceParent,
		task.TraceState,
		task.EnqueueGeneration,
		task.EnqueuedGeneration,
		task.EnqueueLeaseUntil,
		task.NextRunAt,
		task.LeaseUntil,
		now,
		now,
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	task.ID = uint64(id)
	task.CreatedAt = now
	task.UpdatedAt = now

	if _, err := tx.StmtContext(ctx, r.insertOutboxStmt).ExecContext(ctx,
		task.ID,
		domain.OutboxTaskCreated,
		now,
		now,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *TaskRepository) Get(ctx context.Context, id uint64) (*domain.Task, error) {
	var task domain.Task
	if err := r.db.WithContext(ctx).First(&task, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	return &task, nil
}

func (r *TaskRepository) List(ctx context.Context, limit, offset int) ([]domain.Task, error) {
	var tasks []domain.Task
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Offset(offset).Find(&tasks).Error
	return tasks, err
}

func (r *TaskRepository) ListPublicTasks(ctx context.Context, limit, offset int) ([]domain.Task, error) {
	var tasks []domain.Task
	err := r.db.WithContext(ctx).Where("type <> ?", domain.VerificationTaskType).Order("id DESC").Limit(limit).Offset(offset).Find(&tasks).Error
	return tasks, err
}

func (r *TaskRepository) FindReadyPending(ctx context.Context, now time.Time, limit int) ([]domain.Task, error) {
	var tasks []domain.Task
	err := r.db.WithContext(ctx).
		Where("status = ? AND next_run_at <= ? AND enqueued_generation < enqueue_generation AND (enqueue_lease_until IS NULL OR enqueue_lease_until < ?)", domain.TaskPending, now, now).
		Order("next_run_at ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

func (r *TaskRepository) Claim(ctx context.Context, id, generation uint64, leaseUntil time.Time) (bool, error) {
	res := r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("id = ? AND status = ? AND enqueue_generation = ?", id, domain.TaskPending, generation).
		Updates(map[string]any{
			"status":              domain.TaskRunning,
			"lease_until":         leaseUntil,
			"enqueue_lease_until": nil,
		})
	return res.RowsAffected == 1, res.Error
}

func (r *TaskRepository) RenewLease(ctx context.Context, id uint64, leaseUntil time.Time) (bool, error) {
	res := r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("id = ? AND status = ?", id, domain.TaskRunning).
		Update("lease_until", leaseUntil)
	return res.RowsAffected == 1, res.Error
}

func (r *TaskRepository) RecoverStaleRunning(ctx context.Context, now, legacyBefore time.Time) (int64, error) {
	recovered, err := r.recoverStaleVerifications(ctx, now, legacyBefore)
	if err != nil {
		return recovered, err
	}
	res := r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("type <> ?", domain.VerificationTaskType).
		Where(
			"status = ? AND ((lease_until IS NOT NULL AND lease_until < ?) OR (lease_until IS NULL AND updated_at < ?))",
			domain.TaskRunning, now, legacyBefore,
		).
		Updates(map[string]any{
			"status":              domain.TaskPending,
			"last_error":          "recovered stale running task",
			"next_run_at":         time.Now(),
			"lease_until":         nil,
			"enqueue_generation":  gorm.Expr("enqueue_generation + 1"),
			"enqueue_lease_until": nil,
		})
	return recovered + res.RowsAffected, res.Error
}

func (r *TaskRepository) Transition(ctx context.Context, id uint64, from, to domain.TaskStatus, updates map[string]any) (bool, error) {
	prepareTransitionUpdates(from, to, updates)
	updates["status"] = to
	res := r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("id = ? AND status = ?", id, from).
		Updates(updates)
	return res.RowsAffected == 1, res.Error
}

func (r *TaskRepository) TransitionWithAIRouteAudit(
	ctx context.Context,
	id uint64,
	from, to domain.TaskStatus,
	updates map[string]any,
	audit *domain.AIRouteAudit,
) (bool, error) {
	return r.TransitionWithAIRouteAuditAndBudget(ctx, id, from, to, updates, audit, nil, false, 0, 0)
}

func (r *TaskRepository) TransitionWithAIRouteAuditAndBudget(
	ctx context.Context,
	id uint64,
	from, to domain.TaskStatus,
	updates map[string]any,
	audit *domain.AIRouteAudit,
	reservation *domain.AIBudgetReservation,
	settleBudget bool,
	actualUSD float64,
	actualTokens int64,
) (bool, error) {
	updated := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		prepareTransitionUpdates(from, to, updates)
		updates["status"] = to
		res := tx.Model(&domain.Task{}).
			Where("id = ? AND status = ?", id, from).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return nil
		}
		updated = true
		if audit != nil {
			if err := tx.Create(audit).Error; err != nil {
				return err
			}
		}
		if err := r.finalizeAIBudgetTx(tx, reservation, settleBudget, actualUSD, actualTokens); err != nil {
			return err
		}
		return nil
	})
	return updated, err
}

func prepareTransitionUpdates(from, to domain.TaskStatus, updates map[string]any) {
	if from == domain.TaskRunning && to == domain.TaskPending {
		updates["enqueue_generation"] = gorm.Expr("enqueue_generation + 1")
		updates["enqueue_lease_until"] = nil
	}
}

// ClaimNextOutbox holds the row lock only long enough to establish a short
// lease. Broker I/O happens after this transaction commits, so a slow or down
// broker cannot pin a MySQL transaction. If the dispatcher crashes, another
// instance can reclaim the event after leaseUntil.
func (r *TaskRepository) ClaimNextOutbox(ctx context.Context, now, leaseUntil time.Time) (*domain.OutboxEvent, error) {
	var event domain.OutboxEvent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("published_at IS NULL AND (lease_until IS NULL OR lease_until < ?)", now).
			Order("id ASC").
			Limit(1).
			Find(&event)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrOutboxNotFound
		}
		return tx.Model(&event).Update("lease_until", leaseUntil).Error
	})
	if err != nil {
		return nil, err
	}
	event.LeaseUntil = &leaseUntil
	return &event, nil
}

func (r *TaskRepository) ClaimOutboxBatch(ctx context.Context, now, leaseUntil time.Time, limit int) ([]domain.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	events := make([]domain.OutboxEvent, 0, limit)
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("published_at IS NULL AND (lease_until IS NULL OR lease_until < ?)", now).
			Order("id ASC").
			Limit(limit).
			Find(&events)
		if res.Error != nil {
			return res.Error
		}
		if len(events) == 0 {
			return ErrOutboxNotFound
		}

		ids := make([]uint64, 0, len(events))
		for i := range events {
			ids = append(ids, events[i].ID)
		}
		return tx.Model(&domain.OutboxEvent{}).
			Where("id IN ? AND published_at IS NULL", ids).
			Update("lease_until", leaseUntil).Error
	})
	if err != nil {
		return nil, err
	}
	for i := range events {
		events[i].LeaseUntil = &leaseUntil
	}
	return events, nil
}

func (r *TaskRepository) MarkOutboxPublished(ctx context.Context, id uint64, publishedAt time.Time) error {
	return r.db.WithContext(ctx).Model(&domain.OutboxEvent{}).
		Where("id = ? AND published_at IS NULL", id).
		Updates(map[string]any{
			"attempts":     gorm.Expr("attempts + 1"),
			"last_error":   "",
			"lease_until":  nil,
			"published_at": publishedAt,
		}).Error
}

func (r *TaskRepository) MarkOutboxPublishedBatch(ctx context.Context, ids []uint64, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&domain.OutboxEvent{}).
		Where("id IN ? AND published_at IS NULL", ids).
		Updates(map[string]any{
			"attempts":     gorm.Expr("attempts + 1"),
			"last_error":   "",
			"lease_until":  nil,
			"published_at": publishedAt,
		}).Error
}

func (r *TaskRepository) MarkOutboxFailed(ctx context.Context, id uint64, cause error) error {
	return r.db.WithContext(ctx).Model(&domain.OutboxEvent{}).
		Where("id = ? AND published_at IS NULL", id).
		Updates(map[string]any{
			"attempts":    gorm.Expr("attempts + 1"),
			"last_error":  cause.Error(),
			"lease_until": nil,
		}).Error
}

func (r *TaskRepository) MarkOutboxFailedBatch(ctx context.Context, ids []uint64, cause error) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&domain.OutboxEvent{}).
		Where("id IN ? AND published_at IS NULL", ids).
		Updates(map[string]any{
			"attempts":    gorm.Expr("attempts + 1"),
			"last_error":  cause.Error(),
			"lease_until": nil,
		}).Error
}
