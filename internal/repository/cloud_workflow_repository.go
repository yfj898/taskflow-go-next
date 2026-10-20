package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrCloudWorkflowNotFound    = errors.New("cloud workflow not found")
	ErrCloudWorkflowNotRunnable = errors.New("cloud workflow is not runnable")
)

type CloudWorkflowState struct {
	Workflow *domain.CloudWorkflow      `json:"workflow"`
	Resource *domain.CloudResource      `json:"resource"`
	Order    *domain.CloudOrder         `json:"order"`
	Steps    []domain.CloudWorkflowStep `json:"steps"`
}

func (r *CloudResourceRepository) CreateProvisionWorkflow(
	ctx context.Context,
	resource *domain.CloudResource,
	order *domain.CloudOrder,
	demand cloudcontrol.QuotaDemand,
	workflowSpec string,
) (*domain.CloudWorkflow, []domain.CloudWorkflowStep, error) {
	if !cloudTenantPermitted(ctx, resource.TenantID) || resource.TenantID != order.TenantID {
		return nil, nil, ErrCloudTenantAccessDenied
	}
	workflow := &domain.CloudWorkflow{}
	steps := []domain.CloudWorkflowStep{}
	err := r.quotaTransaction(ctx, func(tx *gorm.DB) error {
		resource.OrchestrationMode = domain.CloudOrchestrationSaga
		resource.DesiredBundleSpec = workflowSpec
		resource.QuotaCPUMilli = demand.CPUMilli
		resource.QuotaMemoryMiB = demand.MemoryMiB
		resource.QuotaReplicas = demand.Replicas
		if err := tx.Create(resource).Error; err != nil {
			return err
		}

		order.ResourceID = resource.ID
		order.RequestedGeneration = resource.Generation
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		now := time.Now()
		workflow = &domain.CloudWorkflow{
			ResourceID:       resource.ID,
			OrderID:          order.ID,
			TenantID:         resource.TenantID,
			Kind:             domain.CloudWorkflowKindProvision,
			Status:           domain.CloudWorkflowPending,
			Spec:             workflowSpec,
			LastTransitionAt: &now,
		}
		if err := tx.Create(workflow).Error; err != nil {
			return err
		}

		quotaOutput, _ := json.Marshal(demand)
		steps = []domain.CloudWorkflowStep{
			{
				WorkflowID:  workflow.ID,
				Name:        domain.CloudWorkflowStepQuota,
				Position:    0,
				Status:      domain.CloudWorkflowStepSucceeded,
				Output:      string(quotaOutput),
				StartedAt:   &now,
				CompletedAt: &now,
			},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepNetwork, Position: 1, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepStorage, Position: 2, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCompute, Position: 3, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCommit, Position: 4, Status: domain.CloudWorkflowStepPending},
		}
		if err := tx.Create(&steps).Error; err != nil {
			return err
		}
		// Keep quota admission in this same ACID transaction, but acquire the
		// tenant row lock last so a hot tenant does not hold it across the
		// Resource/Order/Workflow/Step inserts above.
		if err := r.reserveQuotaForResource(tx, resource, demand); err != nil {
			return err
		}
		if resource.QuotaBucketID != 0 {
			return tx.Model(&domain.CloudResource{}).
				Where("id = ?", resource.ID).
				Update("quota_bucket_id", resource.QuotaBucketID).Error
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return workflow, steps, nil
}

func (r *CloudResourceRepository) FindWorkflowByOrderID(ctx context.Context, orderID uint64) (*domain.CloudWorkflow, error) {
	var workflow domain.CloudWorkflow
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).Where("order_id = ?", orderID).First(&workflow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudWorkflowNotFound
		}
		return nil, err
	}
	return &workflow, nil
}

func (r *CloudResourceRepository) GetWorkflowState(ctx context.Context, id uint64) (*CloudWorkflowState, error) {
	var workflow domain.CloudWorkflow
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).First(&workflow, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudWorkflowNotFound
		}
		return nil, err
	}
	var resource domain.CloudResource
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).First(&resource, workflow.ResourceID).Error; err != nil {
		return nil, err
	}
	var order domain.CloudOrder
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).First(&order, workflow.OrderID).Error; err != nil {
		return nil, err
	}
	var steps []domain.CloudWorkflowStep
	if err := r.db.WithContext(ctx).
		Where("workflow_id = ?", workflow.ID).
		Order("position ASC").
		Find(&steps).Error; err != nil {
		return nil, err
	}
	return &CloudWorkflowState{Workflow: &workflow, Resource: &resource, Order: &order, Steps: steps}, nil
}

// GetClaimedWorkflowStates bulk-loads the state required to advance an
// already-claimed workflow batch. ClaimRunnableWorkflows already returned the
// coordinator rows, so the optimized path needs only three reads for the
// entire batch: resources, orders and steps.
func (r *CloudResourceRepository) GetClaimedWorkflowStates(ctx context.Context, workflows []domain.CloudWorkflow) ([]*CloudWorkflowState, error) {
	if len(workflows) == 0 {
		return nil, nil
	}
	resourceIDs := make([]uint64, 0, len(workflows))
	orderIDs := make([]uint64, 0, len(workflows))
	workflowIDs := make([]uint64, 0, len(workflows))
	for i := range workflows {
		resourceIDs = append(resourceIDs, workflows[i].ResourceID)
		orderIDs = append(orderIDs, workflows[i].OrderID)
		workflowIDs = append(workflowIDs, workflows[i].ID)
	}

	var resources []domain.CloudResource
	if err := r.db.WithContext(ctx).Where("id IN ?", resourceIDs).Find(&resources).Error; err != nil {
		return nil, err
	}
	var orders []domain.CloudOrder
	if err := r.db.WithContext(ctx).Where("id IN ?", orderIDs).Find(&orders).Error; err != nil {
		return nil, err
	}
	var steps []domain.CloudWorkflowStep
	if err := r.db.WithContext(ctx).
		Where("workflow_id IN ?", workflowIDs).
		Order("workflow_id ASC, position ASC").
		Find(&steps).Error; err != nil {
		return nil, err
	}

	resourceByID := make(map[uint64]domain.CloudResource, len(resources))
	for i := range resources {
		resourceByID[resources[i].ID] = resources[i]
	}
	orderByID := make(map[uint64]domain.CloudOrder, len(orders))
	for i := range orders {
		orderByID[orders[i].ID] = orders[i]
	}
	stepsByWorkflow := make(map[uint64][]domain.CloudWorkflowStep, len(workflows))
	for i := range steps {
		stepsByWorkflow[steps[i].WorkflowID] = append(stepsByWorkflow[steps[i].WorkflowID], steps[i])
	}

	states := make([]*CloudWorkflowState, 0, len(workflows))
	for i := range workflows {
		workflow := workflows[i]
		resource, ok := resourceByID[workflow.ResourceID]
		if !ok {
			return nil, ErrCloudWorkflowNotFound
		}
		order, ok := orderByID[workflow.OrderID]
		if !ok {
			return nil, ErrCloudWorkflowNotFound
		}
		workflowSteps := stepsByWorkflow[workflow.ID]
		if len(workflowSteps) == 0 {
			return nil, ErrCloudWorkflowNotFound
		}
		states = append(states, &CloudWorkflowState{
			Workflow: &workflow,
			Resource: &resource,
			Order:    &order,
			Steps:    workflowSteps,
		})
	}
	return states, nil
}

func (r *CloudResourceRepository) ListWorkflowsByResource(ctx context.Context, resourceID uint64, limit, offset int) ([]domain.CloudWorkflow, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var workflows []domain.CloudWorkflow
	err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).
		Where("resource_id = ?", resourceID).
		Order("created_at DESC, id DESC").
		Limit(limit).
		Offset(offset).
		Find(&workflows).Error
	return workflows, err
}

func (r *CloudResourceRepository) ListRunnableWorkflows(ctx context.Context, limit int) ([]domain.CloudWorkflow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	now := time.Now()
	var workflows []domain.CloudWorkflow
	err := r.db.WithContext(ctx).
		Where("status IN ? AND (lease_until IS NULL OR lease_until < ?) AND (next_reconcile_at IS NULL OR next_reconcile_at <= ?)", []domain.CloudWorkflowStatus{
			domain.CloudWorkflowPending,
			domain.CloudWorkflowRunning,
			domain.CloudWorkflowCompensating,
		}, now, now).
		Order("updated_at ASC, id ASC").
		Limit(limit).
		Find(&workflows).Error
	return workflows, err
}

// ClaimRunnableWorkflows assigns a disjoint runnable batch to one controller
// replica. SKIP LOCKED prevents replicas from repeatedly scanning the same
// oldest rows and then fighting over individual lease updates.
func (r *CloudResourceRepository) ClaimRunnableWorkflows(ctx context.Context, limit int, leaseUntil time.Time) ([]domain.CloudWorkflow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	now := time.Now()
	workflows := make([]domain.CloudWorkflow, 0, limit)
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status IN ? AND (lease_until IS NULL OR lease_until < ?) AND (next_reconcile_at IS NULL OR next_reconcile_at <= ?)", []domain.CloudWorkflowStatus{
				domain.CloudWorkflowPending,
				domain.CloudWorkflowRunning,
				domain.CloudWorkflowCompensating,
			}, now, now).
			Limit(limit).
			Find(&workflows).Error; err != nil {
			return err
		}
		if len(workflows) == 0 {
			return nil
		}
		ids := make([]uint64, 0, len(workflows))
		for _, workflow := range workflows {
			ids = append(ids, workflow.ID)
		}
		result := tx.Model(&domain.CloudWorkflow{}).
			Where("id IN ?", ids).
			Update("lease_until", leaseUntil)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(ids)) {
			return errors.New("cloud workflow batch claim lost locked rows")
		}
		for i := range workflows {
			workflows[i].LeaseUntil = &leaseUntil
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	return workflows, err
}

func (r *CloudResourceRepository) ReleaseWorkflowLeases(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&domain.CloudWorkflow{}).
		Where("id IN ?", ids).
		Update("lease_until", nil).Error
}

func (r *CloudResourceRepository) ClaimWorkflow(ctx context.Context, id uint64, leaseUntil time.Time) (bool, error) {
	now := time.Now()
	result := r.db.WithContext(ctx).Model(&domain.CloudWorkflow{}).
		Where("id = ? AND status IN ? AND (lease_until IS NULL OR lease_until < ?) AND (next_reconcile_at IS NULL OR next_reconcile_at <= ?)", id, []domain.CloudWorkflowStatus{
			domain.CloudWorkflowPending,
			domain.CloudWorkflowRunning,
			domain.CloudWorkflowCompensating,
		}, now, now).
		Updates(map[string]any{"lease_until": leaseUntil})
	return result.RowsAffected == 1, result.Error
}

func (r *CloudResourceRepository) ReleaseWorkflowLease(ctx context.Context, id uint64) error {
	return r.db.WithContext(ctx).Model(&domain.CloudWorkflow{}).
		Where("id = ?", id).
		Update("lease_until", nil).Error
}

func (r *CloudResourceRepository) MarkForwardStepRunning(ctx context.Context, workflowID uint64, stepName string) error {
	now := time.Now()
	if r.workflowDBMode == CloudWorkflowDBOptimized && r.db.Dialector.Name() == "mysql" {
		return r.db.WithContext(ctx).Exec(`
UPDATE cloud_workflows AS w
JOIN cloud_orders AS o ON o.id = w.order_id
JOIN cloud_workflow_steps AS s ON s.workflow_id = w.id AND s.name = ?
SET w.status = ?,
    w.last_transition_at = ?,
    w.next_reconcile_at = NULL,
    o.status = ?,
    o.last_attempt_at = ?,
    s.status = ?,
    s.attempt_count = s.attempt_count + 1,
    s.started_at = COALESCE(s.started_at, ?)
WHERE w.id = ?
  AND w.status IN (?, ?)
  AND o.status IN (?, ?)
  AND s.status IN (?, ?)`,
			stepName,
			domain.CloudWorkflowRunning, now,
			domain.CloudOrderRunning, now,
			domain.CloudWorkflowStepRunning, now,
			workflowID,
			domain.CloudWorkflowPending, domain.CloudWorkflowRunning,
			domain.CloudOrderPending, domain.CloudOrderRunning,
			domain.CloudWorkflowStepPending, domain.CloudWorkflowStepRunning,
		).Error
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflow{}).
			Where("id = ? AND status IN ?", workflowID, []domain.CloudWorkflowStatus{domain.CloudWorkflowPending, domain.CloudWorkflowRunning}).
			Updates(map[string]any{
				"status":             domain.CloudWorkflowRunning,
				"last_transition_at": now,
				"next_reconcile_at":  nil,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudOrder{}).
			Where("id = (SELECT order_id FROM cloud_workflows WHERE id = ?) AND status IN ?", workflowID, []domain.CloudOrderStatus{domain.CloudOrderPending, domain.CloudOrderRunning}).
			Updates(map[string]any{
				"status":          domain.CloudOrderRunning,
				"last_attempt_at": now,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ? AND status IN ?", workflowID, stepName, []domain.CloudWorkflowStepStatus{domain.CloudWorkflowStepPending, domain.CloudWorkflowStepRunning}).
			Updates(map[string]any{
				"status":        domain.CloudWorkflowStepRunning,
				"attempt_count": gorm.Expr("attempt_count + 1"),
				"started_at":    gorm.Expr("COALESCE(started_at, ?)", now),
			}).Error
	})
}

func (r *CloudResourceRepository) CompleteForwardStep(ctx context.Context, workflowID uint64, stepName, output string, skipped bool) error {
	now := time.Now()
	status := domain.CloudWorkflowStepSucceeded
	if skipped {
		status = domain.CloudWorkflowStepSkipped
	}
	if r.workflowDBMode == CloudWorkflowDBOptimized && r.db.Dialector.Name() == "mysql" {
		return r.db.WithContext(ctx).Exec(`
UPDATE cloud_workflows AS w
JOIN cloud_workflow_steps AS s ON s.workflow_id = w.id AND s.name = ?
SET s.status = ?,
    s.output = ?,
    s.last_error = '',
    s.completed_at = ?,
    w.last_transition_at = ?,
    w.lease_until = NULL,
    w.next_reconcile_at = NULL
WHERE w.id = ?`, stepName, status, output, now, now, workflowID).Error
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{
				"status":       status,
				"output":       output,
				"last_error":   "",
				"completed_at": now,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"last_transition_at": now, "lease_until": nil, "next_reconcile_at": nil}).Error
	})
}

func (r *CloudResourceRepository) RecordForwardStepProgress(ctx context.Context, workflowID uint64, stepName, output string, nextReconcileAt ...time.Time) error {
	now := time.Now()
	var scheduled any
	if len(nextReconcileAt) > 0 {
		scheduled = nextReconcileAt[0]
	}
	if r.workflowDBMode == CloudWorkflowDBOptimized && r.db.Dialector.Name() == "mysql" {
		return r.db.WithContext(ctx).Exec(`
UPDATE cloud_workflows AS w
JOIN cloud_workflow_steps AS s ON s.workflow_id = w.id AND s.name = ?
SET s.status = ?,
    s.output = ?,
    s.last_error = '',
    w.last_transition_at = ?,
    w.lease_until = NULL,
    w.next_reconcile_at = ?
WHERE w.id = ?`, stepName, domain.CloudWorkflowStepRunning, output, now, scheduled, workflowID).Error
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{"status": domain.CloudWorkflowStepRunning, "output": output, "last_error": ""}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"last_transition_at": now, "lease_until": nil, "next_reconcile_at": scheduled}).Error
	})
}

func (r *CloudResourceRepository) FailForwardStep(ctx context.Context, workflowID uint64, stepName, message string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{
				"status":       domain.CloudWorkflowStepFailed,
				"last_error":   message,
				"completed_at": now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudOrder{}).
			Where("id = (SELECT order_id FROM cloud_workflows WHERE id = ?)", workflowID).
			Updates(map[string]any{"status": domain.CloudOrderRunning, "last_error": message}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{
				"status":             domain.CloudWorkflowCompensating,
				"failure_reason":     message,
				"last_transition_at": now,
				"lease_until":        nil,
				"next_reconcile_at":  nil,
			}).Error
	})
}

func (r *CloudResourceRepository) MarkCompensationRunning(ctx context.Context, workflowID uint64, stepName string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ? AND status IN ?", workflowID, stepName, []domain.CloudWorkflowStepStatus{domain.CloudWorkflowStepSucceeded, domain.CloudWorkflowStepFailed, domain.CloudWorkflowStepCompensating}).
			Updates(map[string]any{
				"status":                     domain.CloudWorkflowStepCompensating,
				"compensation_attempt_count": gorm.Expr("compensation_attempt_count + 1"),
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"status": domain.CloudWorkflowCompensating, "last_transition_at": now, "next_reconcile_at": nil}).Error
	})
}

func (r *CloudResourceRepository) CompleteCompensationStep(ctx context.Context, workflowID uint64, stepName string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{
				"status":       domain.CloudWorkflowStepCompensated,
				"last_error":   "",
				"completed_at": now,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"last_transition_at": now, "lease_until": nil, "next_reconcile_at": nil}).Error
	})
}

func (r *CloudResourceRepository) RecordCompensationFailure(ctx context.Context, workflowID uint64, stepName, message string, nextReconcileAt ...time.Time) error {
	now := time.Now()
	var scheduled any
	if len(nextReconcileAt) > 0 {
		scheduled = nextReconcileAt[0]
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{"status": domain.CloudWorkflowStepCompensating, "last_error": message}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"last_transition_at": now, "lease_until": nil, "next_reconcile_at": scheduled}).Error
	})
}

func (r *CloudResourceRepository) RecordCompensationProgress(ctx context.Context, workflowID uint64, stepName string, nextReconcileAt ...time.Time) error {
	now := time.Now()
	var scheduled any
	if len(nextReconcileAt) > 0 {
		scheduled = nextReconcileAt[0]
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{"status": domain.CloudWorkflowStepCompensating, "last_error": ""}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflowID).
			Updates(map[string]any{"last_transition_at": now, "lease_until": nil, "next_reconcile_at": scheduled}).Error
	})
}

func (r *CloudResourceRepository) CommitProvisionWorkflow(ctx context.Context, workflowID uint64, providerID, actualSpec string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workflow domain.CloudWorkflow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workflow, workflowID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudWorkflowNotFound
			}
			return err
		}
		if workflow.Status == domain.CloudWorkflowSucceeded {
			return nil
		}
		if workflow.Status != domain.CloudWorkflowRunning && workflow.Status != domain.CloudWorkflowPending {
			return ErrCloudWorkflowNotRunnable
		}
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, workflow.ResourceID).Error; err != nil {
			return err
		}
		resource.ActualState = domain.CloudResourceActive
		resource.ObservedGeneration = resource.Generation
		resource.ProviderResourceID = providerID
		resource.ActualSpec = actualSpec
		resource.DesiredBundleSpec = workflow.Spec
		resource.ActualBundleSpec = workflow.Spec
		resource.LastError = ""
		resource.LastReconcileAt = &now
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflow.ID, domain.CloudWorkflowStepCommit).
			Updates(map[string]any{
				"status":        domain.CloudWorkflowStepSucceeded,
				"attempt_count": gorm.Expr("attempt_count + 1"),
				"started_at":    gorm.Expr("COALESCE(started_at, ?)", now),
				"completed_at":  now,
				"last_error":    "",
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudOrder{}).
			Where("id = ?", workflow.OrderID).
			Updates(map[string]any{
				"status":       domain.CloudOrderSucceeded,
				"last_error":   "",
				"completed_at": now,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflow.ID).
			Updates(map[string]any{
				"status":             domain.CloudWorkflowSucceeded,
				"failure_reason":     "",
				"last_transition_at": now,
				"completed_at":       now,
				"lease_until":        nil,
			}).Error
	})
}

func (r *CloudResourceRepository) FinalizeProvisionCompensation(ctx context.Context, workflowID uint64) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workflow domain.CloudWorkflow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workflow, workflowID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudWorkflowNotFound
			}
			return err
		}
		if workflow.Status == domain.CloudWorkflowCompensated {
			return nil
		}
		if workflow.Status != domain.CloudWorkflowCompensating {
			return ErrCloudWorkflowNotRunnable
		}
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, workflow.ResourceID).Error; err != nil {
			return err
		}
		if err := r.releaseQuotaForResource(tx, &resource, cloudcontrol.QuotaDemand{
			CPUMilli:  resource.QuotaCPUMilli,
			MemoryMiB: resource.QuotaMemoryMiB,
			Replicas:  resource.QuotaReplicas,
		}); err != nil {
			return err
		}
		resource.QuotaCPUMilli = 0
		resource.QuotaMemoryMiB = 0
		resource.QuotaReplicas = 0
		resource.DesiredState = domain.CloudResourceDesiredDeleted
		resource.ActualState = domain.CloudResourceDeleted
		resource.ObservedGeneration = resource.Generation
		resource.LastError = workflow.FailureReason
		resource.LastReconcileAt = &now
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflow.ID, domain.CloudWorkflowStepQuota).
			Updates(map[string]any{
				"status":                     domain.CloudWorkflowStepCompensated,
				"compensation_attempt_count": gorm.Expr("compensation_attempt_count + 1"),
				"completed_at":               now,
				"last_error":                 "",
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND status IN ?", workflow.ID, []domain.CloudWorkflowStepStatus{domain.CloudWorkflowStepPending, domain.CloudWorkflowStepRunning}).
			Updates(map[string]any{"status": domain.CloudWorkflowStepSkipped, "completed_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudOrder{}).
			Where("id = ?", workflow.OrderID).
			Updates(map[string]any{
				"status":       domain.CloudOrderFailed,
				"last_error":   workflow.FailureReason,
				"completed_at": now,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).
			Where("id = ?", workflow.ID).
			Updates(map[string]any{
				"status":             domain.CloudWorkflowCompensated,
				"last_transition_at": now,
				"completed_at":       now,
				"lease_until":        nil,
			}).Error
	})
}
