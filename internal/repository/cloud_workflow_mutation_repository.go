package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrCloudWorkflowActive         = errors.New("cloud resource already has an active workflow")
	ErrCloudWorkflowStateChanged   = errors.New("cloud resource state changed before workflow admission")
	ErrCloudWorkflowRequiresActive = errors.New("cloud workflow requires an active saga-managed resource")
)

// NormalizeCloudWorkflowIndexes removes the legacy one-workflow-per-resource
// unique index introduced by the first provisioning-only Saga version. A
// resource now owns a sequence of provision/resize/delete workflows.
func NormalizeCloudWorkflowIndexes(db *gorm.DB) error {
	if db == nil {
		return errors.New("nil database")
	}
	migrator := db.Migrator()
	if migrator.HasIndex(&domain.CloudWorkflow{}, "ux_cloud_workflow_resource") {
		return migrator.DropIndex(&domain.CloudWorkflow{}, "ux_cloud_workflow_resource")
	}
	return nil
}

func (r *CloudResourceRepository) CreateResizeWorkflow(
	ctx context.Context,
	resourceID uint64,
	order *domain.CloudOrder,
	previousBundleSpec string,
	targetBundleSpec string,
	targetComputeSpec string,
	targetDemand cloudcontrol.QuotaDemand,
) (*domain.CloudWorkflow, error) {
	var workflow *domain.CloudWorkflow
	err := r.quotaTransaction(ctx, func(tx *gorm.DB) error {
		resource, err := lockSagaResourceForMutation(tx, resourceID, previousBundleSpec)
		if err != nil {
			return err
		}
		if err := ensureNoActiveWorkflow(tx, resource.ID); err != nil {
			return err
		}

		charged := cloudcontrol.QuotaDemand{
			CPUMilli:  maxInt64(resource.QuotaCPUMilli, targetDemand.CPUMilli),
			MemoryMiB: maxInt64(resource.QuotaMemoryMiB, targetDemand.MemoryMiB),
			Replicas:  maxInt64(resource.QuotaReplicas, targetDemand.Replicas),
		}
		additional := cloudcontrol.QuotaDemand{
			CPUMilli:  charged.CPUMilli - resource.QuotaCPUMilli,
			MemoryMiB: charged.MemoryMiB - resource.QuotaMemoryMiB,
			Replicas:  charged.Replicas - resource.QuotaReplicas,
		}
		if err := r.reserveQuotaForResource(tx, resource, additional); err != nil {
			return err
		}

		resource.Generation++
		resource.DesiredState = domain.CloudResourceDesiredActive
		resource.ActualState = domain.CloudResourceProvisioning
		resource.DesiredSpec = targetComputeSpec
		resource.DesiredBundleSpec = targetBundleSpec
		resource.QuotaCPUMilli = charged.CPUMilli
		resource.QuotaMemoryMiB = charged.MemoryMiB
		resource.QuotaReplicas = charged.Replicas
		resource.LastError = ""
		if err := tx.Save(resource).Error; err != nil {
			return err
		}

		order.ResourceID = resource.ID
		order.TenantID = resource.TenantID
		order.Type = domain.CloudOrderResize
		order.Status = domain.CloudOrderPending
		order.RequestedGeneration = resource.Generation
		order.RequestedSpec = targetBundleSpec
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		now := time.Now()
		workflow = &domain.CloudWorkflow{
			ResourceID:       resource.ID,
			OrderID:          order.ID,
			TenantID:         resource.TenantID,
			Kind:             domain.CloudWorkflowKindResize,
			Status:           domain.CloudWorkflowPending,
			Spec:             targetBundleSpec,
			PreviousSpec:     previousBundleSpec,
			LastTransitionAt: &now,
		}
		if err := tx.Create(workflow).Error; err != nil {
			return err
		}
		quotaOutput, _ := json.Marshal(charged)
		steps := []domain.CloudWorkflowStep{
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepQuota, Position: 0, Status: domain.CloudWorkflowStepSucceeded, Output: string(quotaOutput), StartedAt: &now, CompletedAt: &now},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepNetwork, Position: 1, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepStorage, Position: 2, Status: domain.CloudWorkflowStepSkipped, Output: `{"immutable":true}`, StartedAt: &now, CompletedAt: &now},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCompute, Position: 3, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCommit, Position: 4, Status: domain.CloudWorkflowStepPending},
		}
		return tx.Create(&steps).Error
	})
	return workflow, err
}

func (r *CloudResourceRepository) CreateDeleteWorkflow(
	ctx context.Context,
	resourceID uint64,
	order *domain.CloudOrder,
	previousBundleSpec string,
) (*domain.CloudWorkflow, error) {
	var workflow *domain.CloudWorkflow
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		resource, err := lockSagaResourceForMutation(tx, resourceID, previousBundleSpec)
		if err != nil {
			return err
		}
		if err := ensureNoActiveWorkflow(tx, resource.ID); err != nil {
			return err
		}
		bundle, err := cloudcontrol.ParseKubernetesBundleSpec(previousBundleSpec, "default")
		if err != nil {
			return err
		}

		resource.Generation++
		resource.DesiredState = domain.CloudResourceDesiredDeleted
		resource.ActualState = domain.CloudResourceDeleting
		resource.DesiredBundleSpec = previousBundleSpec
		resource.LastError = ""
		if err := tx.Save(resource).Error; err != nil {
			return err
		}

		order.ResourceID = resource.ID
		order.TenantID = resource.TenantID
		order.Type = domain.CloudOrderDelete
		order.Status = domain.CloudOrderPending
		order.RequestedGeneration = resource.Generation
		order.RequestedSpec = previousBundleSpec
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		now := time.Now()
		workflow = &domain.CloudWorkflow{
			ResourceID:       resource.ID,
			OrderID:          order.ID,
			TenantID:         resource.TenantID,
			Kind:             domain.CloudWorkflowKindDelete,
			Status:           domain.CloudWorkflowPending,
			Spec:             previousBundleSpec,
			PreviousSpec:     previousBundleSpec,
			LastTransitionAt: &now,
		}
		if err := tx.Create(workflow).Error; err != nil {
			return err
		}

		storageStatus := domain.CloudWorkflowStepPending
		networkStatus := domain.CloudWorkflowStepPending
		var storageCompletedAt, networkCompletedAt *time.Time
		if !bundle.Storage.Enabled {
			storageStatus = domain.CloudWorkflowStepSkipped
			storageCompletedAt = &now
		}
		if !bundle.Network.Enabled {
			networkStatus = domain.CloudWorkflowStepSkipped
			networkCompletedAt = &now
		}
		steps := []domain.CloudWorkflowStep{
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCompute, Position: 0, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepStorage, Position: 1, Status: storageStatus, CompletedAt: storageCompletedAt},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepNetwork, Position: 2, Status: networkStatus, CompletedAt: networkCompletedAt},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepQuota, Position: 3, Status: domain.CloudWorkflowStepPending},
			{WorkflowID: workflow.ID, Name: domain.CloudWorkflowStepCommit, Position: 4, Status: domain.CloudWorkflowStepPending},
		}
		return tx.Create(&steps).Error
	})
	return workflow, err
}

func (r *CloudResourceRepository) RecordForwardRetryableFailure(ctx context.Context, workflowID uint64, stepName, message string, nextReconcileAt ...time.Time) error {
	now := time.Now()
	var scheduled any
	if len(nextReconcileAt) > 0 {
		scheduled = nextReconcileAt[0]
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.CloudWorkflowStep{}).
			Where("workflow_id = ? AND name = ?", workflowID, stepName).
			Updates(map[string]any{
				"status":     domain.CloudWorkflowStepRunning,
				"last_error": message,
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
			Updates(map[string]any{"status": domain.CloudWorkflowRunning, "last_transition_at": now, "lease_until": nil, "next_reconcile_at": scheduled}).Error
	})
}

func (r *CloudResourceRepository) CommitResizeWorkflow(ctx context.Context, workflowID uint64, providerID, actualSpec string, targetDemand cloudcontrol.QuotaDemand) error {
	return r.finishResizeWorkflow(ctx, workflowID, providerID, actualSpec, targetDemand, false)
}

func (r *CloudResourceRepository) FinalizeResizeCompensation(ctx context.Context, workflowID uint64, previousDemand cloudcontrol.QuotaDemand, previousComputeSpec string) error {
	return r.finishResizeWorkflow(ctx, workflowID, "", "", previousDemand, true, previousComputeSpec)
}

func (r *CloudResourceRepository) finishResizeWorkflow(ctx context.Context, workflowID uint64, providerID, actualSpec string, settledDemand cloudcontrol.QuotaDemand, compensated bool, previousComputeSpec ...string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workflow domain.CloudWorkflow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workflow, workflowID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudWorkflowNotFound
			}
			return err
		}
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, workflow.ResourceID).Error; err != nil {
			return err
		}
		if settledDemand.CPUMilli > resource.QuotaCPUMilli || settledDemand.MemoryMiB > resource.QuotaMemoryMiB || settledDemand.Replicas > resource.QuotaReplicas {
			return fmt.Errorf("resize quota invariant violated for resource=%d", resource.ID)
		}
		if err := r.releaseQuotaForResource(tx, &resource, cloudcontrol.QuotaDemand{
			CPUMilli:  resource.QuotaCPUMilli - settledDemand.CPUMilli,
			MemoryMiB: resource.QuotaMemoryMiB - settledDemand.MemoryMiB,
			Replicas:  resource.QuotaReplicas - settledDemand.Replicas,
		}); err != nil {
			return err
		}
		resource.QuotaCPUMilli = settledDemand.CPUMilli
		resource.QuotaMemoryMiB = settledDemand.MemoryMiB
		resource.QuotaReplicas = settledDemand.Replicas
		resource.ActualState = domain.CloudResourceActive
		resource.DesiredState = domain.CloudResourceDesiredActive
		resource.ObservedGeneration = resource.Generation
		resource.LastReconcileAt = &now
		if compensated {
			resource.DesiredBundleSpec = workflow.PreviousSpec
			resource.ActualBundleSpec = workflow.PreviousSpec
			if len(previousComputeSpec) != 1 {
				return errors.New("missing previous compute spec")
			}
			resource.DesiredSpec = previousComputeSpec[0]
			resource.LastError = workflow.FailureReason
		} else {
			resource.DesiredBundleSpec = workflow.Spec
			resource.ActualBundleSpec = workflow.Spec
			resource.ActualSpec = actualSpec
			resource.ProviderResourceID = providerID
			resource.LastError = ""
		}
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}

		if compensated {
			if err := markQuotaCompensated(tx, workflow.ID, now); err != nil {
				return err
			}
			if err := tx.Model(&domain.CloudWorkflowStep{}).
				Where("workflow_id = ? AND status IN ?", workflow.ID, []domain.CloudWorkflowStepStatus{domain.CloudWorkflowStepPending, domain.CloudWorkflowStepRunning}).
				Updates(map[string]any{"status": domain.CloudWorkflowStepSkipped, "completed_at": now}).Error; err != nil {
				return err
			}
			if err := tx.Model(&domain.CloudOrder{}).Where("id = ?", workflow.OrderID).Updates(map[string]any{
				"status": domain.CloudOrderFailed, "last_error": workflow.FailureReason, "completed_at": now,
			}).Error; err != nil {
				return err
			}
			return tx.Model(&domain.CloudWorkflow{}).Where("id = ?", workflow.ID).Updates(map[string]any{
				"status": domain.CloudWorkflowCompensated, "last_transition_at": now, "completed_at": now, "lease_until": nil,
			}).Error
		}

		if err := markCommitSucceeded(tx, workflow, now); err != nil {
			return err
		}
		return nil
	})
}

func (r *CloudResourceRepository) CompleteDeleteQuotaStep(ctx context.Context, workflowID uint64) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workflow domain.CloudWorkflow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workflow, workflowID).Error; err != nil {
			return err
		}
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, workflow.ResourceID).Error; err != nil {
			return err
		}
		if err := r.releaseQuotaForResource(tx, &resource, cloudcontrol.QuotaDemand{
			CPUMilli: resource.QuotaCPUMilli, MemoryMiB: resource.QuotaMemoryMiB, Replicas: resource.QuotaReplicas,
		}); err != nil {
			return err
		}
		resource.QuotaCPUMilli = 0
		resource.QuotaMemoryMiB = 0
		resource.QuotaReplicas = 0
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.CloudWorkflowStep{}).Where("workflow_id = ? AND name = ?", workflowID, domain.CloudWorkflowStepQuota).Updates(map[string]any{
			"status": domain.CloudWorkflowStepSucceeded, "attempt_count": gorm.Expr("attempt_count + 1"), "started_at": gorm.Expr("COALESCE(started_at, ?)", now), "completed_at": now, "last_error": "",
		}).Error; err != nil {
			return err
		}
		return tx.Model(&domain.CloudWorkflow{}).Where("id = ?", workflowID).Updates(map[string]any{"last_transition_at": now, "lease_until": nil}).Error
	})
}

func (r *CloudResourceRepository) CommitDeleteWorkflow(ctx context.Context, workflowID uint64) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workflow domain.CloudWorkflow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workflow, workflowID).Error; err != nil {
			return err
		}
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, workflow.ResourceID).Error; err != nil {
			return err
		}
		if resource.QuotaCPUMilli != 0 || resource.QuotaMemoryMiB != 0 || resource.QuotaReplicas != 0 {
			return errors.New("cannot commit delete workflow before quota is released")
		}
		resource.ActualState = domain.CloudResourceDeleted
		resource.DesiredState = domain.CloudResourceDesiredDeleted
		resource.ObservedGeneration = resource.Generation
		resource.ActualSpec = `{}`
		resource.ActualBundleSpec = ""
		resource.ProviderResourceID = ""
		resource.LastError = ""
		resource.LastReconcileAt = &now
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		return markCommitSucceeded(tx, workflow, now)
	})
}

func lockSagaResourceForMutation(tx *gorm.DB, resourceID uint64, expectedBundleSpec string) (*domain.CloudResource, error) {
	var resource domain.CloudResource
	if err := tenantScopedQuery(tx.Statement.Context, tx.Clauses(clause.Locking{Strength: "UPDATE"})).First(&resource, resourceID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudResourceNotFound
		}
		return nil, err
	}
	if resource.OrchestrationMode != domain.CloudOrchestrationSaga || resource.ActualState != domain.CloudResourceActive || resource.DesiredState != domain.CloudResourceDesiredActive || resource.ObservedGeneration != resource.Generation {
		return nil, ErrCloudWorkflowRequiresActive
	}
	if resource.ActualBundleSpec == "" || resource.ActualBundleSpec != expectedBundleSpec {
		return nil, ErrCloudWorkflowStateChanged
	}
	return &resource, nil
}

func ensureNoActiveWorkflow(tx *gorm.DB, resourceID uint64) error {
	var count int64
	if err := tx.Model(&domain.CloudWorkflow{}).
		Where("resource_id = ? AND status IN ?", resourceID, []domain.CloudWorkflowStatus{domain.CloudWorkflowPending, domain.CloudWorkflowRunning, domain.CloudWorkflowCompensating}).
		Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return ErrCloudWorkflowActive
	}
	return nil
}

func markQuotaCompensated(tx *gorm.DB, workflowID uint64, now time.Time) error {
	return tx.Model(&domain.CloudWorkflowStep{}).Where("workflow_id = ? AND name = ?", workflowID, domain.CloudWorkflowStepQuota).Updates(map[string]any{
		"status": domain.CloudWorkflowStepCompensated, "compensation_attempt_count": gorm.Expr("compensation_attempt_count + 1"), "completed_at": now, "last_error": "",
	}).Error
}

func markCommitSucceeded(tx *gorm.DB, workflow domain.CloudWorkflow, now time.Time) error {
	if err := tx.Model(&domain.CloudWorkflowStep{}).Where("workflow_id = ? AND name = ?", workflow.ID, domain.CloudWorkflowStepCommit).Updates(map[string]any{
		"status": domain.CloudWorkflowStepSucceeded, "attempt_count": gorm.Expr("attempt_count + 1"), "started_at": gorm.Expr("COALESCE(started_at, ?)", now), "completed_at": now, "last_error": "",
	}).Error; err != nil {
		return err
	}
	if err := tx.Model(&domain.CloudOrder{}).Where("id = ?", workflow.OrderID).Updates(map[string]any{
		"status": domain.CloudOrderSucceeded, "last_error": "", "completed_at": now,
	}).Error; err != nil {
		return err
	}
	return tx.Model(&domain.CloudWorkflow{}).Where("id = ?", workflow.ID).Updates(map[string]any{
		"status": domain.CloudWorkflowSucceeded, "failure_reason": "", "last_transition_at": now, "completed_at": now, "lease_until": nil,
	}).Error
}
