package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrCloudResourceNotFound    = errors.New("cloud resource not found")
	ErrCloudOrderNotFound       = errors.New("cloud order not found")
	ErrCloudResourceDeleted     = errors.New("cloud resource is already deleting or deleted")
	ErrCloudResourceSagaManaged = errors.New("cloud resource is saga-managed")
	ErrCloudQuotaNotFound       = errors.New("cloud tenant quota not found")
	ErrCloudQuotaExceeded       = errors.New("cloud tenant quota exceeded")
	ErrCloudQuotaBelowUsage     = errors.New("cloud tenant quota limit is below current usage")
	ErrCloudQuotaModeMismatch   = errors.New("cloud tenant quota accounting mode mismatch")
)

type CloudResourceRepository struct {
	db                 *gorm.DB
	quotaAdmissionMode string
	quotaBucketCount   int
	workflowDBMode     string
}

func NewCloudResourceRepository(db *gorm.DB) *CloudResourceRepository {
	return &CloudResourceRepository{
		db:                 db,
		quotaAdmissionMode: CloudQuotaAdmissionLocked,
		quotaBucketCount:   16,
		workflowDBMode:     CloudWorkflowDBBaseline,
	}
}

const (
	CloudQuotaAdmissionLocked          = "locked"
	CloudQuotaAdmissionAtomicVersioned = "atomic_versioned"
	CloudQuotaAdmissionBucketed        = "bucketed"
	CloudQuotaAccountingSingle         = "single"
	CloudQuotaAccountingBucketed       = "bucketed"
	CloudWorkflowDBLegacy              = "legacy"
	CloudWorkflowDBBaseline            = "baseline"
	CloudWorkflowDBOptimized           = "optimized"
)

func (r *CloudResourceRepository) SetQuotaAdmissionMode(mode string) error {
	switch mode {
	case "", CloudQuotaAdmissionLocked:
		r.quotaAdmissionMode = CloudQuotaAdmissionLocked
	case CloudQuotaAdmissionAtomicVersioned:
		r.quotaAdmissionMode = CloudQuotaAdmissionAtomicVersioned
	case CloudQuotaAdmissionBucketed:
		r.quotaAdmissionMode = CloudQuotaAdmissionBucketed
	default:
		return fmt.Errorf("unsupported cloud quota admission mode %q", mode)
	}
	return nil
}

func (r *CloudResourceRepository) SetQuotaBucketCount(count int) error {
	if count < 2 || count > 64 {
		return fmt.Errorf("cloud quota bucket count must be between 2 and 64: %d", count)
	}
	r.quotaBucketCount = count
	return nil
}

func (r *CloudResourceRepository) SetWorkflowDBMode(mode string) error {
	switch mode {
	case CloudWorkflowDBLegacy:
		r.workflowDBMode = CloudWorkflowDBLegacy
	case "", CloudWorkflowDBBaseline:
		r.workflowDBMode = CloudWorkflowDBBaseline
	case CloudWorkflowDBOptimized:
		r.workflowDBMode = CloudWorkflowDBOptimized
	default:
		return fmt.Errorf("unsupported cloud workflow DB mode %q", mode)
	}
	return nil
}

func (r *CloudResourceRepository) WorkflowDBOptimized() bool {
	return r != nil && r.workflowDBMode == CloudWorkflowDBOptimized
}

func (r *CloudResourceRepository) WorkflowDBLegacy() bool {
	return r != nil && r.workflowDBMode == CloudWorkflowDBLegacy
}

func (r *CloudResourceRepository) quotaTransaction(ctx context.Context, fn func(*gorm.DB) error) error {
	db := r.db.WithContext(ctx)
	if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed && db.Dialector.Name() == "mysql" {
		return db.Transaction(fn, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	}
	return db.Transaction(fn)
}

func (r *CloudResourceRepository) CreateResourceWithOrder(ctx context.Context, resource *domain.CloudResource, order *domain.CloudOrder, demand cloudcontrol.QuotaDemand) error {
	if !cloudTenantPermitted(ctx, resource.TenantID) || resource.TenantID != order.TenantID {
		return ErrCloudTenantAccessDenied
	}
	return r.quotaTransaction(ctx, func(tx *gorm.DB) error {
		if err := r.reserveQuotaForResource(tx, resource, demand); err != nil {
			return err
		}
		resource.QuotaCPUMilli = demand.CPUMilli
		resource.QuotaMemoryMiB = demand.MemoryMiB
		resource.QuotaReplicas = demand.Replicas
		if err := tx.Create(resource).Error; err != nil {
			return err
		}
		order.ResourceID = resource.ID
		order.RequestedGeneration = resource.Generation
		return tx.Create(order).Error
	})
}

func (r *CloudResourceRepository) SetTenantQuota(ctx context.Context, tenantID string, limits cloudcontrol.QuotaDemand) (*domain.CloudTenantQuota, error) {
	if !cloudTenantPermitted(ctx, tenantID) {
		return nil, ErrCloudTenantAccessDenied
	}
	if !limits.NonNegative() {
		return nil, errors.New("quota limits must be non-negative")
	}
	var quota domain.CloudTenantQuota
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&quota, "tenant_id = ?", tenantID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			quota = domain.CloudTenantQuota{
				TenantID:       tenantID,
				AccountingMode: CloudQuotaAccountingSingle,
				CPUMilliLimit:  limits.CPUMilli,
				MemoryMiBLimit: limits.MemoryMiB,
				ReplicaLimit:   limits.Replicas,
				Version:        1,
			}
			if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
				quota.AccountingMode = CloudQuotaAccountingBucketed
			}
			if err := tx.Create(&quota).Error; err != nil {
				return err
			}
			if quota.AccountingMode == CloudQuotaAccountingBucketed {
				return r.syncQuotaBuckets(tx, tenantID, limits)
			}
			return nil
		}
		if err != nil {
			return err
		}
		if quota.AccountingMode == "" {
			quota.AccountingMode = CloudQuotaAccountingSingle
		}

		used := cloudcontrol.QuotaDemand{
			CPUMilli:  quota.CPUMilliUsed,
			MemoryMiB: quota.MemoryMiBUsed,
			Replicas:  quota.ReplicaUsed,
		}
		if quota.AccountingMode == CloudQuotaAccountingBucketed {
			var usageErr error
			used, usageErr = quotaBucketUsage(tx, tenantID)
			if usageErr != nil {
				return usageErr
			}
		}
		if limits.CPUMilli < used.CPUMilli || limits.MemoryMiB < used.MemoryMiB || limits.Replicas < used.Replicas {
			return fmt.Errorf("%w: tenant=%s used(cpu_milli=%d memory_mib=%d replicas=%d) requested_limits(cpu_milli=%d memory_mib=%d replicas=%d)",
				ErrCloudQuotaBelowUsage,
				tenantID,
				used.CPUMilli,
				used.MemoryMiB,
				used.Replicas,
				limits.CPUMilli,
				limits.MemoryMiB,
				limits.Replicas,
			)
		}

		desiredMode := CloudQuotaAccountingSingle
		if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
			desiredMode = CloudQuotaAccountingBucketed
		}
		if quota.AccountingMode != desiredMode {
			if used.CPUMilli != 0 || used.MemoryMiB != 0 || used.Replicas != 0 {
				return fmt.Errorf("%w: tenant=%s cannot migrate %s -> %s with nonzero usage", ErrCloudQuotaModeMismatch, tenantID, quota.AccountingMode, desiredMode)
			}
			if desiredMode == CloudQuotaAccountingSingle {
				if err := tx.Where("tenant_id = ?", tenantID).Delete(&domain.CloudTenantQuotaBucket{}).Error; err != nil {
					return err
				}
			}
		}
		quota.CPUMilliLimit = limits.CPUMilli
		quota.MemoryMiBLimit = limits.MemoryMiB
		quota.ReplicaLimit = limits.Replicas
		quota.CPUMilliUsed = used.CPUMilli
		quota.MemoryMiBUsed = used.MemoryMiB
		quota.ReplicaUsed = used.Replicas
		quota.AccountingMode = desiredMode
		quota.Version++
		if err := tx.Save(&quota).Error; err != nil {
			return err
		}
		if desiredMode == CloudQuotaAccountingBucketed {
			return r.syncQuotaBuckets(tx, tenantID, limits)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if quota.AccountingMode == CloudQuotaAccountingBucketed {
		used, err := quotaBucketUsage(r.db.WithContext(ctx), tenantID)
		if err != nil {
			return nil, err
		}
		quota.CPUMilliUsed = used.CPUMilli
		quota.MemoryMiBUsed = used.MemoryMiB
		quota.ReplicaUsed = used.Replicas
	}
	return &quota, nil
}

func (r *CloudResourceRepository) GetTenantQuota(ctx context.Context, tenantID string) (*domain.CloudTenantQuota, error) {
	if !cloudTenantPermitted(ctx, tenantID) {
		return nil, ErrCloudTenantAccessDenied
	}
	var quota domain.CloudTenantQuota
	if err := r.db.WithContext(ctx).First(&quota, "tenant_id = ?", tenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudQuotaNotFound
		}
		return nil, err
	}
	if quota.AccountingMode == "" {
		quota.AccountingMode = CloudQuotaAccountingSingle
	}
	if quota.AccountingMode == CloudQuotaAccountingBucketed {
		used, err := quotaBucketUsage(r.db.WithContext(ctx), tenantID)
		if err != nil {
			return nil, err
		}
		quota.CPUMilliUsed = used.CPUMilli
		quota.MemoryMiBUsed = used.MemoryMiB
		quota.ReplicaUsed = used.Replicas
	}
	return &quota, nil
}

func quotaBucketUsage(db *gorm.DB, tenantID string) (cloudcontrol.QuotaDemand, error) {
	var totals struct {
		CPUMilli  int64
		MemoryMiB int64
		Replicas  int64
	}
	err := db.Model(&domain.CloudTenantQuotaBucket{}).
		Select("COALESCE(SUM(cpu_milli_used),0) AS cpu_milli, COALESCE(SUM(memory_mib_used),0) AS memory_mi_b, COALESCE(SUM(replica_used),0) AS replicas").
		Where("tenant_id = ?", tenantID).
		Scan(&totals).Error
	return cloudcontrol.QuotaDemand{CPUMilli: totals.CPUMilli, MemoryMiB: totals.MemoryMiB, Replicas: totals.Replicas}, err
}

func (r *CloudResourceRepository) syncQuotaBuckets(tx *gorm.DB, tenantID string, limits cloudcontrol.QuotaDemand) error {
	var existing []domain.CloudTenantQuotaBucket
	if err := tx.Where("tenant_id = ?", tenantID).Find(&existing).Error; err != nil {
		return err
	}
	byID := make(map[uint16]domain.CloudTenantQuotaBucket, len(existing))
	for i := range existing {
		bucket := existing[i]
		if int(bucket.BucketID) > r.quotaBucketCount {
			if bucket.CPUMilliUsed != 0 || bucket.MemoryMiBUsed != 0 || bucket.ReplicaUsed != 0 {
				return fmt.Errorf("%w: tenant=%s bucket=%d remains in use while bucket count shrinks", ErrCloudQuotaModeMismatch, tenantID, bucket.BucketID)
			}
			if err := tx.Delete(&bucket).Error; err != nil {
				return err
			}
			continue
		}
		byID[bucket.BucketID] = bucket
	}
	for bucketID := 1; bucketID <= r.quotaBucketCount; bucketID++ {
		cpuLimit := splitQuotaLimit(limits.CPUMilli, bucketID, r.quotaBucketCount)
		memoryLimit := splitQuotaLimit(limits.MemoryMiB, bucketID, r.quotaBucketCount)
		replicaLimit := splitQuotaLimit(limits.Replicas, bucketID, r.quotaBucketCount)
		id := uint16(bucketID)
		bucket, ok := byID[id]
		if !ok {
			bucket = domain.CloudTenantQuotaBucket{
				TenantID:       tenantID,
				BucketID:       id,
				CPUMilliLimit:  cpuLimit,
				MemoryMiBLimit: memoryLimit,
				ReplicaLimit:   replicaLimit,
				Version:        1,
			}
			if err := tx.Create(&bucket).Error; err != nil {
				return err
			}
			continue
		}
		if bucket.CPUMilliUsed > cpuLimit || bucket.MemoryMiBUsed > memoryLimit || bucket.ReplicaUsed > replicaLimit {
			return fmt.Errorf("%w: tenant=%s bucket=%d used(cpu_milli=%d memory_mib=%d replicas=%d) new_limit(cpu_milli=%d memory_mib=%d replicas=%d)",
				ErrCloudQuotaBelowUsage,
				tenantID,
				bucketID,
				bucket.CPUMilliUsed,
				bucket.MemoryMiBUsed,
				bucket.ReplicaUsed,
				cpuLimit,
				memoryLimit,
				replicaLimit,
			)
		}
		if err := tx.Model(&domain.CloudTenantQuotaBucket{}).
			Where("tenant_id = ? AND bucket_id = ?", tenantID, id).
			Updates(map[string]any{
				"cpu_milli_limit":  cpuLimit,
				"memory_mib_limit": memoryLimit,
				"replica_limit":    replicaLimit,
				"version":          gorm.Expr("version + 1"),
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (r *CloudResourceRepository) FindOrderByIdempotencyKey(ctx context.Context, key string) (*domain.CloudOrder, error) {
	var order domain.CloudOrder
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).Where("idempotency_key = ?", key).First(&order).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudOrderNotFound
		}
		return nil, err
	}
	return &order, nil
}

// FindOrderByTenantIdempotencyKey is the only safe user-facing lookup after
// removing the legacy globally unique index. The same idempotency key may be
// legitimately reused by two different tenants.
func (r *CloudResourceRepository) FindOrderByTenantIdempotencyKey(ctx context.Context, tenant, key string) (*domain.CloudOrder, error) {
	if !cloudTenantPermitted(ctx, tenant) {
		return nil, ErrCloudTenantAccessDenied
	}
	var order domain.CloudOrder
	query := tenantScopedQuery(ctx, r.db.WithContext(ctx))
	if err := query.Where("tenant_id = ? AND idempotency_key = ?", tenant, key).First(&order).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudOrderNotFound
		}
		return nil, err
	}
	return &order, nil
}

func (r *CloudResourceRepository) GetResource(ctx context.Context, id uint64) (*domain.CloudResource, error) {
	var resource domain.CloudResource
	if err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).First(&resource, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCloudResourceNotFound
		}
		return nil, err
	}
	return &resource, nil
}

func (r *CloudResourceRepository) ListResources(ctx context.Context, tenant string, limit, offset int) ([]domain.CloudResource, error) {
	query := tenantScopedQuery(ctx, r.db.WithContext(ctx)).Order("id DESC").Limit(limit).Offset(offset)
	if tenant != "" {
		query = query.Where("tenant_id = ?", tenant)
	}
	var resources []domain.CloudResource
	return resources, query.Find(&resources).Error
}

func (r *CloudResourceRepository) ListOrders(ctx context.Context, resourceID uint64, limit, offset int) ([]domain.CloudOrder, error) {
	var orders []domain.CloudOrder
	err := tenantScopedQuery(ctx, r.db.WithContext(ctx)).
		Where("resource_id = ?", resourceID).
		Order("id DESC").
		Limit(limit).
		Offset(offset).
		Find(&orders).Error
	return orders, err
}

func (r *CloudResourceRepository) ResizeResourceWithOrder(ctx context.Context, id uint64, desiredSpec string, demand cloudcontrol.QuotaDemand, order *domain.CloudOrder) (*domain.CloudResource, error) {
	var resource domain.CloudResource
	err := r.quotaTransaction(ctx, func(tx *gorm.DB) error {
		if err := tenantScopedQuery(ctx, tx.Clauses(clause.Locking{Strength: "UPDATE"})).First(&resource, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudResourceNotFound
			}
			return err
		}
		if resource.TenantID != order.TenantID {
			return ErrCloudTenantAccessDenied
		}
		if resource.DesiredState == domain.CloudResourceDesiredDeleted {
			return ErrCloudResourceDeleted
		}
		if resource.OrchestrationMode == domain.CloudOrchestrationSaga {
			return ErrCloudResourceSagaManaged
		}
		charged := cloudcontrol.QuotaDemand{
			CPUMilli:  maxInt64(resource.QuotaCPUMilli, demand.CPUMilli),
			MemoryMiB: maxInt64(resource.QuotaMemoryMiB, demand.MemoryMiB),
			Replicas:  maxInt64(resource.QuotaReplicas, demand.Replicas),
		}
		additional := cloudcontrol.QuotaDemand{
			CPUMilli:  charged.CPUMilli - resource.QuotaCPUMilli,
			MemoryMiB: charged.MemoryMiB - resource.QuotaMemoryMiB,
			Replicas:  charged.Replicas - resource.QuotaReplicas,
		}
		if err := r.reserveQuotaForResource(tx, &resource, additional); err != nil {
			return err
		}
		resource.Generation++
		resource.DesiredSpec = desiredSpec
		resource.DesiredState = domain.CloudResourceDesiredActive
		resource.QuotaCPUMilli = charged.CPUMilli
		resource.QuotaMemoryMiB = charged.MemoryMiB
		resource.QuotaReplicas = charged.Replicas
		resource.LastError = ""
		if err := supersedeCloudOrders(tx, resource.ID, resource.Generation); err != nil {
			return err
		}
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		order.ResourceID = resource.ID
		order.RequestedGeneration = resource.Generation
		order.RequestedSpec = desiredSpec
		return tx.Create(order).Error
	})
	if err != nil {
		return nil, err
	}
	return &resource, nil
}

func (r *CloudResourceRepository) DeleteResourceWithOrder(ctx context.Context, id uint64, order *domain.CloudOrder) (*domain.CloudResource, error) {
	var resource domain.CloudResource
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tenantScopedQuery(ctx, tx.Clauses(clause.Locking{Strength: "UPDATE"})).First(&resource, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudResourceNotFound
			}
			return err
		}
		if resource.TenantID != order.TenantID {
			return ErrCloudTenantAccessDenied
		}
		if resource.DesiredState == domain.CloudResourceDesiredDeleted {
			return ErrCloudResourceDeleted
		}
		if resource.OrchestrationMode == domain.CloudOrchestrationSaga {
			return ErrCloudResourceSagaManaged
		}
		resource.Generation++
		resource.DesiredState = domain.CloudResourceDesiredDeleted
		resource.LastError = ""
		if err := supersedeCloudOrders(tx, resource.ID, resource.Generation); err != nil {
			return err
		}
		if err := tx.Save(&resource).Error; err != nil {
			return err
		}
		order.ResourceID = resource.ID
		order.RequestedGeneration = resource.Generation
		order.RequestedSpec = resource.DesiredSpec
		return tx.Create(order).Error
	})
	if err != nil {
		return nil, err
	}
	return &resource, nil
}

func (r *CloudResourceRepository) ListNeedsReconcile(ctx context.Context, driftBefore time.Time, limit int) ([]domain.CloudResource, error) {
	var resources []domain.CloudResource
	now := time.Now()
	err := r.db.WithContext(ctx).
		Where(
			"(orchestration_mode IS NULL OR orchestration_mode <> ?) AND (observed_generation < generation OR (desired_state = ? AND actual_state <> ?) OR (desired_state = ? AND observed_generation = generation AND (last_reconcile_at IS NULL OR last_reconcile_at < ?))) AND (reconcile_lease_until IS NULL OR reconcile_lease_until < ?)",
			domain.CloudOrchestrationSaga,
			domain.CloudResourceDesiredDeleted,
			domain.CloudResourceDeleted,
			domain.CloudResourceDesiredActive,
			driftBefore,
			now,
		).
		Order("updated_at ASC, id ASC").
		Limit(limit).
		Find(&resources).Error
	return resources, err
}

func (r *CloudResourceRepository) ClaimReconcile(ctx context.Context, id, generation uint64, leaseUntil time.Time) (bool, error) {
	now := time.Now()
	result := r.db.WithContext(ctx).Model(&domain.CloudResource{}).
		Where("id = ? AND generation = ? AND (reconcile_lease_until IS NULL OR reconcile_lease_until < ?)", id, generation, now).
		Update("reconcile_lease_until", leaseUntil)
	return result.RowsAffected == 1, result.Error
}

func (r *CloudResourceRepository) MarkOrderRunning(ctx context.Context, resourceID, generation uint64) error {
	now := time.Now()
	return r.db.WithContext(ctx).Model(&domain.CloudOrder{}).
		Where("resource_id = ? AND requested_generation = ? AND status IN ?", resourceID, generation, []domain.CloudOrderStatus{domain.CloudOrderPending, domain.CloudOrderRunning}).
		Updates(map[string]any{
			"status":          domain.CloudOrderRunning,
			"attempt_count":   gorm.Expr("attempt_count + 1"),
			"last_attempt_at": now,
		}).Error
}

func (r *CloudResourceRepository) RecordReconcileProgress(ctx context.Context, id, generation uint64, state domain.CloudResourceActualState, actualSpec, providerID string) (bool, error) {
	updates := map[string]any{
		"actual_state":          state,
		"actual_spec":           actualSpec,
		"last_error":            "",
		"reconcile_lease_until": nil,
		"last_reconcile_at":     time.Now(),
	}
	if providerID != "" {
		updates["provider_resource_id"] = providerID
	}
	result := r.db.WithContext(ctx).Model(&domain.CloudResource{}).
		Where("id = ? AND generation = ?", id, generation).
		Updates(updates)
	return result.RowsAffected == 1, result.Error
}

func (r *CloudResourceRepository) RecordReconcileFailure(ctx context.Context, id, generation uint64, message string) (bool, error) {
	returnValue := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&domain.CloudResource{}).
			Where("id = ? AND generation = ?", id, generation).
			Updates(map[string]any{
				"actual_state":          domain.CloudResourceError,
				"last_error":            message,
				"reconcile_lease_until": nil,
				"last_reconcile_at":     time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return nil
		}
		returnValue = true
		return tx.Model(&domain.CloudOrder{}).
			Where("resource_id = ? AND requested_generation = ? AND status IN ?", id, generation, []domain.CloudOrderStatus{domain.CloudOrderPending, domain.CloudOrderRunning}).
			Updates(map[string]any{
				"status":     domain.CloudOrderRunning,
				"last_error": message,
			}).Error
	})
	return returnValue, err
}

func (r *CloudResourceRepository) CompleteReconcile(ctx context.Context, id, generation uint64, state domain.CloudResourceActualState, actualSpec, providerID string, settledQuota cloudcontrol.QuotaDemand) (bool, error) {
	completed := false
	now := time.Now()
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var resource domain.CloudResource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&resource, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCloudResourceNotFound
			}
			return err
		}
		if resource.Generation != generation {
			return nil
		}
		if settledQuota.CPUMilli > resource.QuotaCPUMilli || settledQuota.MemoryMiB > resource.QuotaMemoryMiB || settledQuota.Replicas > resource.QuotaReplicas {
			return fmt.Errorf("quota invariant violated: settled demand exceeds admitted charge for resource=%d generation=%d", id, generation)
		}
		if err := r.releaseQuotaForResource(tx, &resource, cloudcontrol.QuotaDemand{
			CPUMilli:  resource.QuotaCPUMilli - settledQuota.CPUMilli,
			MemoryMiB: resource.QuotaMemoryMiB - settledQuota.MemoryMiB,
			Replicas:  resource.QuotaReplicas - settledQuota.Replicas,
		}); err != nil {
			return err
		}
		updates := map[string]any{
			"actual_state":          state,
			"actual_spec":           actualSpec,
			"observed_generation":   generation,
			"quota_cpu_milli":       settledQuota.CPUMilli,
			"quota_memory_mib":      settledQuota.MemoryMiB,
			"quota_replicas":        settledQuota.Replicas,
			"last_error":            "",
			"reconcile_lease_until": nil,
			"last_reconcile_at":     now,
		}
		if providerID != "" {
			updates["provider_resource_id"] = providerID
		}
		result := tx.Model(&domain.CloudResource{}).
			Where("id = ? AND generation = ?", id, generation).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return nil
		}
		completed = true
		return tx.Model(&domain.CloudOrder{}).
			Where("resource_id = ? AND requested_generation = ? AND status IN ?", id, generation, []domain.CloudOrderStatus{domain.CloudOrderPending, domain.CloudOrderRunning}).
			Updates(map[string]any{
				"status":       domain.CloudOrderSucceeded,
				"last_error":   "",
				"completed_at": now,
			}).Error
	})
	return completed, err
}

func supersedeCloudOrders(tx *gorm.DB, resourceID, newGeneration uint64) error {
	now := time.Now()
	return tx.Model(&domain.CloudOrder{}).
		Where("resource_id = ? AND requested_generation < ? AND status IN ?", resourceID, newGeneration, []domain.CloudOrderStatus{domain.CloudOrderPending, domain.CloudOrderRunning}).
		Updates(map[string]any{
			"status":       domain.CloudOrderSuperseded,
			"last_error":   "superseded by a newer desired generation",
			"completed_at": now,
		}).Error
}

func (r *CloudResourceRepository) reserveQuotaForResource(tx *gorm.DB, resource *domain.CloudResource, demand cloudcontrol.QuotaDemand) error {
	if resource == nil {
		return errors.New("nil cloud resource for quota reservation")
	}
	if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
		return r.reserveQuotaBucketed(tx, resource, demand)
	}
	return r.reserveQuota(tx, resource.TenantID, demand)
}

func (r *CloudResourceRepository) releaseQuotaForResource(tx *gorm.DB, resource *domain.CloudResource, released cloudcontrol.QuotaDemand) error {
	if resource == nil {
		return errors.New("nil cloud resource for quota release")
	}
	if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
		return r.releaseQuotaBucketed(tx, resource, released)
	}
	return r.releaseQuota(tx, resource.TenantID, released)
}

func quotaDemandZero(demand cloudcontrol.QuotaDemand) bool {
	return demand.CPUMilli == 0 && demand.MemoryMiB == 0 && demand.Replicas == 0
}

func quotaBucketStart(resource *domain.CloudResource, count int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(resource.TenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(resource.Provider))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(resource.Name))
	return int(h.Sum32()%uint32(count)) + 1
}

func splitQuotaLimit(total int64, bucketID, count int) int64 {
	if count <= 0 || total <= 0 {
		return 0
	}
	base := total / int64(count)
	remainder := total % int64(count)
	if int64(bucketID) <= remainder {
		return base + 1
	}
	return base
}

func (r *CloudResourceRepository) reserveQuotaBucketed(tx *gorm.DB, resource *domain.CloudResource, demand cloudcontrol.QuotaDemand) error {
	if !demand.NonNegative() {
		return errors.New("quota demand must be non-negative")
	}
	if quotaDemandZero(demand) {
		return nil
	}
	var quota domain.CloudTenantQuota
	if err := tx.Select("tenant_id", "accounting_mode", "cpu_milli_limit", "memory_mib_limit", "replica_limit").
		First(&quota, "tenant_id = ?", resource.TenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: tenant=%s", ErrCloudQuotaNotFound, resource.TenantID)
		}
		return err
	}
	if quota.AccountingMode != CloudQuotaAccountingBucketed {
		return fmt.Errorf("%w: tenant=%s expected=%s actual=%s", ErrCloudQuotaModeMismatch, resource.TenantID, CloudQuotaAccountingBucketed, quota.AccountingMode)
	}

	capacityWhere := "tenant_id = ? AND cpu_milli_used + ? <= cpu_milli_limit AND memory_mib_used + ? <= memory_mib_limit AND replica_used + ? <= replica_limit"
	capacityArgs := []any{resource.TenantID, demand.CPUMilli, demand.MemoryMiB, demand.Replicas}
	selectBucket := func(skipLocked bool) (*domain.CloudTenantQuotaBucket, error) {
		var bucket domain.CloudTenantQuotaBucket
		query := tx.Where(capacityWhere, capacityArgs...)
		if resource.QuotaBucketID != 0 {
			query = query.Where("bucket_id = ?", resource.QuotaBucketID)
		} else {
			query = query.Order("bucket_id ASC")
		}
		locking := clause.Locking{Strength: "UPDATE"}
		if skipLocked {
			locking.Options = "SKIP LOCKED"
		}
		err := query.Clauses(locking).First(&bucket).Error
		if err != nil {
			return nil, err
		}
		return &bucket, nil
	}

	// Existing resources must stay on their original bucket so resize/delete
	// and compensation release the exact reservation row. New resources first
	// use SKIP LOCKED so concurrent admissions naturally spread over independent
	// rows. If every eligible row is only temporarily locked, retry once with a
	// blocking locking read; this avoids false quota rejection without ever
	// holding more than one bucket lock in a transaction.
	var bucket *domain.CloudTenantQuotaBucket
	var err error
	if resource.QuotaBucketID != 0 {
		bucket, err = selectBucket(false)
	} else {
		bucket, err = selectBucket(true)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			bucket, err = selectBucket(false)
		}
	}
	if err == nil {
		resource.QuotaBucketID = bucket.BucketID
		result := tx.Model(&domain.CloudTenantQuotaBucket{}).
			Where("tenant_id = ? AND bucket_id = ?", resource.TenantID, bucket.BucketID).
			Updates(map[string]any{
				"cpu_milli_used":  gorm.Expr("cpu_milli_used + ?", demand.CPUMilli),
				"memory_mib_used": gorm.Expr("memory_mib_used + ?", demand.MemoryMiB),
				"replica_used":    gorm.Expr("replica_used + ?", demand.Replicas),
				"version":         gorm.Expr("version + 1"),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}
		return errors.New("quota bucket locked row disappeared during reservation")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	var used struct {
		CPUMilli int64
		Memory   int64
		Replicas int64
	}
	if err := tx.Model(&domain.CloudTenantQuotaBucket{}).
		Select("COALESCE(SUM(cpu_milli_used),0) AS cpu_milli, COALESCE(SUM(memory_mib_used),0) AS memory, COALESCE(SUM(replica_used),0) AS replicas").
		Where("tenant_id = ?", resource.TenantID).
		Scan(&used).Error; err != nil {
		return err
	}
	return fmt.Errorf("%w: tenant=%s bucket=%d requested(cpu_milli=%d memory_mib=%d replicas=%d) aggregate_available(cpu_milli=%d memory_mib=%d replicas=%d)",
		ErrCloudQuotaExceeded,
		resource.TenantID,
		resource.QuotaBucketID,
		demand.CPUMilli,
		demand.MemoryMiB,
		demand.Replicas,
		quota.CPUMilliLimit-used.CPUMilli,
		quota.MemoryMiBLimit-used.Memory,
		quota.ReplicaLimit-used.Replicas,
	)
}

func (r *CloudResourceRepository) releaseQuotaBucketed(tx *gorm.DB, resource *domain.CloudResource, released cloudcontrol.QuotaDemand) error {
	if !released.NonNegative() {
		return errors.New("quota release must be non-negative")
	}
	if quotaDemandZero(released) {
		return nil
	}
	if resource.QuotaBucketID == 0 {
		return fmt.Errorf("%w: tenant=%s resource=%d missing quota bucket", ErrCloudQuotaModeMismatch, resource.TenantID, resource.ID)
	}
	result := tx.Model(&domain.CloudTenantQuotaBucket{}).
		Where(
			"tenant_id = ? AND bucket_id = ? AND cpu_milli_used >= ? AND memory_mib_used >= ? AND replica_used >= ?",
			resource.TenantID, resource.QuotaBucketID, released.CPUMilli, released.MemoryMiB, released.Replicas,
		).
		Updates(map[string]any{
			"cpu_milli_used":  gorm.Expr("cpu_milli_used - ?", released.CPUMilli),
			"memory_mib_used": gorm.Expr("memory_mib_used - ?", released.MemoryMiB),
			"replica_used":    gorm.Expr("replica_used - ?", released.Replicas),
			"version":         gorm.Expr("version + 1"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var bucket domain.CloudTenantQuotaBucket
	if err := tx.First(&bucket, "tenant_id = ? AND bucket_id = ?", resource.TenantID, resource.QuotaBucketID).Error; err != nil {
		return err
	}
	return fmt.Errorf("quota bucket accounting underflow: tenant=%s bucket=%d release(cpu_milli=%d memory_mib=%d replicas=%d) used(cpu_milli=%d memory_mib=%d replicas=%d)",
		resource.TenantID,
		resource.QuotaBucketID,
		released.CPUMilli,
		released.MemoryMiB,
		released.Replicas,
		bucket.CPUMilliUsed,
		bucket.MemoryMiBUsed,
		bucket.ReplicaUsed,
	)
}

func (r *CloudResourceRepository) reserveQuota(tx *gorm.DB, tenantID string, demand cloudcontrol.QuotaDemand) error {
	if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
		return fmt.Errorf("%w: bucketed quota requires resource-aware reservation", ErrCloudQuotaModeMismatch)
	}
	if r.quotaAdmissionMode == CloudQuotaAdmissionAtomicVersioned && tx.Dialector.Name() == "mysql" {
		return reserveQuotaAtomicVersioned(tx, tenantID, demand)
	}
	return reserveQuotaLocked(tx, tenantID, demand)
}

func reserveQuotaLocked(tx *gorm.DB, tenantID string, demand cloudcontrol.QuotaDemand) error {
	if !demand.NonNegative() {
		return errors.New("quota demand must be non-negative")
	}
	if demand.CPUMilli == 0 && demand.MemoryMiB == 0 && demand.Replicas == 0 {
		return nil
	}
	var quota domain.CloudTenantQuota
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&quota, "tenant_id = ?", tenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: tenant=%s", ErrCloudQuotaNotFound, tenantID)
		}
		return err
	}
	if quota.AccountingMode == CloudQuotaAccountingBucketed {
		return fmt.Errorf("%w: tenant=%s expected=%s actual=%s", ErrCloudQuotaModeMismatch, tenantID, CloudQuotaAccountingSingle, quota.AccountingMode)
	}
	availableCPU := quota.CPUMilliLimit - quota.CPUMilliUsed
	availableMemory := quota.MemoryMiBLimit - quota.MemoryMiBUsed
	availableReplicas := quota.ReplicaLimit - quota.ReplicaUsed
	if demand.CPUMilli > availableCPU || demand.MemoryMiB > availableMemory || demand.Replicas > availableReplicas {
		return fmt.Errorf("%w: tenant=%s requested(cpu_milli=%d memory_mib=%d replicas=%d) available(cpu_milli=%d memory_mib=%d replicas=%d)",
			ErrCloudQuotaExceeded,
			tenantID,
			demand.CPUMilli,
			demand.MemoryMiB,
			demand.Replicas,
			availableCPU,
			availableMemory,
			availableReplicas,
		)
	}
	return tx.Model(&domain.CloudTenantQuota{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]any{
			"cpu_milli_used":  gorm.Expr("cpu_milli_used + ?", demand.CPUMilli),
			"memory_mib_used": gorm.Expr("memory_mib_used + ?", demand.MemoryMiB),
			"replica_used":    gorm.Expr("replica_used + ?", demand.Replicas),
			"version":         gorm.Expr("version + 1"),
		}).Error
}

func reserveQuotaAtomicVersioned(tx *gorm.DB, tenantID string, demand cloudcontrol.QuotaDemand) error {
	if !demand.NonNegative() {
		return errors.New("quota demand must be non-negative")
	}
	if demand.CPUMilli == 0 && demand.MemoryMiB == 0 && demand.Replicas == 0 {
		return nil
	}
	result := tx.Model(&domain.CloudTenantQuota{}).
		Where(
			"tenant_id = ? AND COALESCE(accounting_mode, ?) <> ? AND cpu_milli_used + ? <= cpu_milli_limit AND memory_mib_used + ? <= memory_mib_limit AND replica_used + ? <= replica_limit",
			tenantID, CloudQuotaAccountingSingle, CloudQuotaAccountingBucketed, demand.CPUMilli, demand.MemoryMiB, demand.Replicas,
		).
		Updates(map[string]any{
			"cpu_milli_used":  gorm.Expr("cpu_milli_used + ?", demand.CPUMilli),
			"memory_mib_used": gorm.Expr("memory_mib_used + ?", demand.MemoryMiB),
			"replica_used":    gorm.Expr("replica_used + ?", demand.Replicas),
			"version":         gorm.Expr("version + 1"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var quota domain.CloudTenantQuota
	if err := tx.First(&quota, "tenant_id = ?", tenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: tenant=%s", ErrCloudQuotaNotFound, tenantID)
		}
		return err
	}
	if quota.AccountingMode == CloudQuotaAccountingBucketed {
		return fmt.Errorf("%w: tenant=%s expected=%s actual=%s", ErrCloudQuotaModeMismatch, tenantID, CloudQuotaAccountingSingle, quota.AccountingMode)
	}
	return fmt.Errorf("%w: tenant=%s requested(cpu_milli=%d memory_mib=%d replicas=%d) available(cpu_milli=%d memory_mib=%d replicas=%d)",
		ErrCloudQuotaExceeded,
		tenantID,
		demand.CPUMilli,
		demand.MemoryMiB,
		demand.Replicas,
		quota.CPUMilliLimit-quota.CPUMilliUsed,
		quota.MemoryMiBLimit-quota.MemoryMiBUsed,
		quota.ReplicaLimit-quota.ReplicaUsed,
	)
}

func (r *CloudResourceRepository) releaseQuota(tx *gorm.DB, tenantID string, released cloudcontrol.QuotaDemand) error {
	if r.quotaAdmissionMode == CloudQuotaAdmissionBucketed {
		return fmt.Errorf("%w: bucketed quota requires resource-aware release", ErrCloudQuotaModeMismatch)
	}
	if r.quotaAdmissionMode == CloudQuotaAdmissionAtomicVersioned && tx.Dialector.Name() == "mysql" {
		return releaseQuotaAtomicVersioned(tx, tenantID, released)
	}
	return releaseQuotaLocked(tx, tenantID, released)
}

func releaseQuotaLocked(tx *gorm.DB, tenantID string, released cloudcontrol.QuotaDemand) error {
	if !released.NonNegative() {
		return errors.New("quota release must be non-negative")
	}
	if released.CPUMilli == 0 && released.MemoryMiB == 0 && released.Replicas == 0 {
		return nil
	}
	var quota domain.CloudTenantQuota
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&quota, "tenant_id = ?", tenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: tenant=%s", ErrCloudQuotaNotFound, tenantID)
		}
		return err
	}
	if released.CPUMilli > quota.CPUMilliUsed || released.MemoryMiB > quota.MemoryMiBUsed || released.Replicas > quota.ReplicaUsed {
		return fmt.Errorf("quota accounting underflow: tenant=%s release(cpu_milli=%d memory_mib=%d replicas=%d) used(cpu_milli=%d memory_mib=%d replicas=%d)",
			tenantID,
			released.CPUMilli,
			released.MemoryMiB,
			released.Replicas,
			quota.CPUMilliUsed,
			quota.MemoryMiBUsed,
			quota.ReplicaUsed,
		)
	}
	return tx.Model(&domain.CloudTenantQuota{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]any{
			"cpu_milli_used":  gorm.Expr("cpu_milli_used - ?", released.CPUMilli),
			"memory_mib_used": gorm.Expr("memory_mib_used - ?", released.MemoryMiB),
			"replica_used":    gorm.Expr("replica_used - ?", released.Replicas),
			"version":         gorm.Expr("version + 1"),
		}).Error
}

func releaseQuotaAtomicVersioned(tx *gorm.DB, tenantID string, released cloudcontrol.QuotaDemand) error {
	if !released.NonNegative() {
		return errors.New("quota release must be non-negative")
	}
	if released.CPUMilli == 0 && released.MemoryMiB == 0 && released.Replicas == 0 {
		return nil
	}
	result := tx.Model(&domain.CloudTenantQuota{}).
		Where(
			"tenant_id = ? AND cpu_milli_used >= ? AND memory_mib_used >= ? AND replica_used >= ?",
			tenantID, released.CPUMilli, released.MemoryMiB, released.Replicas,
		).
		Updates(map[string]any{
			"cpu_milli_used":  gorm.Expr("cpu_milli_used - ?", released.CPUMilli),
			"memory_mib_used": gorm.Expr("memory_mib_used - ?", released.MemoryMiB),
			"replica_used":    gorm.Expr("replica_used - ?", released.Replicas),
			"version":         gorm.Expr("version + 1"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var quota domain.CloudTenantQuota
	if err := tx.First(&quota, "tenant_id = ?", tenantID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: tenant=%s", ErrCloudQuotaNotFound, tenantID)
		}
		return err
	}
	return fmt.Errorf("quota accounting underflow: tenant=%s release(cpu_milli=%d memory_mib=%d replicas=%d) used(cpu_milli=%d memory_mib=%d replicas=%d)",
		tenantID,
		released.CPUMilli,
		released.MemoryMiB,
		released.Replicas,
		quota.CPUMilliUsed,
		quota.MemoryMiBUsed,
		quota.ReplicaUsed,
	)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
