package repository

import (
	"context"
	"errors"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrAIRouteAuditNotFound = errors.New("AI route audit not found")
var ErrAIRoutingPolicyNotFound = errors.New("AI routing policy not found")

func (r *TaskRepository) CreateAIRouteAudit(ctx context.Context, audit *domain.AIRouteAudit) error {
	return r.db.WithContext(ctx).Create(audit).Error
}

func (r *TaskRepository) ListAIRouteAudits(ctx context.Context, taskID uint64) ([]domain.AIRouteAudit, error) {
	var audits []domain.AIRouteAudit
	err := r.db.WithContext(ctx).
		Where("task_id = ?", taskID).
		Order("attempt ASC, id ASC").
		Find(&audits).Error
	return audits, err
}

func (r *TaskRepository) GetAIRouteAudit(ctx context.Context, taskID, auditID uint64) (*domain.AIRouteAudit, error) {
	var audit domain.AIRouteAudit
	err := r.db.WithContext(ctx).
		Where("id = ? AND task_id = ?", auditID, taskID).
		First(&audit).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrAIRouteAuditNotFound
	}
	if err != nil {
		return nil, err
	}
	return &audit, nil
}

func (r *TaskRepository) GetAIRoutingPolicy(ctx context.Context) (*domain.AIRoutingPolicy, error) {
	var policy domain.AIRoutingPolicy
	err := r.db.WithContext(ctx).First(&policy, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrAIRoutingPolicyNotFound
	}
	if err != nil {
		return nil, err
	}
	return &policy, nil
}

func (r *TaskRepository) UpsertAIRoutingPolicy(ctx context.Context, policy *domain.AIRoutingPolicy) error {
	policy.ID = 1
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"version",
			"strategy",
			"default_output_tokens",
			"tenant_quota_managed",
			"tenant_quota_enabled",
			"tenant_quota_default_limit",
			"tenant_quota_overrides",
			"updated_at",
		}),
	}).Create(policy).Error
}

func (r *TaskRepository) UpdateAIRoutingPolicy(ctx context.Context, strategy string, defaultOutputTokens int) (*domain.AIRoutingPolicy, error) {
	var updated domain.AIRoutingPolicy
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current domain.AIRoutingPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, 1).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAIRoutingPolicyNotFound
			}
			return err
		}
		updated = domain.AIRoutingPolicy{
			ID:                      1,
			Version:                 current.Version + 1,
			Strategy:                strategy,
			DefaultOutputTokens:     defaultOutputTokens,
			TenantQuotaManaged:      current.TenantQuotaManaged,
			TenantQuotaEnabled:      current.TenantQuotaEnabled,
			TenantQuotaDefaultLimit: current.TenantQuotaDefaultLimit,
			TenantQuotaOverrides:    current.TenantQuotaOverrides,
			UpdatedAt:               time.Now(),
		}
		return tx.Model(&domain.AIRoutingPolicy{}).
			Where("id = ?", 1).
			Updates(map[string]any{
				"version":               updated.Version,
				"strategy":              updated.Strategy,
				"default_output_tokens": updated.DefaultOutputTokens,
				"updated_at":            updated.UpdatedAt,
			}).Error
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (r *TaskRepository) UpdateAITenantQuotaPolicy(ctx context.Context, enabled bool, defaultLimit int, overrides string) (*domain.AIRoutingPolicy, error) {
	var updated domain.AIRoutingPolicy
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current domain.AIRoutingPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, 1).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAIRoutingPolicyNotFound
			}
			return err
		}
		updated = current
		updated.Version = current.Version + 1
		updated.TenantQuotaManaged = true
		updated.TenantQuotaEnabled = enabled
		updated.TenantQuotaDefaultLimit = defaultLimit
		updated.TenantQuotaOverrides = overrides
		updated.UpdatedAt = time.Now()
		return tx.Model(&domain.AIRoutingPolicy{}).
			Where("id = ?", 1).
			Updates(map[string]any{
				"version":                    updated.Version,
				"tenant_quota_managed":       updated.TenantQuotaManaged,
				"tenant_quota_enabled":       updated.TenantQuotaEnabled,
				"tenant_quota_default_limit": updated.TenantQuotaDefaultLimit,
				"tenant_quota_overrides":     updated.TenantQuotaOverrides,
				"updated_at":                 updated.UpdatedAt,
			}).Error
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (r *TaskRepository) ResetAITenantQuotaPolicy(ctx context.Context) (*domain.AIRoutingPolicy, error) {
	var updated domain.AIRoutingPolicy
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current domain.AIRoutingPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, 1).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAIRoutingPolicyNotFound
			}
			return err
		}
		updated = current
		updated.Version = current.Version + 1
		updated.TenantQuotaManaged = false
		updated.TenantQuotaEnabled = false
		updated.TenantQuotaDefaultLimit = 0
		updated.TenantQuotaOverrides = ""
		updated.UpdatedAt = time.Now()
		return tx.Model(&domain.AIRoutingPolicy{}).
			Where("id = ?", 1).
			Updates(map[string]any{
				"version":                    updated.Version,
				"tenant_quota_managed":       updated.TenantQuotaManaged,
				"tenant_quota_enabled":       updated.TenantQuotaEnabled,
				"tenant_quota_default_limit": updated.TenantQuotaDefaultLimit,
				"tenant_quota_overrides":     updated.TenantQuotaOverrides,
				"updated_at":                 updated.UpdatedAt,
			}).Error
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}
