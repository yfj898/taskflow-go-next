package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrAIBudgetAccountNotFound = errors.New("AI budget account not found")
var ErrAIBudgetExceeded = errors.New("AI budget exceeded")

type AIBudgetExceededError struct {
	Scope          string
	ScopeKey       string
	Kind           string
	RequestedUSD   float64
	RequestedToken int64
}

func (e *AIBudgetExceededError) Error() string {
	return fmt.Sprintf("%s: scope=%s key=%s kind=%s", ErrAIBudgetExceeded, e.Scope, e.ScopeKey, e.Kind)
}

func (e *AIBudgetExceededError) Unwrap() error { return ErrAIBudgetExceeded }

func normalizeBudgetScope(scope, key string) (string, string, error) {
	scope = strings.ToLower(strings.TrimSpace(scope))
	key = strings.TrimSpace(key)
	switch scope {
	case domain.AIBudgetScopeGlobal:
		return scope, domain.AIBudgetScopeGlobal, nil
	case domain.AIBudgetScopeTenant:
		if key == "" {
			return "", "", errors.New("tenant budget requires a scope key")
		}
		return scope, key, nil
	default:
		return "", "", fmt.Errorf("unsupported AI budget scope %q", scope)
	}
}

func (r *TaskRepository) UpsertAIBudgetAccount(ctx context.Context, scope, key string, limitUSD float64, limitTokens int64) (*domain.AIBudgetAccount, error) {
	scope, key, err := normalizeBudgetScope(scope, key)
	if err != nil {
		return nil, err
	}
	if limitUSD < 0 || math.IsNaN(limitUSD) || math.IsInf(limitUSD, 0) {
		return nil, errors.New("limit_usd must be a finite non-negative number")
	}
	if limitTokens < 0 {
		return nil, errors.New("limit_tokens must be non-negative")
	}

	var updated domain.AIBudgetAccount
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var account domain.AIBudgetAccount
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("scope = ? AND scope_key = ?", scope, key).
			First(&account).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			account = domain.AIBudgetAccount{
				Scope:       scope,
				ScopeKey:    key,
				LimitUSD:    limitUSD,
				LimitTokens: limitTokens,
				Version:     1,
			}
			if err := tx.Create(&account).Error; err != nil {
				return err
			}
			updated = account
			return nil
		}
		if err != nil {
			return err
		}
		account.LimitUSD = limitUSD
		account.LimitTokens = limitTokens
		account.Version++
		if err := tx.Model(&account).Updates(map[string]any{
			"limit_usd":    account.LimitUSD,
			"limit_tokens": account.LimitTokens,
			"version":      account.Version,
			"updated_at":   time.Now(),
		}).Error; err != nil {
			return err
		}
		updated = account
		updated.UpdatedAt = time.Now()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (r *TaskRepository) GetAIBudgetAccount(ctx context.Context, scope, key string) (*domain.AIBudgetAccount, error) {
	scope, key, err := normalizeBudgetScope(scope, key)
	if err != nil {
		return nil, err
	}
	var account domain.AIBudgetAccount
	err = r.db.WithContext(ctx).Where("scope = ? AND scope_key = ?", scope, key).First(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrAIBudgetAccountNotFound
	}
	if err != nil {
		return nil, err
	}
	return &account, nil
}

func (r *TaskRepository) ListAIBudgetAccounts(ctx context.Context) ([]domain.AIBudgetAccount, error) {
	var accounts []domain.AIBudgetAccount
	err := r.db.WithContext(ctx).Order("scope ASC, scope_key ASC").Find(&accounts).Error
	return accounts, err
}

func budgetFits(account *domain.AIBudgetAccount, reserveUSD float64, reserveTokens int64) (bool, string) {
	if account.LimitUSD > 0 && account.SpentUSD+account.ReservedUSD+reserveUSD > account.LimitUSD+1e-9 {
		return false, "usd"
	}
	if account.LimitTokens > 0 && account.SpentTokens+account.ReservedTokens+reserveTokens > account.LimitTokens {
		return false, "tokens"
	}
	return true, ""
}

func (r *TaskRepository) ReserveAIBudget(ctx context.Context, taskID uint64, attempt int, tenantID string, reserveUSD float64, reserveTokens int64) (*domain.AIBudgetReservation, error) {
	if reserveUSD < 0 || reserveTokens < 0 {
		return nil, errors.New("AI budget reservation must be non-negative")
	}
	tenantID = strings.TrimSpace(tenantID)
	var reservation *domain.AIBudgetReservation
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing domain.AIBudgetReservation
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("task_id = ? AND attempt = ?", taskID, attempt).
			First(&existing).Error
		if err == nil {
			reservation = &existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var global domain.AIBudgetAccount
		globalErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("scope = ? AND scope_key = ?", domain.AIBudgetScopeGlobal, domain.AIBudgetScopeGlobal).
			First(&global).Error
		if globalErr != nil && !errors.Is(globalErr, gorm.ErrRecordNotFound) {
			return globalErr
		}

		var tenant domain.AIBudgetAccount
		tenantErr := gorm.ErrRecordNotFound
		if tenantID != "" {
			tenantErr = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("scope = ? AND scope_key = ?", domain.AIBudgetScopeTenant, tenantID).
				First(&tenant).Error
			if tenantErr != nil && !errors.Is(tenantErr, gorm.ErrRecordNotFound) {
				return tenantErr
			}
		}

		applyGlobal := globalErr == nil && (global.LimitUSD > 0 || global.LimitTokens > 0)
		applyTenant := tenantErr == nil && (tenant.LimitUSD > 0 || tenant.LimitTokens > 0)
		if !applyGlobal && !applyTenant {
			return nil
		}
		if applyGlobal {
			if ok, kind := budgetFits(&global, reserveUSD, reserveTokens); !ok {
				return &AIBudgetExceededError{Scope: global.Scope, ScopeKey: global.ScopeKey, Kind: kind, RequestedUSD: reserveUSD, RequestedToken: reserveTokens}
			}
		}
		if applyTenant {
			if ok, kind := budgetFits(&tenant, reserveUSD, reserveTokens); !ok {
				return &AIBudgetExceededError{Scope: tenant.Scope, ScopeKey: tenant.ScopeKey, Kind: kind, RequestedUSD: reserveUSD, RequestedToken: reserveTokens}
			}
		}

		if applyGlobal {
			if err := tx.Model(&global).Updates(map[string]any{
				"reserved_usd":    gorm.Expr("reserved_usd + ?", reserveUSD),
				"reserved_tokens": gorm.Expr("reserved_tokens + ?", reserveTokens),
				"version":         gorm.Expr("version + 1"),
			}).Error; err != nil {
				return err
			}
		}
		if applyTenant {
			if err := tx.Model(&tenant).Updates(map[string]any{
				"reserved_usd":    gorm.Expr("reserved_usd + ?", reserveUSD),
				"reserved_tokens": gorm.Expr("reserved_tokens + ?", reserveTokens),
				"version":         gorm.Expr("version + 1"),
			}).Error; err != nil {
				return err
			}
		}

		created := &domain.AIBudgetReservation{
			TaskID: taskID, Attempt: attempt, TenantID: tenantID,
			ApplyGlobal: applyGlobal, ApplyTenant: applyTenant,
			ReservedUSD: reserveUSD, ReservedTokens: reserveTokens,
			Status: domain.AIBudgetReservationReserved,
		}
		if err := tx.Create(created).Error; err != nil {
			return err
		}
		reservation = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reservation, nil
}

func settleBudgetAccount(tx *gorm.DB, scope, key string, reservedUSD float64, reservedTokens int64, actualUSD float64, actualTokens int64) error {
	var account domain.AIBudgetAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("scope = ? AND scope_key = ?", scope, key).First(&account).Error; err != nil {
		return err
	}
	newReservedUSD := math.Max(0, account.ReservedUSD-reservedUSD)
	newReservedTokens := account.ReservedTokens - reservedTokens
	if newReservedTokens < 0 {
		newReservedTokens = 0
	}
	return tx.Model(&account).Updates(map[string]any{
		"reserved_usd":    newReservedUSD,
		"reserved_tokens": newReservedTokens,
		"spent_usd":       account.SpentUSD + actualUSD,
		"spent_tokens":    account.SpentTokens + actualTokens,
		"version":         account.Version + 1,
	}).Error
}

func releaseBudgetAccount(tx *gorm.DB, scope, key string, reservedUSD float64, reservedTokens int64) error {
	var account domain.AIBudgetAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("scope = ? AND scope_key = ?", scope, key).First(&account).Error; err != nil {
		return err
	}
	newReservedUSD := math.Max(0, account.ReservedUSD-reservedUSD)
	newReservedTokens := account.ReservedTokens - reservedTokens
	if newReservedTokens < 0 {
		newReservedTokens = 0
	}
	return tx.Model(&account).Updates(map[string]any{
		"reserved_usd":    newReservedUSD,
		"reserved_tokens": newReservedTokens,
		"version":         account.Version + 1,
	}).Error
}

func (r *TaskRepository) finalizeAIBudgetTx(tx *gorm.DB, reservation *domain.AIBudgetReservation, settle bool, actualUSD float64, actualTokens int64) error {
	if reservation == nil {
		return nil
	}
	var current domain.AIBudgetReservation
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, reservation.ID).Error; err != nil {
		return err
	}
	if current.Status != domain.AIBudgetReservationReserved {
		return nil
	}
	if settle {
		if current.ApplyGlobal {
			if err := settleBudgetAccount(tx, domain.AIBudgetScopeGlobal, domain.AIBudgetScopeGlobal, current.ReservedUSD, current.ReservedTokens, actualUSD, actualTokens); err != nil {
				return err
			}
		}
		if current.ApplyTenant {
			if err := settleBudgetAccount(tx, domain.AIBudgetScopeTenant, current.TenantID, current.ReservedUSD, current.ReservedTokens, actualUSD, actualTokens); err != nil {
				return err
			}
		}
	} else {
		if current.ApplyGlobal {
			if err := releaseBudgetAccount(tx, domain.AIBudgetScopeGlobal, domain.AIBudgetScopeGlobal, current.ReservedUSD, current.ReservedTokens); err != nil {
				return err
			}
		}
		if current.ApplyTenant {
			if err := releaseBudgetAccount(tx, domain.AIBudgetScopeTenant, current.TenantID, current.ReservedUSD, current.ReservedTokens); err != nil {
				return err
			}
		}
	}
	now := time.Now()
	status := domain.AIBudgetReservationReleased
	updates := map[string]any{"status": status, "settled_at": &now}
	if settle {
		status = domain.AIBudgetReservationSettled
		updates["status"] = status
		updates["actual_usd"] = actualUSD
		updates["actual_tokens"] = actualTokens
	}
	return tx.Model(&current).Updates(updates).Error
}
