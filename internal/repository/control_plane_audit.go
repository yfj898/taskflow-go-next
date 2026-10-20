package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *TaskRepository) CreateControlPlaneAudit(ctx context.Context, audit *domain.ControlPlaneAudit) error {
	return r.db.WithContext(ctx).Create(audit).Error
}

func (r *TaskRepository) CompleteControlPlaneAudit(ctx context.Context, id uint64, statusCode int, outcome string, completedAt time.Time) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state, err := lockControlPlaneAuditChainState(tx)
		if err != nil {
			return err
		}
		var audit domain.ControlPlaneAudit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&audit, id).Error; err != nil {
			return err
		}
		if audit.Outcome != "started" || audit.ChainSequence != nil {
			return fmt.Errorf("control-plane audit %d is not pending completion", id)
		}

		completedAt = completedAt.UTC().Truncate(time.Millisecond)
		sequence := state.LastSequence + 1
		audit.StatusCode = statusCode
		audit.Outcome = outcome
		audit.CompletedAt = &completedAt
		audit.ChainSequence = &sequence
		audit.PreviousHash = state.HeadHash
		recordHash, err := domain.ComputeControlPlaneAuditHash(audit)
		if err != nil {
			return err
		}
		audit.RecordHash = recordHash

		result := tx.Model(&domain.ControlPlaneAudit{}).
			Where("id = ? AND outcome = ? AND chain_sequence IS NULL", id, "started").
			Updates(map[string]any{
				"status_code":    audit.StatusCode,
				"outcome":        audit.Outcome,
				"completed_at":   audit.CompletedAt,
				"chain_sequence": sequence,
				"previous_hash":  audit.PreviousHash,
				"record_hash":    audit.RecordHash,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("control-plane audit %d completion rows=%d", id, result.RowsAffected)
		}
		if r.controlPlaneSIEMDelivery {
			if err := createControlPlaneAuditDelivery(tx, audit, completedAt); err != nil {
				return err
			}
		}
		state.LastSequence = sequence
		state.HeadHash = recordHash
		state.UpdatedAt = completedAt
		return tx.Save(state).Error
	})
}

func (r *TaskRepository) ListControlPlaneAudits(ctx context.Context, limit, offset int) ([]domain.ControlPlaneAudit, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var audits []domain.ControlPlaneAudit
	err := r.db.WithContext(ctx).
		Order("id DESC").
		Limit(limit).
		Offset(offset).
		Find(&audits).Error
	return audits, err
}

func (r *TaskRepository) ListControlPlaneAuditsAfterSequence(ctx context.Context, after uint64, limit int) ([]domain.ControlPlaneAudit, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	var audits []domain.ControlPlaneAudit
	err := r.db.WithContext(ctx).
		Where("chain_sequence IS NOT NULL AND chain_sequence > ?", after).
		Order("chain_sequence ASC").
		Limit(limit).
		Find(&audits).Error
	return audits, err
}

func (r *TaskRepository) GetControlPlaneAuditCheckpoint(ctx context.Context) (domain.ControlPlaneAuditChainState, error) {
	var state domain.ControlPlaneAuditChainState
	err := r.db.WithContext(ctx).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.ControlPlaneAuditChainState{ID: 1}, nil
	}
	return state, err
}

func (r *TaskRepository) SealUnchainedControlPlaneAudits(ctx context.Context, batch int) (int, error) {
	if batch <= 0 || batch > 5000 {
		batch = 1000
	}
	sealed := 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state, err := lockControlPlaneAuditChainState(tx)
		if err != nil {
			return err
		}
		var audits []domain.ControlPlaneAudit
		if err := tx.
			Where("chain_sequence IS NULL AND outcome <> ? AND completed_at IS NOT NULL", "started").
			Order("id ASC").
			Limit(batch).
			Find(&audits).Error; err != nil {
			return err
		}
		for i := range audits {
			sequence := state.LastSequence + 1
			audits[i].ChainSequence = &sequence
			audits[i].PreviousHash = state.HeadHash
			recordHash, err := domain.ComputeControlPlaneAuditHash(audits[i])
			if err != nil {
				return err
			}
			result := tx.Model(&domain.ControlPlaneAudit{}).
				Where("id = ? AND chain_sequence IS NULL", audits[i].ID).
				Updates(map[string]any{
					"chain_sequence": sequence,
					"previous_hash":  audits[i].PreviousHash,
					"record_hash":    recordHash,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("seal control-plane audit %d rows=%d", audits[i].ID, result.RowsAffected)
			}
			audits[i].RecordHash = recordHash
			if r.controlPlaneSIEMDelivery {
				if audits[i].CompletedAt == nil {
					return fmt.Errorf("seal control-plane audit %d missing completion time", audits[i].ID)
				}
				if err := createControlPlaneAuditDelivery(tx, audits[i], *audits[i].CompletedAt); err != nil {
					return err
				}
			}
			state.LastSequence = sequence
			state.HeadHash = recordHash
			sealed++
		}
		if sealed > 0 {
			state.UpdatedAt = time.Now().UTC().Truncate(time.Millisecond)
			return tx.Save(state).Error
		}
		return nil
	})
	return sealed, err
}

func (r *TaskRepository) BackfillControlPlaneAuditDeliveries(ctx context.Context, batch int) (int, error) {
	if batch <= 0 || batch > 5000 {
		batch = 1000
	}
	created := 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var audits []domain.ControlPlaneAudit
		if err := tx.
			Where("chain_sequence IS NOT NULL AND NOT EXISTS (?) AND NOT EXISTS (?)", tx.
				Model(&domain.ControlPlaneAuditDelivery{}).
				Select("1").
				Where("control_plane_audit_deliveries.chain_sequence = control_plane_audits.chain_sequence"), tx.
				Model(&domain.ControlPlaneAuditDeliveryReceipt{}).
				Select("1").
				Where("control_plane_audit_delivery_receipts.chain_sequence = control_plane_audits.chain_sequence")).
			Order("chain_sequence ASC").
			Limit(batch).
			Find(&audits).Error; err != nil {
			return err
		}
		for _, audit := range audits {
			if audit.CompletedAt == nil {
				return fmt.Errorf("control-plane audit %d sequence=%d missing completion time", audit.ID, *audit.ChainSequence)
			}
			if err := createControlPlaneAuditDelivery(tx, audit, *audit.CompletedAt); err != nil {
				return err
			}
			created++
		}
		return nil
	})
	return created, err
}

func createControlPlaneAuditDelivery(tx *gorm.DB, audit domain.ControlPlaneAudit, readyAt time.Time) error {
	if audit.ChainSequence == nil || audit.RecordHash == "" {
		return fmt.Errorf("control-plane audit %d is not sealed for delivery", audit.ID)
	}
	payload, err := json.Marshal(audit)
	if err != nil {
		return fmt.Errorf("marshal control-plane audit delivery: %w", err)
	}
	delivery := domain.ControlPlaneAuditDelivery{
		AuditID:       audit.ID,
		ChainSequence: *audit.ChainSequence,
		RecordHash:    audit.RecordHash,
		PayloadJSON:   string(payload),
		State:         domain.ControlPlaneDeliveryPending,
		NextAttemptAt: readyAt.UTC().Truncate(time.Millisecond),
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_sequence"}},
		DoNothing: true,
	}).Create(&delivery).Error
}

func (r *TaskRepository) VerifyControlPlaneAuditChain(ctx context.Context) (domain.ControlPlaneAuditIntegrityReport, error) {
	report := domain.ControlPlaneAuditIntegrityReport{Valid: true}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state, err := readControlPlaneAuditChainState(tx)
		if err != nil {
			return err
		}
		report.LastSequence = state.LastSequence
		report.HeadHash = state.HeadHash
		report.RetentionAnchorSequence = state.RetentionAnchorSequence
		report.RetentionAnchorHash = state.RetentionAnchorHash

		var unsealed int64
		if err := tx.Model(&domain.ControlPlaneAudit{}).
			Where("chain_sequence IS NULL AND outcome <> ? AND completed_at IS NOT NULL", "started").
			Count(&unsealed).Error; err != nil {
			return err
		}
		if unsealed > 0 {
			report.Valid = false
			report.Failure = fmt.Sprintf("%d completed audit rows are not sealed", unsealed)
			return nil
		}

		var audits []domain.ControlPlaneAudit
		if err := tx.
			Where("chain_sequence IS NOT NULL AND chain_sequence > ?", state.RetentionAnchorSequence).
			Order("chain_sequence ASC").
			Find(&audits).Error; err != nil {
			return err
		}
		expectedSequence := state.RetentionAnchorSequence + 1
		expectedPrevious := state.RetentionAnchorHash
		for _, audit := range audits {
			if audit.ChainSequence == nil || *audit.ChainSequence != expectedSequence {
				report.Valid = false
				report.Failure = fmt.Sprintf("chain sequence gap at expected=%d", expectedSequence)
				return nil
			}
			if audit.PreviousHash != expectedPrevious {
				report.Valid = false
				report.Failure = fmt.Sprintf("previous hash mismatch at sequence=%d", expectedSequence)
				return nil
			}
			recomputed, err := domain.ComputeControlPlaneAuditHash(audit)
			if err != nil {
				return err
			}
			if recomputed != audit.RecordHash {
				report.Valid = false
				report.Failure = fmt.Sprintf("record hash mismatch at sequence=%d", expectedSequence)
				return nil
			}
			report.Checked++
			expectedPrevious = audit.RecordHash
			expectedSequence++
		}

		if expectedSequence-1 != state.LastSequence {
			report.Valid = false
			report.Failure = fmt.Sprintf("chain head sequence mismatch: verified=%d state=%d", expectedSequence-1, state.LastSequence)
			return nil
		}
		if state.LastSequence == state.RetentionAnchorSequence {
			expectedPrevious = state.RetentionAnchorHash
		}
		if expectedPrevious != state.HeadHash {
			report.Valid = false
			report.Failure = "chain head hash mismatch"
		}
		return nil
	})
	return report, err
}

func (r *TaskRepository) PurgeControlPlaneAuditsBefore(ctx context.Context, cutoff time.Time, batch int) (int, error) {
	if batch <= 0 || batch > 5000 {
		batch = 1000
	}
	purged := 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state, err := lockControlPlaneAuditChainState(tx)
		if err != nil {
			return err
		}
		var audits []domain.ControlPlaneAudit
		if err := tx.
			Where("chain_sequence IS NOT NULL AND chain_sequence > ?", state.RetentionAnchorSequence).
			Order("chain_sequence ASC").
			Limit(batch).
			Find(&audits).Error; err != nil {
			return err
		}

		var ids []uint64
		anchorSequence := state.RetentionAnchorSequence
		anchorHash := state.RetentionAnchorHash
		for _, audit := range audits {
			if audit.CompletedAt == nil || !audit.CompletedAt.Before(cutoff) {
				break
			}
			ids = append(ids, audit.ID)
			anchorSequence = *audit.ChainSequence
			anchorHash = audit.RecordHash
		}
		if len(ids) == 0 {
			return nil
		}
		result := tx.Where("id IN ?", ids).Delete(&domain.ControlPlaneAudit{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(ids)) {
			return fmt.Errorf("control-plane retention deleted=%d expected=%d", result.RowsAffected, len(ids))
		}
		state.RetentionAnchorSequence = anchorSequence
		state.RetentionAnchorHash = anchorHash
		state.UpdatedAt = time.Now().UTC().Truncate(time.Millisecond)
		if err := tx.Save(state).Error; err != nil {
			return err
		}
		purged = len(ids)
		return nil
	})
	return purged, err
}

func lockControlPlaneAuditChainState(tx *gorm.DB) (*domain.ControlPlaneAuditChainState, error) {
	seed := domain.ControlPlaneAuditChainState{ID: 1}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
		return nil, err
	}
	var state domain.ControlPlaneAuditChainState
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func readControlPlaneAuditChainState(tx *gorm.DB) (*domain.ControlPlaneAuditChainState, error) {
	var state domain.ControlPlaneAuditChainState
	err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &domain.ControlPlaneAuditChainState{ID: 1}, nil
	}
	return &state, err
}
