package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrControlPlaneSIEMPromotionIntentNotFound = errors.New("control-plane SIEM promotion intent not found")
	ErrControlPlaneSIEMPromotionConflict       = errors.New("control-plane SIEM promotion intent conflict")
	ErrControlPlaneSIEMPromotionApproval       = errors.New("control-plane SIEM promotion approval required")
	ErrControlPlaneSIEMPromotionBusy           = errors.New("control-plane SIEM promotion execution is busy")
	ErrControlPlaneSIEMRPOFenced               = errors.New("control-plane SIEM receiver promotion fenced by RPO policy")
)

func (r *TaskRepository) CreateControlPlaneSIEMPromotionIntent(
	ctx context.Context,
	target string,
	expectedTerm uint64,
	requestedBy, reason string,
	requiredApprovals int,
	expiresAt, now time.Time,
) (domain.ControlPlaneSIEMPromotionIntent, error) {
	target = strings.TrimSpace(target)
	requestedBy = strings.TrimSpace(requestedBy)
	reason = strings.TrimSpace(reason)
	if target == "" || requestedBy == "" || reason == "" || expectedTerm == 0 || requiredApprovals <= 0 {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("promotion target, expected term, requester and reason are required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	expiresAt = expiresAt.UTC().Truncate(time.Millisecond)
	if !expiresAt.After(now) {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("promotion intent expiry must be in the future")
	}
	var intent domain.ControlPlaneSIEMPromotionIntent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var state domain.ControlPlaneSIEMReceiverRoutingState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if state.Term != expectedTerm {
			return fmt.Errorf("%w: current term=%d expected=%d", ErrControlPlaneSIEMPromotionConflict, state.Term, expectedTerm)
		}
		if state.ActiveReceiver == target {
			return fmt.Errorf("%w: receiver %s is already active", ErrControlPlaneSIEMPromotionConflict, target)
		}
		if err := tx.Model(&domain.ControlPlaneSIEMPromotionIntent{}).
			Where("state IN ? AND expires_at <= ?", []string{domain.ControlPlaneSIEMPromotionPending, domain.ControlPlaneSIEMPromotionApproved}, now).
			Updates(map[string]any{"state": domain.ControlPlaneSIEMPromotionExpired, "updated_at": now}).Error; err != nil {
			return err
		}
		var active int64
		if err := tx.Model(&domain.ControlPlaneSIEMPromotionIntent{}).
			Where("state IN ?", []string{domain.ControlPlaneSIEMPromotionPending, domain.ControlPlaneSIEMPromotionApproved, domain.ControlPlaneSIEMPromotionExecuting}).
			Count(&active).Error; err != nil {
			return err
		}
		if active > 0 {
			return fmt.Errorf("%w: another promotion intent is active", ErrControlPlaneSIEMPromotionConflict)
		}
		intent = domain.ControlPlaneSIEMPromotionIntent{
			TargetReceiver:    target,
			ExpectedTerm:      expectedTerm,
			State:             domain.ControlPlaneSIEMPromotionPending,
			RequestedBy:       requestedBy,
			RequiredApprovals: requiredApprovals,
			Reason:            reason,
			ExpiresAt:         expiresAt,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		return tx.Create(&intent).Error
	})
	return intent, err
}

func (r *TaskRepository) ApproveControlPlaneSIEMPromotionIntent(
	ctx context.Context,
	id uint64,
	approvedBy, note string,
	now time.Time,
) (domain.ControlPlaneSIEMPromotionIntent, error) {
	approvedBy = strings.TrimSpace(approvedBy)
	note = strings.TrimSpace(note)
	if id == 0 || approvedBy == "" {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("promotion intent id and approver are required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	var intent domain.ControlPlaneSIEMPromotionIntent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrControlPlaneSIEMPromotionIntentNotFound
			}
			return err
		}
		if intent.State == domain.ControlPlaneSIEMPromotionPending && !intent.ExpiresAt.After(now) {
			intent.State = domain.ControlPlaneSIEMPromotionExpired
			intent.UpdatedAt = now
			if err := tx.Save(&intent).Error; err != nil {
				return err
			}
			return fmt.Errorf("%w: promotion intent expired", ErrControlPlaneSIEMPromotionConflict)
		}
		if intent.State != domain.ControlPlaneSIEMPromotionPending {
			return fmt.Errorf("%w: promotion intent state=%s", ErrControlPlaneSIEMPromotionConflict, intent.State)
		}
		if intent.RequestedBy == approvedBy {
			return fmt.Errorf("%w: requester and approver must be different operators", ErrControlPlaneSIEMPromotionApproval)
		}
		approvedAt := now
		intent.State = domain.ControlPlaneSIEMPromotionApproved
		intent.ApprovedBy = approvedBy
		intent.ApprovalNote = note
		intent.ApprovedAt = &approvedAt
		intent.UpdatedAt = now
		return tx.Save(&intent).Error
	})
	return intent, err
}

func (r *TaskRepository) GetControlPlaneSIEMPromotionIntent(ctx context.Context, id uint64) (domain.ControlPlaneSIEMPromotionIntent, error) {
	var intent domain.ControlPlaneSIEMPromotionIntent
	if err := r.db.WithContext(ctx).First(&intent, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return intent, ErrControlPlaneSIEMPromotionIntentNotFound
		}
		return intent, err
	}
	return intent, nil
}

func (r *TaskRepository) ListControlPlaneSIEMPromotionIntents(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMPromotionIntent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var intents []domain.ControlPlaneSIEMPromotionIntent
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Find(&intents).Error
	return intents, err
}

func (r *TaskRepository) ClaimControlPlaneSIEMPromotionExecution(
	ctx context.Context,
	intentID uint64,
	target string,
	expectedTerm uint64,
	token string,
	leaseUntil, now time.Time,
) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	target = strings.TrimSpace(target)
	token = strings.TrimSpace(token)
	if target == "" || expectedTerm == 0 || token == "" {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, fmt.Errorf("promotion execution target, term and token are required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	leaseUntil = leaseUntil.UTC().Truncate(time.Millisecond)
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if state.Term != expectedTerm {
			return fmt.Errorf("%w: current term=%d expected=%d", ErrControlPlaneSIEMPromotionConflict, state.Term, expectedTerm)
		}
		if state.RPOFenced {
			return fmt.Errorf("%w: %s", ErrControlPlaneSIEMRPOFenced, state.RPOFenceReason)
		}
		if state.TransitionToken != "" && state.TransitionLeaseUntil != nil && state.TransitionLeaseUntil.After(now) {
			return ErrControlPlaneSIEMPromotionBusy
		}
		if intentID > 0 {
			var intent domain.ControlPlaneSIEMPromotionIntent
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, intentID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrControlPlaneSIEMPromotionIntentNotFound
				}
				return err
			}
			if intent.State != domain.ControlPlaneSIEMPromotionApproved || intent.ApprovedBy == "" || intent.RequestedBy == intent.ApprovedBy {
				return fmt.Errorf("%w: promotion intent state=%s", ErrControlPlaneSIEMPromotionApproval, intent.State)
			}
			if !intent.ExpiresAt.After(now) {
				intent.State = domain.ControlPlaneSIEMPromotionExpired
				intent.UpdatedAt = now
				if err := tx.Save(&intent).Error; err != nil {
					return err
				}
				return fmt.Errorf("%w: promotion intent expired", ErrControlPlaneSIEMPromotionConflict)
			}
			if intent.TargetReceiver != target || intent.ExpectedTerm != expectedTerm {
				return fmt.Errorf("%w: promotion intent target/term mismatch", ErrControlPlaneSIEMPromotionConflict)
			}
			intent.State = domain.ControlPlaneSIEMPromotionExecuting
			intent.UpdatedAt = now
			if err := tx.Save(&intent).Error; err != nil {
				return err
			}
		}
		state.TransitionIntentID = intentID
		state.TransitionToken = token
		state.TransitionLeaseUntil = &leaseUntil
		state.UpdatedAt = now
		return tx.Save(&state).Error
	})
	return state, err
}

func (r *TaskRepository) ReleaseControlPlaneSIEMPromotionExecution(ctx context.Context, intentID uint64, token string, now time.Time) error {
	token = strings.TrimSpace(token)
	now = now.UTC().Truncate(time.Millisecond)
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var state domain.ControlPlaneSIEMReceiverRoutingState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if state.TransitionToken != token || state.TransitionIntentID != intentID {
			return nil
		}
		state.TransitionIntentID = 0
		state.TransitionToken = ""
		state.TransitionLeaseUntil = nil
		state.UpdatedAt = now
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		if intentID > 0 {
			var intent domain.ControlPlaneSIEMPromotionIntent
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, intentID).Error; err == nil && intent.State == domain.ControlPlaneSIEMPromotionExecuting {
				intent.State = domain.ControlPlaneSIEMPromotionApproved
				intent.UpdatedAt = now
				return tx.Save(&intent).Error
			}
		}
		return nil
	})
}

func (r *TaskRepository) CommitControlPlaneSIEMPromotionExecution(
	ctx context.Context,
	target, action, promotedBy, requestedBy, approvedBy, reason string,
	expectedTerm, intentID, rpoSequences uint64,
	token string,
	promotedAt time.Time,
) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	target = strings.TrimSpace(target)
	action = strings.TrimSpace(action)
	promotedBy = strings.TrimSpace(promotedBy)
	token = strings.TrimSpace(token)
	promotedAt = promotedAt.UTC().Truncate(time.Millisecond)
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if state.Term != expectedTerm {
			return fmt.Errorf("%w: current term=%d expected=%d", ErrControlPlaneSIEMPromotionConflict, state.Term, expectedTerm)
		}
		if state.TransitionToken != token || state.TransitionIntentID != intentID || state.TransitionLeaseUntil == nil || !state.TransitionLeaseUntil.After(promotedAt) {
			return ErrControlPlaneSIEMPromotionBusy
		}
		previousReceiver := state.ActiveReceiver
		previousTerm := state.Term
		state.ActiveReceiver = target
		state.Term++
		state.PromotedBy = promotedBy
		state.PromotedAt = promotedAt
		state.TransitionIntentID = 0
		state.TransitionToken = ""
		state.TransitionLeaseUntil = nil
		state.UpdatedAt = promotedAt
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		entry := domain.ControlPlaneSIEMReceiverTermHistory{
			Term:             state.Term,
			PreviousTerm:     previousTerm,
			PreviousReceiver: previousReceiver,
			ActiveReceiver:   state.ActiveReceiver,
			Action:           action,
			IntentID:         intentID,
			RequestedBy:      strings.TrimSpace(requestedBy),
			ApprovedBy:       strings.TrimSpace(approvedBy),
			Reason:           strings.TrimSpace(reason),
			RPOSequences:     rpoSequences,
			CreatedAt:        promotedAt,
		}
		if err := appendControlPlaneSIEMTermHistoryTx(tx, &entry); err != nil {
			return err
		}
		if intentID > 0 {
			var intent domain.ControlPlaneSIEMPromotionIntent
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, intentID).Error; err != nil {
				return err
			}
			if intent.State != domain.ControlPlaneSIEMPromotionExecuting {
				return fmt.Errorf("%w: promotion intent state=%s", ErrControlPlaneSIEMPromotionConflict, intent.State)
			}
			executedAt := promotedAt
			intent.State = domain.ControlPlaneSIEMPromotionExecuted
			intent.ResultTerm = state.Term
			intent.ExecutedAt = &executedAt
			intent.UpdatedAt = promotedAt
			if err := tx.Save(&intent).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return state, err
}

func (r *TaskRepository) ObserveControlPlaneSIEMReceiverRPO(
	ctx context.Context,
	rpo, maxRPO uint64,
	grace time.Duration,
	now time.Time,
) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	now = now.UTC().Truncate(time.Millisecond)
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if rpo <= maxRPO {
			state.RPOBreachSince = nil
			state.RPOFenced = false
			state.RPOFenceReason = ""
		} else {
			if state.RPOBreachSince == nil {
				breach := now
				state.RPOBreachSince = &breach
			}
			if grace <= 0 || !now.Before(state.RPOBreachSince.Add(grace)) {
				state.RPOFenced = true
				state.RPOFenceReason = fmt.Sprintf("receiver RPO %d sequences exceeds policy limit %d", rpo, maxRPO)
			}
		}
		state.UpdatedAt = now
		return tx.Save(&state).Error
	})
	return state, err
}

func (r *TaskRepository) ListControlPlaneSIEMReceiverTermHistory(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMReceiverTermHistory, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var entries []domain.ControlPlaneSIEMReceiverTermHistory
	err := r.db.WithContext(ctx).Order("term ASC").Limit(limit).Find(&entries).Error
	return entries, err
}

func (r *TaskRepository) ensureControlPlaneSIEMTermHistory(ctx context.Context, state domain.ControlPlaneSIEMReceiverRoutingState, action string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&domain.ControlPlaneSIEMReceiverTermHistory{}).Where("term = ?", state.Term).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		entry := domain.ControlPlaneSIEMReceiverTermHistory{
			Term:           state.Term,
			ActiveReceiver: state.ActiveReceiver,
			Action:         action,
			RequestedBy:    state.PromotedBy,
			CreatedAt:      state.PromotedAt.UTC().Truncate(time.Millisecond),
		}
		return appendControlPlaneSIEMTermHistoryTx(tx, &entry)
	})
}

func appendControlPlaneSIEMTermHistoryTx(tx *gorm.DB, entry *domain.ControlPlaneSIEMReceiverTermHistory) error {
	var existing domain.ControlPlaneSIEMReceiverTermHistory
	if err := tx.Where("term = ?", entry.Term).First(&existing).Error; err == nil {
		if existing.ActiveReceiver == entry.ActiveReceiver {
			return nil
		}
		return fmt.Errorf("SIEM receiver term history conflict term=%d", entry.Term)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	var previous domain.ControlPlaneSIEMReceiverTermHistory
	err := tx.Order("term DESC").First(&previous).Error
	if err == nil {
		if previous.Term >= entry.Term {
			return fmt.Errorf("SIEM receiver term history is not monotonic: previous=%d next=%d", previous.Term, entry.Term)
		}
		entry.PreviousHash = previous.RecordHash
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	hash, err := domain.ComputeControlPlaneSIEMReceiverTermHistoryHash(*entry)
	if err != nil {
		return err
	}
	entry.RecordHash = hash
	return tx.Create(entry).Error
}
