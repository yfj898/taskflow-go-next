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
	ErrControlPlaneDeliveryDLQNotFound = errors.New("control-plane delivery DLQ row not found")
	ErrControlPlaneSIEMStaleFence      = errors.New("stale control-plane SIEM delivery leader fence")
)

func (r *TaskRepository) ActivateControlPlaneSIEMDeliveryFence(ctx context.Context, candidate uint64) (uint64, error) {
	var active uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seed := domain.ControlPlaneSIEMDeliveryCoordinatorState{ID: 1}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
			return err
		}
		var state domain.ControlPlaneSIEMDeliveryCoordinatorState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		next := state.ActiveFence + 1
		if candidate > next {
			next = candidate
		}
		state.ActiveFence = next
		state.UpdatedAt = time.Now().UTC().Truncate(time.Millisecond)
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		active = next
		return nil
	})
	return active, err
}

func (r *TaskRepository) GetControlPlaneSIEMReceiverRoutingState(ctx context.Context, defaultReceiver string) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	defaultReceiver = strings.TrimSpace(defaultReceiver)
	if defaultReceiver == "" {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, fmt.Errorf("default SIEM receiver is empty")
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	seed := domain.ControlPlaneSIEMReceiverRoutingState{
		ID:             1,
		ActiveReceiver: defaultReceiver,
		Term:           1,
		PromotedAt:     now,
	}
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	var state domain.ControlPlaneSIEMReceiverRoutingState
	if err := r.db.WithContext(ctx).First(&state, 1).Error; err != nil {
		return state, err
	}
	if err := r.ensureControlPlaneSIEMTermHistory(ctx, state, "bootstrap-existing"); err != nil {
		return state, err
	}
	return state, nil
}

func (r *TaskRepository) FindControlPlaneSIEMReceiverRoutingState(ctx context.Context) (domain.ControlPlaneSIEMReceiverRoutingState, bool, error) {
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	return state, true, nil
}

func (r *TaskRepository) RestoreControlPlaneSIEMReceiverRoutingState(ctx context.Context, activeReceiver string, term uint64, source string, restoredAt time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	activeReceiver = strings.TrimSpace(activeReceiver)
	source = strings.TrimSpace(source)
	if activeReceiver == "" || term == 0 {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, fmt.Errorf("restored SIEM receiver routing state is invalid")
	}
	seed := domain.ControlPlaneSIEMReceiverRoutingState{
		ID:             1,
		ActiveReceiver: activeReceiver,
		Term:           term,
		PromotedBy:     source,
		PromotedAt:     restoredAt.UTC().Truncate(time.Millisecond),
	}
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	var state domain.ControlPlaneSIEMReceiverRoutingState
	if err := r.db.WithContext(ctx).First(&state, 1).Error; err != nil {
		return state, err
	}
	if state.ActiveReceiver != activeReceiver || state.Term != term {
		return state, fmt.Errorf("existing SIEM receiver routing state conflicts with signed checkpoint recovery: receiver=%s term=%d", state.ActiveReceiver, state.Term)
	}
	if err := r.ensureControlPlaneSIEMTermHistory(ctx, state, "receiver-checkpoint-dr"); err != nil {
		return state, err
	}
	return state, nil
}

func (r *TaskRepository) PromoteControlPlaneSIEMReceiver(ctx context.Context, target, promotedBy string, expectedTerm uint64, promotedAt time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	target = strings.TrimSpace(target)
	promotedBy = strings.TrimSpace(promotedBy)
	if target == "" || promotedBy == "" {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, fmt.Errorf("SIEM receiver promotion target and operator are required")
	}
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seed := domain.ControlPlaneSIEMReceiverRoutingState{
			ID:             1,
			ActiveReceiver: target,
			Term:           1,
			PromotedBy:     promotedBy,
			PromotedAt:     promotedAt.UTC().Truncate(time.Millisecond),
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if expectedTerm > 0 && state.Term != expectedTerm {
			return fmt.Errorf("SIEM receiver term changed: got=%d expected=%d", state.Term, expectedTerm)
		}
		if state.ActiveReceiver == target {
			return nil
		}
		previousReceiver := state.ActiveReceiver
		previousTerm := state.Term
		state.ActiveReceiver = target
		state.Term++
		state.PromotedBy = promotedBy
		state.PromotedAt = promotedAt.UTC().Truncate(time.Millisecond)
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		entry := domain.ControlPlaneSIEMReceiverTermHistory{
			Term:             state.Term,
			PreviousTerm:     previousTerm,
			PreviousReceiver: previousReceiver,
			ActiveReceiver:   state.ActiveReceiver,
			Action:           "direct-promotion",
			RequestedBy:      promotedBy,
			CreatedAt:        state.PromotedAt,
		}
		return appendControlPlaneSIEMTermHistoryTx(tx, &entry)
	})
	return state, err
}

func (r *TaskRepository) BackfillControlPlaneAuditDeliveryReceipts(ctx context.Context, batch int) (int, error) {
	if batch <= 0 || batch > 5000 {
		batch = 1000
	}
	created := 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var deliveries []domain.ControlPlaneAuditDelivery
		if err := tx.
			Where("state = ? AND delivered_at IS NOT NULL AND NOT EXISTS (?)", domain.ControlPlaneDeliveryDelivered, tx.
				Model(&domain.ControlPlaneAuditDeliveryReceipt{}).
				Select("1").
				Where("control_plane_audit_delivery_receipts.chain_sequence = control_plane_audit_deliveries.chain_sequence")).
			Order("chain_sequence ASC").
			Limit(batch).
			Find(&deliveries).Error; err != nil {
			return err
		}
		for _, delivery := range deliveries {
			if delivery.DeliveredAt == nil {
				continue
			}
			receipt := domain.ControlPlaneAuditDeliveryReceipt{
				DeliveryID:    delivery.ID,
				AuditID:       delivery.AuditID,
				ChainSequence: delivery.ChainSequence,
				RecordHash:    delivery.RecordHash,
				AckStatus:     "legacy",
				DeliveredAt:   delivery.DeliveredAt.UTC().Truncate(time.Millisecond),
			}
			result := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "chain_sequence"}},
				DoNothing: true,
			}).Create(&receipt)
			if result.Error != nil {
				return result.Error
			}
			created += int(result.RowsAffected)
		}
		return nil
	})
	return created, err
}

func (r *TaskRepository) ClaimControlPlaneAuditDeliveries(
	ctx context.Context,
	now, leaseUntil time.Time,
	leaseToken string,
	leaderFence uint64,
	limit int,
) ([]domain.ControlPlaneAuditDelivery, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var deliveries []domain.ControlPlaneAuditDelivery
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ensureControlPlaneSIEMFence(tx, leaderFence); err != nil {
			return err
		}
		res := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("((state = ? AND next_attempt_at <= ?) OR (state = ? AND lease_until < ?))",
				domain.ControlPlaneDeliveryPending, now,
				domain.ControlPlaneDeliveryDelivering, now).
			Order("chain_sequence ASC").
			Limit(limit).
			Find(&deliveries)
		if res.Error != nil {
			return res.Error
		}
		if len(deliveries) == 0 {
			return nil
		}
		ids := make([]uint64, 0, len(deliveries))
		for i := range deliveries {
			ids = append(ids, deliveries[i].ID)
			deliveries[i].State = domain.ControlPlaneDeliveryDelivering
			deliveries[i].Attempts++
			deliveries[i].LeaseToken = leaseToken
			deliveries[i].LeaseUntil = &leaseUntil
			deliveries[i].LeaderFence = leaderFence
		}
		result := tx.Model(&domain.ControlPlaneAuditDelivery{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"state":        domain.ControlPlaneDeliveryDelivering,
				"attempts":     gorm.Expr("attempts + 1"),
				"lease_token":  leaseToken,
				"lease_until":  leaseUntil,
				"leader_fence": leaderFence,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(ids)) {
			return fmt.Errorf("claim control-plane deliveries rows=%d expected=%d", result.RowsAffected, len(ids))
		}
		return nil
	})
	return deliveries, err
}

func (r *TaskRepository) MarkControlPlaneAuditDeliveriesDelivered(
	ctx context.Context,
	ids []uint64,
	leaseToken string,
	leaderFence uint64,
	deliveredAt time.Time,
	ack domain.ControlPlaneSIEMAcknowledgement,
) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ensureControlPlaneSIEMFence(tx, leaderFence); err != nil {
			return err
		}
		deliveredAt = deliveredAt.UTC().Truncate(time.Millisecond)
		query := tx.Model(&domain.ControlPlaneAuditDelivery{}).
			Where("id IN ? AND state = ? AND lease_token = ?", ids, domain.ControlPlaneDeliveryDelivering, leaseToken)
		if leaderFence > 0 {
			query = query.Where("leader_fence = ?", leaderFence)
		}
		result := query.Updates(map[string]any{
			"state":           domain.ControlPlaneDeliveryDelivered,
			"lease_token":     "",
			"lease_until":     nil,
			"last_error":      "",
			"delivered_at":    deliveredAt,
			"next_attempt_at": deliveredAt,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(ids)) {
			return fmt.Errorf("settle control-plane deliveries rows=%d expected=%d", result.RowsAffected, len(ids))
		}
		var deliveries []domain.ControlPlaneAuditDelivery
		if err := tx.Where("id IN ?", ids).Find(&deliveries).Error; err != nil {
			return err
		}
		if len(deliveries) != len(ids) {
			return fmt.Errorf("load settled control-plane deliveries rows=%d expected=%d", len(deliveries), len(ids))
		}
		for _, delivery := range deliveries {
			receipt := domain.ControlPlaneAuditDeliveryReceipt{
				DeliveryID:     delivery.ID,
				AuditID:        delivery.AuditID,
				ChainSequence:  delivery.ChainSequence,
				RecordHash:     delivery.RecordHash,
				ReceiptID:      ack.ReceiptID,
				IdempotencyKey: ack.IdempotencyKey,
				PayloadSHA256:  ack.PayloadSHA256,
				AckHash:        ack.AckHash,
				AckStatus:      ack.Status,
				ReceiverID:     ack.ReceiverID,
				ReceiverGen:    ack.ReceiverGen,
				ReceiverTerm:   ack.ReceiverTerm,
				ReplicaCopies:  ack.ReplicaCopies,
				DeliveredAt:    deliveredAt,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "chain_sequence"}},
				DoNothing: true,
			}).Create(&receipt).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *TaskRepository) FailControlPlaneAuditDeliveries(
	ctx context.Context,
	deliveries []domain.ControlPlaneAuditDelivery,
	leaseToken string,
	leaderFence uint64,
	now, retryAt time.Time,
	cause error,
	maxAttempts int,
) (retried int, dead int, err error) {
	if len(deliveries) == 0 {
		return 0, 0, nil
	}
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	message := boundedDeliveryError(cause)
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ensureControlPlaneSIEMFence(tx, leaderFence); err != nil {
			return err
		}
		for _, delivery := range deliveries {
			if delivery.Attempts >= maxAttempts {
				query := tx.Model(&domain.ControlPlaneAuditDelivery{}).
					Where("id = ? AND state = ? AND lease_token = ?", delivery.ID, domain.ControlPlaneDeliveryDelivering, leaseToken)
				if leaderFence > 0 {
					query = query.Where("leader_fence = ?", leaderFence)
				}
				result := query.
					Updates(map[string]any{
						"state":       domain.ControlPlaneDeliveryDead,
						"lease_token": "",
						"lease_until": nil,
						"last_error":  message,
					})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return fmt.Errorf("dead-letter control-plane delivery %d rows=%d", delivery.ID, result.RowsAffected)
				}
				dlq := domain.ControlPlaneAuditDeliveryDLQ{
					DeliveryID:    delivery.ID,
					AuditID:       delivery.AuditID,
					ChainSequence: delivery.ChainSequence,
					RecordHash:    delivery.RecordHash,
					PayloadJSON:   delivery.PayloadJSON,
					State:         domain.ControlPlaneDeliveryDLQDead,
					Attempts:      delivery.Attempts,
					LastError:     message,
					FailedAt:      now.UTC().Truncate(time.Millisecond),
				}
				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "delivery_id"}},
					DoUpdates: clause.Assignments(map[string]any{
						"state":         domain.ControlPlaneDeliveryDLQDead,
						"failure_count": gorm.Expr("failure_count + 1"),
						"attempts":      delivery.Attempts,
						"last_error":    message,
						"failed_at":     now.UTC().Truncate(time.Millisecond),
						"replayed_at":   nil,
					}),
				}).Create(&dlq).Error; err != nil {
					return err
				}
				dead++
				continue
			}

			query := tx.Model(&domain.ControlPlaneAuditDelivery{}).
				Where("id = ? AND state = ? AND lease_token = ?", delivery.ID, domain.ControlPlaneDeliveryDelivering, leaseToken)
			if leaderFence > 0 {
				query = query.Where("leader_fence = ?", leaderFence)
			}
			result := query.
				Updates(map[string]any{
					"state":           domain.ControlPlaneDeliveryPending,
					"lease_token":     "",
					"lease_until":     nil,
					"last_error":      message,
					"next_attempt_at": retryAt.UTC().Truncate(time.Millisecond),
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("retry control-plane delivery %d rows=%d", delivery.ID, result.RowsAffected)
			}
			retried++
		}
		return nil
	})
	return retried, dead, err
}

func (r *TaskRepository) ListControlPlaneAuditDeliveries(ctx context.Context, state string, limit int) ([]domain.ControlPlaneAuditDelivery, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := r.db.WithContext(ctx).Order("chain_sequence DESC").Limit(limit)
	state = strings.TrimSpace(state)
	if state != "" {
		query = query.Where("state = ?", state)
	}
	var deliveries []domain.ControlPlaneAuditDelivery
	err := query.Find(&deliveries).Error
	return deliveries, err
}

func (r *TaskRepository) ListControlPlaneAuditDeliveryDLQ(ctx context.Context, limit int) ([]domain.ControlPlaneAuditDeliveryDLQ, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []domain.ControlPlaneAuditDeliveryDLQ
	err := r.db.WithContext(ctx).
		Where("state = ?", domain.ControlPlaneDeliveryDLQDead).
		Order("failed_at DESC, id DESC").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (r *TaskRepository) ListControlPlaneAuditDeliveryReceipts(ctx context.Context, afterSequence uint64, limit int) ([]domain.ControlPlaneAuditDeliveryReceipt, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []domain.ControlPlaneAuditDeliveryReceipt
	err := r.db.WithContext(ctx).
		Where("chain_sequence > ?", afterSequence).
		Order("chain_sequence ASC").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (r *TaskRepository) GetControlPlaneAuditBySequence(ctx context.Context, sequence uint64) (domain.ControlPlaneAudit, error) {
	var audit domain.ControlPlaneAudit
	err := r.db.WithContext(ctx).
		Where("chain_sequence = ?", sequence).
		First(&audit).Error
	return audit, err
}

func (r *TaskRepository) ImportControlPlaneAuditDeliveryReceipt(ctx context.Context, receipt domain.ControlPlaneAuditDeliveryReceipt) (bool, error) {
	if receipt.ChainSequence == 0 || strings.TrimSpace(receipt.RecordHash) == "" || strings.TrimSpace(receipt.ReceiptID) == "" {
		return false, fmt.Errorf("invalid reconciled control-plane SIEM receipt")
	}
	created := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var audit domain.ControlPlaneAudit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain_sequence = ?", receipt.ChainSequence).
			First(&audit).Error; err != nil {
			return err
		}
		if audit.RecordHash != receipt.RecordHash {
			return fmt.Errorf("reconciled SIEM receipt sequence=%d record hash mismatch", receipt.ChainSequence)
		}
		if receipt.AuditID != 0 && receipt.AuditID != audit.ID {
			return fmt.Errorf("reconciled SIEM receipt sequence=%d audit id mismatch", receipt.ChainSequence)
		}
		receipt.AuditID = audit.ID

		var existing domain.ControlPlaneAuditDeliveryReceipt
		err := tx.Where("chain_sequence = ?", receipt.ChainSequence).First(&existing).Error
		if err == nil {
			if existing.RecordHash == receipt.RecordHash && existing.ReceiptID == "" && existing.IdempotencyKey == "" && existing.PayloadSHA256 == "" {
				updates := map[string]any{
					"receipt_id":      receipt.ReceiptID,
					"idempotency_key": receipt.IdempotencyKey,
					"payload_sha256":  receipt.PayloadSHA256,
					"ack_hash":        receipt.AckHash,
					"ack_status":      receipt.AckStatus,
					"receiver_id":     receipt.ReceiverID,
					"receiver_gen":    receipt.ReceiverGen,
					"receiver_term":   receipt.ReceiverTerm,
					"replica_copies":  receipt.ReplicaCopies,
					"recovery_source": receipt.RecoverySource,
				}
				if err := tx.Model(&domain.ControlPlaneAuditDeliveryReceipt{}).Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
					return err
				}
				created = true
				return nil
			}
			if existing.RecordHash != receipt.RecordHash ||
				existing.ReceiptID != receipt.ReceiptID ||
				existing.IdempotencyKey != receipt.IdempotencyKey ||
				existing.PayloadSHA256 != receipt.PayloadSHA256 {
				return fmt.Errorf("reconciled SIEM receipt sequence=%d conflicts with local receipt", receipt.ChainSequence)
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var delivery domain.ControlPlaneAuditDelivery
		if err := tx.Where("chain_sequence = ?", receipt.ChainSequence).First(&delivery).Error; err == nil {
			receipt.DeliveryID = delivery.ID
			if err := tx.Where("delivery_id = ?", delivery.ID).Delete(&domain.ControlPlaneAuditDeliveryDLQ{}).Error; err != nil {
				return err
			}
			if err := tx.Delete(&delivery).Error; err != nil {
				return err
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if receipt.DeliveredAt.IsZero() {
			receipt.DeliveredAt = time.Now().UTC().Truncate(time.Millisecond)
		} else {
			receipt.DeliveredAt = receipt.DeliveredAt.UTC().Truncate(time.Millisecond)
		}
		if err := tx.Create(&receipt).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, err
}

func (r *TaskRepository) ReplayControlPlaneAuditDeliveryDLQ(ctx context.Context, id uint64, now time.Time) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var dlq domain.ControlPlaneAuditDeliveryDLQ
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&dlq, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrControlPlaneDeliveryDLQNotFound
			}
			return err
		}
		if dlq.State != domain.ControlPlaneDeliveryDLQDead {
			return fmt.Errorf("control-plane delivery DLQ row %d is not replayable", id)
		}
		result := tx.Model(&domain.ControlPlaneAuditDelivery{}).
			Where("id = ? AND state = ?", dlq.DeliveryID, domain.ControlPlaneDeliveryDead).
			Updates(map[string]any{
				"state":           domain.ControlPlaneDeliveryPending,
				"attempts":        0,
				"next_attempt_at": now.UTC().Truncate(time.Millisecond),
				"lease_token":     "",
				"lease_until":     nil,
				"last_error":      "",
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("replay control-plane delivery %d rows=%d", dlq.DeliveryID, result.RowsAffected)
		}
		replayedAt := now.UTC().Truncate(time.Millisecond)
		return tx.Model(&domain.ControlPlaneAuditDeliveryDLQ{}).
			Where("id = ? AND state = ?", id, domain.ControlPlaneDeliveryDLQDead).
			Updates(map[string]any{
				"state":       domain.ControlPlaneDeliveryDLQReplayed,
				"replayed_at": replayedAt,
			}).Error
	})
}

func (r *TaskRepository) ControlPlaneAuditDeliveryStats(ctx context.Context) (domain.ControlPlaneAuditDeliveryStats, error) {
	var stats domain.ControlPlaneAuditDeliveryStats
	states := []struct {
		state string
		dst   *int64
	}{
		{domain.ControlPlaneDeliveryPending, &stats.Pending},
		{domain.ControlPlaneDeliveryDelivering, &stats.Delivering},
		{domain.ControlPlaneDeliveryDelivered, &stats.Delivered},
		{domain.ControlPlaneDeliveryDead, &stats.Dead},
	}
	for _, item := range states {
		if err := r.db.WithContext(ctx).Model(&domain.ControlPlaneAuditDelivery{}).Where("state = ?", item.state).Count(item.dst).Error; err != nil {
			return stats, err
		}
	}
	if err := r.db.WithContext(ctx).Model(&domain.ControlPlaneAuditDeliveryDLQ{}).
		Where("state = ?", domain.ControlPlaneDeliveryDLQDead).
		Count(&stats.DLQ).Error; err != nil {
		return stats, err
	}
	return stats, nil
}

func (r *TaskRepository) PurgeControlPlaneSIEMDeliveryHistory(
	ctx context.Context,
	deliveredBefore, replayedDLQBefore time.Time,
	batch int,
) (delivered int, replayedDLQ int, err error) {
	if batch <= 0 || batch > 5000 {
		batch = 1000
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var deliveryIDs []uint64
		if err := tx.Model(&domain.ControlPlaneAuditDelivery{}).
			Where("state = ? AND delivered_at IS NOT NULL AND delivered_at < ?", domain.ControlPlaneDeliveryDelivered, deliveredBefore).
			Order("delivered_at ASC, id ASC").
			Limit(batch).
			Pluck("id", &deliveryIDs).Error; err != nil {
			return err
		}
		if len(deliveryIDs) > 0 {
			result := tx.Where("id IN ? AND state = ?", deliveryIDs, domain.ControlPlaneDeliveryDelivered).
				Delete(&domain.ControlPlaneAuditDelivery{})
			if result.Error != nil {
				return result.Error
			}
			delivered = int(result.RowsAffected)
		}

		var dlqIDs []uint64
		if err := tx.Model(&domain.ControlPlaneAuditDeliveryDLQ{}).
			Where("state = ? AND replayed_at IS NOT NULL AND replayed_at < ?", domain.ControlPlaneDeliveryDLQReplayed, replayedDLQBefore).
			Order("replayed_at ASC, id ASC").
			Limit(batch).
			Pluck("id", &dlqIDs).Error; err != nil {
			return err
		}
		if len(dlqIDs) > 0 {
			result := tx.Where("id IN ? AND state = ?", dlqIDs, domain.ControlPlaneDeliveryDLQReplayed).
				Delete(&domain.ControlPlaneAuditDeliveryDLQ{})
			if result.Error != nil {
				return result.Error
			}
			replayedDLQ = int(result.RowsAffected)
		}
		return nil
	})
	return delivered, replayedDLQ, err
}

func ensureControlPlaneSIEMFence(tx *gorm.DB, fence uint64) error {
	if fence == 0 {
		return nil
	}
	var state domain.ControlPlaneSIEMDeliveryCoordinatorState
	err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrControlPlaneSIEMStaleFence
	}
	if err != nil {
		return err
	}
	if state.ActiveFence != fence {
		return ErrControlPlaneSIEMStaleFence
	}
	return nil
}

func boundedDeliveryError(cause error) string {
	if cause == nil {
		return "delivery failed"
	}
	message := cause.Error()
	if len(message) > 1024 {
		message = message[:1024]
	}
	return message
}
