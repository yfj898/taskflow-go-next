package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDeadLetterNotFound = errors.New("dead letter not found")
var ErrDeadLetterReplayActive = errors.New("dead letter replay is active")

func (r *TaskRepository) QuarantineDeadLetter(ctx context.Context, message *domain.DeadLetterMessage) error {
	if message.ReplayState == "" {
		message.ReplayState = domain.DeadLetterQuarantined
	}
	res := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_backend"}, {Name: "source_queue"}, {Name: "source_delivery_id"}},
		DoNothing: true,
	}).Create(message)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}
	return r.db.WithContext(ctx).
		Where("source_backend = ? AND source_queue = ? AND source_delivery_id = ?", message.SourceBackend, message.SourceQueue, message.SourceDeliveryID).
		First(message).Error
}

func (r *TaskRepository) GetDeadLetter(ctx context.Context, id uint64) (*domain.DeadLetterMessage, error) {
	var message domain.DeadLetterMessage
	if err := r.db.WithContext(ctx).First(&message, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDeadLetterNotFound
		}
		return nil, err
	}
	return &message, nil
}

func (r *TaskRepository) ListDeadLetters(ctx context.Context, limit, offset int) ([]domain.DeadLetterMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var messages []domain.DeadLetterMessage
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Offset(offset).Find(&messages).Error
	return messages, err
}

func (r *TaskRepository) ClaimDeadLetterReplay(ctx context.Context, id uint64, now, leaseUntil time.Time) (bool, error) {
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).
		Where("id = ? AND replayed_at IS NULL AND (replay_state = ? OR (replay_state = ? AND replay_lease_until < ?))",
			id, domain.DeadLetterQuarantined, domain.DeadLetterReplaying, now).
		Updates(map[string]any{
			"replay_state":       domain.DeadLetterReplaying,
			"replay_lease_until": leaseUntil,
			"replay_count":       gorm.Expr("replay_count + 1"),
			"last_replay_error":  "",
		})
	return res.RowsAffected == 1, res.Error
}

func (r *TaskRepository) MarkDeadLetterReplayFailed(ctx context.Context, id uint64, cause error) error {
	return r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).
		Where("id = ? AND replay_state = ? AND replayed_at IS NULL", id, domain.DeadLetterReplaying).
		Updates(map[string]any{
			"replay_state":       domain.DeadLetterQuarantined,
			"replay_lease_until": nil,
			"last_replay_error":  cause.Error(),
		}).Error
}

func (r *TaskRepository) MarkDeadLetterReplayed(ctx context.Context, id uint64, replayedAt time.Time) error {
	return r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).
		Where("id = ? AND replay_state = ? AND replayed_at IS NULL", id, domain.DeadLetterReplaying).
		Updates(map[string]any{
			"replay_state":       domain.DeadLetterReplayed,
			"replay_lease_until": nil,
			"last_replay_error":  "",
			"replayed_at":        replayedAt,
		}).Error
}

// ConfirmTaskEnqueuedAndDeadLetterReplayed settles the Phase 16 broker
// visibility marker and the Phase 18 replay record in one MySQL transaction.
// The broker publish necessarily happens before this transaction, so a crash
// in that narrow broker/DB window can still cause a later duplicate publish;
// the exact-generation task Claim remains the final execution fence.
func (r *TaskRepository) ConfirmTaskEnqueuedAndDeadLetterReplayed(
	ctx context.Context,
	claim EnqueueClaim,
	deadLetterID uint64,
	replayedAt time.Time,
	replayAudits ...*domain.DeadLetterReplayAudit,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&domain.Task{}).
			Where("id = ? AND enqueue_generation = ?", claim.TaskID, claim.Generation).
			Updates(map[string]any{
				"enqueued_generation": claim.Generation,
				"enqueue_lease_until": nil,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("task enqueue generation changed before DLQ replay settlement")
		}

		res = tx.Model(&domain.DeadLetterMessage{}).
			Where("id = ? AND replay_state = ? AND replayed_at IS NULL", deadLetterID, domain.DeadLetterReplaying).
			Updates(map[string]any{
				"replay_state":       domain.DeadLetterReplayed,
				"replay_lease_until": nil,
				"last_replay_error":  "",
				"replayed_at":        replayedAt,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("dead letter replay lease lost before settlement")
		}
		for _, audit := range replayAudits {
			if audit != nil {
				if err := tx.Create(audit).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}
