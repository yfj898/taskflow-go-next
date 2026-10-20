package repository

import (
	"context"
	"errors"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type EnqueueClaimState string

const (
	EnqueueClaimAcquired       EnqueueClaimState = "acquired"
	EnqueueClaimAlreadyVisible EnqueueClaimState = "already_visible"
	EnqueueClaimContended      EnqueueClaimState = "contended"
	EnqueueClaimNotReady       EnqueueClaimState = "not_ready"
	EnqueueClaimObsolete       EnqueueClaimState = "obsolete"
)

type EnqueueClaim struct {
	TaskID     uint64
	Generation uint64
}

// ClaimTaskEnqueue serializes all publishers for one task generation. A
// positive expectedGeneration pins callers such as the task-created outbox to
// that generation; zero means "the task's current generation" and is used by
// the reconciler.
func (r *TaskRepository) ClaimTaskEnqueue(
	ctx context.Context,
	taskID uint64,
	expectedGeneration uint64,
	now time.Time,
	leaseUntil time.Time,
) (EnqueueClaim, EnqueueClaimState, error) {
	var task domain.Task
	state := EnqueueClaimNotReady
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "status", "next_run_at", "enqueue_generation", "enqueued_generation", "enqueue_lease_until").
			First(&task, taskID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				state = EnqueueClaimObsolete
				return nil
			}
			return err
		}

		if expectedGeneration > 0 && task.EnqueueGeneration != expectedGeneration {
			state = EnqueueClaimObsolete
			return nil
		}
		if task.EnqueuedGeneration >= task.EnqueueGeneration {
			state = EnqueueClaimAlreadyVisible
			return nil
		}
		// A non-pending task at the same generation is itself durable evidence
		// that this generation reached a worker through the broker. This closes
		// the publish -> worker-claim -> publisher-crash window: an outbox row
		// must converge to published instead of retrying forever merely because
		// the worker ran before broker visibility was confirmed in MySQL.
		if task.Status != domain.TaskPending {
			state = EnqueueClaimAlreadyVisible
			return nil
		}
		if task.NextRunAt.After(now) {
			state = EnqueueClaimNotReady
			return nil
		}
		if task.EnqueueLeaseUntil != nil && task.EnqueueLeaseUntil.After(now) {
			state = EnqueueClaimContended
			return nil
		}
		if err := tx.Model(&domain.Task{}).
			Where("id = ? AND enqueue_generation = ?", task.ID, task.EnqueueGeneration).
			Update("enqueue_lease_until", leaseUntil).Error; err != nil {
			return err
		}
		state = EnqueueClaimAcquired
		return nil
	})
	return EnqueueClaim{TaskID: task.ID, Generation: task.EnqueueGeneration}, state, err
}

// ConfirmTaskEnqueued marks one generation as broker-visible. It intentionally
// does not require status=pending: a fast worker may claim the message before
// the publisher finishes this confirmation.
func (r *TaskRepository) ConfirmTaskEnqueued(ctx context.Context, claim EnqueueClaim) error {
	return r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("id = ? AND enqueue_generation = ?", claim.TaskID, claim.Generation).
		Updates(map[string]any{
			"enqueued_generation": claim.Generation,
			"enqueue_lease_until": nil,
		}).Error
}

func (r *TaskRepository) ReleaseTaskEnqueue(ctx context.Context, claim EnqueueClaim) error {
	return r.db.WithContext(ctx).Model(&domain.Task{}).
		Where("id = ? AND enqueue_generation = ? AND enqueued_generation < ?", claim.TaskID, claim.Generation, claim.Generation).
		Update("enqueue_lease_until", nil).Error
}

func (r *TaskRepository) ConfirmTaskEnqueuedAndOutboxBatch(
	ctx context.Context,
	claims []EnqueueClaim,
	outboxIDs []uint64,
	publishedAt time.Time,
) error {
	if len(claims) == 0 && len(outboxIDs) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, claim := range claims {
			if err := tx.Model(&domain.Task{}).
				Where("id = ? AND enqueue_generation = ?", claim.TaskID, claim.Generation).
				Updates(map[string]any{
					"enqueued_generation": claim.Generation,
					"enqueue_lease_until": nil,
				}).Error; err != nil {
				return err
			}
		}
		if len(outboxIDs) == 0 {
			return nil
		}
		return tx.Model(&domain.OutboxEvent{}).
			Where("id IN ? AND published_at IS NULL", outboxIDs).
			Updates(map[string]any{
				"attempts":     gorm.Expr("attempts + 1"),
				"last_error":   "",
				"lease_until":  nil,
				"published_at": publishedAt,
			}).Error
	})
}

func (r *TaskRepository) ReleaseOutboxBatch(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&domain.OutboxEvent{}).
		Where("id IN ? AND published_at IS NULL", ids).
		Update("lease_until", nil).Error
}
