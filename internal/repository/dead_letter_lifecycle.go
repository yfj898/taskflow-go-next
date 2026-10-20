package repository

import (
	"context"
	"errors"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DeadLetterLifecycleStat struct {
	FailureClass string
	ReplayState  domain.DeadLetterState
	Count        int64
	OldestAt     time.Time
}

type DeadLetterLifecycleResult struct {
	RetentionPurged int64
	CapacityPurged  int64
	AuditPurged     int64
	Remaining       int64
	OverLimit       int64
}

func (r *TaskRepository) CreateDeadLetterReplayAudit(ctx context.Context, audit *domain.DeadLetterReplayAudit) error {
	return r.db.WithContext(ctx).Create(audit).Error
}

func (r *TaskRepository) ListDeadLetterReplayAudits(ctx context.Context, deadLetterID uint64, limit, offset int) ([]domain.DeadLetterReplayAudit, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var audits []domain.DeadLetterReplayAudit
	err := r.db.WithContext(ctx).
		Where("dead_letter_id = ?", deadLetterID).
		Order("id DESC").Limit(limit).Offset(offset).Find(&audits).Error
	return audits, err
}

// PurgeDeadLetter deliberately refuses every replaying record, even when its
// lease has expired. Replay recovery owns that state transition; deletion must
// never race a broker publish whose database settlement may still be pending.
func (r *TaskRepository) PurgeDeadLetter(ctx context.Context, id uint64, minCopies int) error {
	if minCopies <= 0 {
		minCopies = 1
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var message domain.DeadLetterMessage
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&message, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrDeadLetterNotFound
			}
			return err
		}
		if message.ReplayState == domain.DeadLetterReplaying {
			return ErrDeadLetterReplayActive
		}
		var archiveCount int64
		if err := tx.Model(&domain.DeadLetterArchive{}).
			Where("dead_letter_id = ? AND state = ? AND verified_at IS NOT NULL AND verified_copies >= ?", id, domain.DeadLetterArchiveReady, minCopies).
			Count(&archiveCount).Error; err != nil {
			return err
		}
		if archiveCount != 1 {
			return ErrDeadLetterArchiveRequired
		}
		return tx.Delete(&domain.DeadLetterMessage{}, id).Error
	})
}

func (r *TaskRepository) DeadLetterLifecycleStats(ctx context.Context) ([]DeadLetterLifecycleStat, error) {
	type row struct {
		FailureClass string
		ReplayState  domain.DeadLetterState
		Count        int64
		OldestAt     *time.Time
	}
	var rows []row
	if err := r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).
		Select("failure_class, replay_state, COUNT(*) AS count, MIN(created_at) AS oldest_at").
		Group("failure_class, replay_state").Scan(&rows).Error; err != nil {
		return nil, err
	}
	stats := make([]DeadLetterLifecycleStat, 0, len(rows))
	for _, item := range rows {
		stat := DeadLetterLifecycleStat{FailureClass: item.FailureClass, ReplayState: item.ReplayState, Count: item.Count}
		if item.OldestAt != nil {
			stat.OldestAt = *item.OldestAt
		}
		stats = append(stats, stat)
	}
	return stats, nil
}

// RunDeadLetterLifecycle applies age retention before the hard capacity target.
// Age retention may remove quarantined/replayed rows according to their
// independently configured windows. The count cap is deliberately stricter:
// it evicts only replayed terminal evidence. Fresh unresolved quarantine and
// replaying rows can therefore keep the table above maxEntries; OverLimit
// exposes that safety condition instead of silently discarding evidence.
func (r *TaskRepository) RunDeadLetterLifecycle(
	ctx context.Context,
	replayedBefore, quarantinedBefore, auditBefore time.Time,
	archiveVerifiedAfter time.Time,
	minArchiveCopies, maxEntries, batch int,
) (DeadLetterLifecycleResult, error) {
	if minArchiveCopies <= 0 {
		minArchiveCopies = 1
	}
	if batch <= 0 {
		batch = 500
	}
	result := DeadLetterLifecycleResult{}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		remainingBatch := batch
		var retentionIDs []uint64
		if !replayedBefore.IsZero() || !quarantinedBefore.IsZero() {
			query := tx.Model(&domain.DeadLetterMessage{}).
				Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
			query = query.Where("EXISTS (SELECT 1 FROM queue_dead_letter_archives a WHERE a.dead_letter_id = queue_dead_letters.id AND a.state = ? AND a.verified_at >= ? AND a.verified_copies >= ?)", domain.DeadLetterArchiveReady, archiveVerifiedAfter, minArchiveCopies)
			switch {
			case !replayedBefore.IsZero() && !quarantinedBefore.IsZero():
				query = query.Where("(replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?) OR (replay_state = ? AND created_at < ?)",
					domain.DeadLetterReplayed, replayedBefore, domain.DeadLetterQuarantined, quarantinedBefore)
			case !replayedBefore.IsZero():
				query = query.Where("replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?", domain.DeadLetterReplayed, replayedBefore)
			default:
				query = query.Where("replay_state = ? AND created_at < ?", domain.DeadLetterQuarantined, quarantinedBefore)
			}
			if err := query.Order("created_at ASC").Limit(remainingBatch).Pluck("id", &retentionIDs).Error; err != nil {
				return err
			}
		}
		if len(retentionIDs) > 0 {
			deleteQuery := tx.Where("id IN ?", retentionIDs).
				Where("EXISTS (SELECT 1 FROM queue_dead_letter_archives a WHERE a.dead_letter_id = queue_dead_letters.id AND a.state = ? AND a.verified_at >= ? AND a.verified_copies >= ?)", domain.DeadLetterArchiveReady, archiveVerifiedAfter, minArchiveCopies)
			switch {
			case !replayedBefore.IsZero() && !quarantinedBefore.IsZero():
				deleteQuery = deleteQuery.Where("(replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?) OR (replay_state = ? AND created_at < ?)",
					domain.DeadLetterReplayed, replayedBefore, domain.DeadLetterQuarantined, quarantinedBefore)
			case !replayedBefore.IsZero():
				deleteQuery = deleteQuery.Where("replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?", domain.DeadLetterReplayed, replayedBefore)
			default:
				deleteQuery = deleteQuery.Where("replay_state = ? AND created_at < ?", domain.DeadLetterQuarantined, quarantinedBefore)
			}
			res := deleteQuery.Delete(&domain.DeadLetterMessage{})
			if res.Error != nil {
				return res.Error
			}
			result.RetentionPurged = res.RowsAffected
			remainingBatch -= len(retentionIDs)
		}

		var total int64
		if err := tx.Model(&domain.DeadLetterMessage{}).Count(&total).Error; err != nil {
			return err
		}
		if maxEntries > 0 && total > int64(maxEntries) && remainingBatch > 0 {
			need := total - int64(maxEntries)
			limit := remainingBatch
			if need < int64(limit) {
				limit = int(need)
			}
			var capacityIDs []uint64
			if err := tx.Model(&domain.DeadLetterMessage{}).
				Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Where("EXISTS (SELECT 1 FROM queue_dead_letter_archives a WHERE a.dead_letter_id = queue_dead_letters.id AND a.state = ? AND a.verified_at >= ? AND a.verified_copies >= ?)", domain.DeadLetterArchiveReady, archiveVerifiedAfter, minArchiveCopies).
				Where("replay_state = ?", domain.DeadLetterReplayed).
				Order("created_at ASC").Limit(limit).Pluck("id", &capacityIDs).Error; err != nil {
				return err
			}
			if len(capacityIDs) > 0 {
				res := tx.Where("id IN ? AND replay_state = ?", capacityIDs, domain.DeadLetterReplayed).
					Where("EXISTS (SELECT 1 FROM queue_dead_letter_archives a WHERE a.dead_letter_id = queue_dead_letters.id AND a.state = ? AND a.verified_at >= ? AND a.verified_copies >= ?)", domain.DeadLetterArchiveReady, archiveVerifiedAfter, minArchiveCopies).
					Delete(&domain.DeadLetterMessage{})
				if res.Error != nil {
					return res.Error
				}
				result.CapacityPurged = res.RowsAffected
				total -= res.RowsAffected
			}
		}
		result.Remaining = total
		if maxEntries > 0 && total > int64(maxEntries) {
			result.OverLimit = total - int64(maxEntries)
		}

		if !auditBefore.IsZero() {
			var auditIDs []uint64
			if err := tx.Model(&domain.DeadLetterReplayAudit{}).
				Where("created_at < ?", auditBefore).Order("created_at ASC").Limit(batch).Pluck("id", &auditIDs).Error; err != nil {
				return err
			}
			if len(auditIDs) > 0 {
				res := tx.Where("id IN ?", auditIDs).Delete(&domain.DeadLetterReplayAudit{})
				if res.Error != nil {
					return res.Error
				}
				result.AuditPurged = res.RowsAffected
			}
		}
		return nil
	})
	return result, err
}
