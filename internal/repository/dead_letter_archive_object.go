package repository

import (
	"context"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm/clause"
)

func (r *TaskRepository) UpsertDeadLetterArchiveReplica(ctx context.Context, replica *domain.DeadLetterArchiveReplica) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "archive_id"}, {Name: "sink"}},
		DoUpdates: clause.Assignments(map[string]any{
			"object_key":  replica.ObjectKey,
			"sha256":      replica.SHA256,
			"size_bytes":  replica.SizeBytes,
			"verified_at": replica.VerifiedAt,
			"updated_at":  time.Now(),
		}),
	}).Create(replica).Error
}

func (r *TaskRepository) ListDeadLetterArchiveReplicas(ctx context.Context, archiveID uint64) ([]domain.DeadLetterArchiveReplica, error) {
	var replicas []domain.DeadLetterArchiveReplica
	err := r.db.WithContext(ctx).Where("archive_id = ?", archiveID).Order("id ASC").Find(&replicas).Error
	return replicas, err
}

func (r *TaskRepository) MarkDeadLetterArchiveVerificationSummary(ctx context.Context, archiveID uint64, verifiedAt time.Time, copies int) error {
	if copies < 0 {
		copies = 0
	}
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND state = ?", archiveID, domain.DeadLetterArchiveReady).
		Updates(map[string]any{"verified_at": verifiedAt, "verified_copies": copies})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrDeadLetterArchiveNotFound
	}
	return nil
}

func (r *TaskRepository) ListDeadLetterArchiveObjectGCCandidates(ctx context.Context, before time.Time, limit int) ([]domain.DeadLetterArchive, error) {
	if limit <= 0 {
		limit = 50
	}
	var archives []domain.DeadLetterArchive
	err := r.db.WithContext(ctx).
		Where("state = ? AND archived_at IS NOT NULL AND archived_at < ? AND (last_restored_at IS NULL OR last_restored_at < ?) AND NOT EXISTS (SELECT 1 FROM queue_dead_letters d WHERE d.id = queue_dead_letter_archives.dead_letter_id)",
			domain.DeadLetterArchiveReady, before, before).
		Order("archived_at ASC, id ASC").Limit(limit).Find(&archives).Error
	return archives, err
}

func (r *TaskRepository) ClaimDeadLetterArchiveGC(ctx context.Context, archiveID uint64, before time.Time) error {
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND state = ? AND archived_at IS NOT NULL AND archived_at < ? AND (last_restored_at IS NULL OR last_restored_at < ?) AND NOT EXISTS (SELECT 1 FROM queue_dead_letters d WHERE d.id = queue_dead_letter_archives.dead_letter_id)",
			archiveID, domain.DeadLetterArchiveReady, before, before).
		Update("state", domain.DeadLetterArchiveExpiring)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrDeadLetterArchiveActive
	}
	return nil
}

func (r *TaskRepository) ReleaseDeadLetterArchiveGC(ctx context.Context, archiveID uint64, cause error) error {
	lastError := ""
	if cause != nil {
		lastError = cause.Error()
	}
	return r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND state = ?", archiveID, domain.DeadLetterArchiveExpiring).
		Updates(map[string]any{"state": domain.DeadLetterArchiveReady, "last_archive_error": lastError}).Error
}

func (r *TaskRepository) MarkDeadLetterArchiveExpired(ctx context.Context, archiveID uint64, expiredAt time.Time) error {
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND state = ?", archiveID, domain.DeadLetterArchiveExpiring).
		Updates(map[string]any{
			"state":              domain.DeadLetterArchiveExpired,
			"expired_at":         expiredAt,
			"verified_at":        nil,
			"verified_copies":    0,
			"last_archive_error": "",
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrDeadLetterArchiveActive
	}
	return nil
}

func (r *TaskRepository) ListDeadLetterArchiveDrillCandidates(ctx context.Context, before time.Time, limit int) ([]domain.DeadLetterArchive, error) {
	if limit <= 0 {
		limit = 3
	}
	var archives []domain.DeadLetterArchive
	err := r.db.WithContext(ctx).
		Where("state = ? AND (last_drill_at IS NULL OR last_drill_at < ?)", domain.DeadLetterArchiveReady, before).
		Order("COALESCE(last_drill_at, archived_at, created_at) ASC, id ASC").Limit(limit).Find(&archives).Error
	return archives, err
}

func (r *TaskRepository) MarkDeadLetterArchiveDrill(ctx context.Context, archiveID uint64, at time.Time, cause error) error {
	lastError := ""
	if cause != nil {
		lastError = cause.Error()
	}
	return r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ?", archiveID).
		Updates(map[string]any{"last_drill_at": at, "last_drill_error": lastError}).Error
}

func (r *TaskRepository) ListReferencedArchiveObjectKeys(ctx context.Context, sink string) ([]string, error) {
	keys := make([]string, 0)
	var primary []string
	if err := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("sink = ? AND state <> ? AND object_key <> ''", sink, domain.DeadLetterArchiveExpired).
		Pluck("object_key", &primary).Error; err != nil {
		return nil, err
	}
	keys = append(keys, primary...)

	var replicas []string
	if err := r.db.WithContext(ctx).Table("queue_dead_letter_archive_replicas r").
		Select("r.object_key").
		Joins("JOIN queue_dead_letter_archives a ON a.id = r.archive_id").
		Where("r.sink = ? AND a.state <> ?", sink, domain.DeadLetterArchiveExpired).
		Pluck("r.object_key", &replicas).Error; err != nil {
		return nil, err
	}
	keys = append(keys, replicas...)
	return keys, nil
}
