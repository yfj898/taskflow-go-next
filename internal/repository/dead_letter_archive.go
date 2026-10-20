package repository

import (
	"context"
	"errors"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrDeadLetterArchiveNotFound        = errors.New("dead letter archive not found")
	ErrDeadLetterArchiveActive          = errors.New("dead letter archive is active")
	ErrDeadLetterArchiveRequired        = errors.New("dead letter must be archived before purge")
	ErrDeadLetterArchiveLeaseLost       = errors.New("dead letter archive lease lost")
	ErrDeadLetterArchiveRestoreConflict = errors.New("dead letter restore conflicts with an existing source delivery")
)

type DeadLetterArchiveClaim struct {
	ArchiveID    uint64
	DeadLetterID uint64
	LeaseToken   string
	Ready        bool
	Message      domain.DeadLetterMessage
	ReplayAudits []domain.DeadLetterReplayAudit
	Archive      domain.DeadLetterArchive
}

func (r *TaskRepository) ClaimDeadLetterArchive(
	ctx context.Context,
	deadLetterID uint64,
	sink, leaseToken string,
	now, leaseUntil time.Time,
) (DeadLetterArchiveClaim, error) {
	claim := DeadLetterArchiveClaim{}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var message domain.DeadLetterMessage
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&message, deadLetterID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrDeadLetterNotFound
			}
			return err
		}
		if message.ReplayState == domain.DeadLetterReplaying {
			return ErrDeadLetterReplayActive
		}

		var archive domain.DeadLetterArchive
		err := tx.Where("dead_letter_id = ?", deadLetterID).First(&archive).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			archive = domain.DeadLetterArchive{
				DeadLetterID:      deadLetterID,
				State:             domain.DeadLetterArchiveArchiving,
				Sink:              sink,
				FormatVersion:     1,
				SourceBackend:     message.SourceBackend,
				SourceQueue:       message.SourceQueue,
				SourceDeliveryID:  message.SourceDeliveryID,
				FailureClass:      message.FailureClass,
				ArchiveAttempts:   1,
				ArchiveLeaseToken: leaseToken,
				ArchiveLeaseUntil: &leaseUntil,
			}
			if err := tx.Create(&archive).Error; err != nil {
				return err
			}
		case err != nil:
			return err
		case archive.State == domain.DeadLetterArchiveReady:
			claim = DeadLetterArchiveClaim{
				ArchiveID: deadLetterID,
				Ready:     true,
				Message:   message,
				Archive:   archive,
			}
			claim.ArchiveID = archive.ID
			claim.DeadLetterID = deadLetterID
			return nil
		case archive.State == domain.DeadLetterArchiveArchiving && archive.ArchiveLeaseUntil != nil && archive.ArchiveLeaseUntil.After(now):
			return ErrDeadLetterArchiveActive
		default:
			res := tx.Model(&domain.DeadLetterArchive{}).
				Where("id = ? AND state <> ?", archive.ID, domain.DeadLetterArchiveReady).
				Updates(map[string]any{
					"state":               domain.DeadLetterArchiveArchiving,
					"sink":                sink,
					"archive_attempts":    gorm.Expr("archive_attempts + 1"),
					"archive_lease_token": leaseToken,
					"archive_lease_until": leaseUntil,
					"last_archive_error":  "",
				})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != 1 {
				return ErrDeadLetterArchiveActive
			}
			archive.State = domain.DeadLetterArchiveArchiving
			archive.Sink = sink
			archive.ArchiveLeaseToken = leaseToken
			archive.ArchiveLeaseUntil = &leaseUntil
		}

		var audits []domain.DeadLetterReplayAudit
		if err := tx.Where("dead_letter_id = ?", deadLetterID).Order("id ASC").Find(&audits).Error; err != nil {
			return err
		}
		claim = DeadLetterArchiveClaim{
			ArchiveID:    archive.ID,
			DeadLetterID: deadLetterID,
			LeaseToken:   leaseToken,
			Message:      message,
			ReplayAudits: audits,
			Archive:      archive,
		}
		return nil
	})
	return claim, err
}

func (r *TaskRepository) MarkDeadLetterArchiveFailed(ctx context.Context, claim DeadLetterArchiveClaim, cause error) error {
	msg := "archive failed"
	if cause != nil {
		msg = cause.Error()
	}
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND dead_letter_id = ? AND state = ? AND archive_lease_token = ?",
			claim.ArchiveID, claim.DeadLetterID, domain.DeadLetterArchiveArchiving, claim.LeaseToken).
		Updates(map[string]any{
			"state":               domain.DeadLetterArchiveFailed,
			"archive_lease_token": "",
			"archive_lease_until": nil,
			"last_archive_error":  msg,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrDeadLetterArchiveLeaseLost
	}
	return nil
}

func (r *TaskRepository) ConfirmDeadLetterArchive(
	ctx context.Context,
	claim DeadLetterArchiveClaim,
	objectKey, sha256 string,
	sizeBytes int64,
	formatVersion int,
	archivedAt time.Time,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var message domain.DeadLetterMessage
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&message, claim.DeadLetterID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrDeadLetterNotFound
			}
			return err
		}
		if message.ReplayState == domain.DeadLetterReplaying {
			return ErrDeadLetterReplayActive
		}
		res := tx.Model(&domain.DeadLetterArchive{}).
			Where("id = ? AND dead_letter_id = ? AND state = ? AND archive_lease_token = ?",
				claim.ArchiveID, claim.DeadLetterID, domain.DeadLetterArchiveArchiving, claim.LeaseToken).
			Updates(map[string]any{
				"state":               domain.DeadLetterArchiveReady,
				"object_key":          objectKey,
				"sha256":              sha256,
				"size_bytes":          sizeBytes,
				"format_version":      formatVersion,
				"archive_lease_token": "",
				"archive_lease_until": nil,
				"last_archive_error":  "",
				"archived_at":         archivedAt,
				"verified_at":         archivedAt,
				"verified_copies":     1,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return ErrDeadLetterArchiveLeaseLost
		}
		return nil
	})
}

func (r *TaskRepository) MarkDeadLetterArchiveVerified(ctx context.Context, id uint64, verifiedAt time.Time) error {
	res := r.db.WithContext(ctx).Model(&domain.DeadLetterArchive{}).
		Where("id = ? AND state = ?", id, domain.DeadLetterArchiveReady).
		Updates(map[string]any{"verified_at": verifiedAt, "verified_copies": gorm.Expr("GREATEST(verified_copies, 1)")})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrDeadLetterArchiveNotFound
	}
	return nil
}

func (r *TaskRepository) GetDeadLetterArchive(ctx context.Context, id uint64) (*domain.DeadLetterArchive, error) {
	var archive domain.DeadLetterArchive
	if err := r.db.WithContext(ctx).First(&archive, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDeadLetterArchiveNotFound
		}
		return nil, err
	}
	return &archive, nil
}

func (r *TaskRepository) GetDeadLetterArchiveByDeadLetterID(ctx context.Context, deadLetterID uint64) (*domain.DeadLetterArchive, error) {
	var archive domain.DeadLetterArchive
	if err := r.db.WithContext(ctx).Where("dead_letter_id = ?", deadLetterID).First(&archive).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDeadLetterArchiveNotFound
		}
		return nil, err
	}
	return &archive, nil
}

func (r *TaskRepository) ListDeadLetterArchives(ctx context.Context, limit, offset int) ([]domain.DeadLetterArchive, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var archives []domain.DeadLetterArchive
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Offset(offset).Find(&archives).Error
	return archives, err
}

func (r *TaskRepository) RestoreDeadLetterArchive(ctx context.Context, archiveID uint64, restored *domain.DeadLetterMessage, now time.Time) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var archive domain.DeadLetterArchive
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&archive, archiveID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrDeadLetterArchiveNotFound
			}
			return err
		}
		if archive.State != domain.DeadLetterArchiveReady {
			return ErrDeadLetterArchiveActive
		}

		var existing int64
		if err := tx.Model(&domain.DeadLetterMessage{}).
			Where("source_backend = ? AND source_queue = ? AND source_delivery_id = ?",
				restored.SourceBackend, restored.SourceQueue, restored.SourceDeliveryID).
			Count(&existing).Error; err != nil {
			return err
		}
		if existing > 0 {
			return ErrDeadLetterArchiveRestoreConflict
		}

		if err := tx.Create(restored).Error; err != nil {
			return err
		}
		return tx.Model(&domain.DeadLetterArchive{}).Where("id = ?", archiveID).Updates(map[string]any{
			"restore_count":                gorm.Expr("restore_count + 1"),
			"last_restored_at":             now,
			"last_restored_dead_letter_id": restored.ID,
		}).Error
	})
}

func (r *TaskRepository) ListDeadLetterArchiveCandidates(
	ctx context.Context,
	replayedBefore, quarantinedBefore time.Time,
	maxEntries, limit int,
) ([]uint64, error) {
	if limit <= 0 {
		limit = 50
	}
	ids := make([]uint64, 0, limit)
	seen := make(map[uint64]struct{}, limit)
	appendUnique := func(values []uint64) {
		for _, id := range values {
			if len(ids) >= limit {
				return
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}

	if !replayedBefore.IsZero() || !quarantinedBefore.IsZero() {
		query := r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{})
		switch {
		case !replayedBefore.IsZero() && !quarantinedBefore.IsZero():
			query = query.Where("(replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?) OR (replay_state = ? AND created_at < ?)",
				domain.DeadLetterReplayed, replayedBefore, domain.DeadLetterQuarantined, quarantinedBefore)
		case !replayedBefore.IsZero():
			query = query.Where("replay_state = ? AND replayed_at IS NOT NULL AND replayed_at < ?", domain.DeadLetterReplayed, replayedBefore)
		default:
			query = query.Where("replay_state = ? AND created_at < ?", domain.DeadLetterQuarantined, quarantinedBefore)
		}
		var retentionIDs []uint64
		if err := query.Order("created_at ASC").Limit(limit).Pluck("id", &retentionIDs).Error; err != nil {
			return nil, err
		}
		appendUnique(retentionIDs)
	}

	if maxEntries > 0 && len(ids) < limit {
		var total int64
		if err := r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).Count(&total).Error; err != nil {
			return nil, err
		}
		if total > int64(maxEntries) {
			need := int(total - int64(maxEntries))
			remaining := limit - len(ids)
			if need < remaining {
				remaining = need
			}
			if remaining > 0 {
				var capacityIDs []uint64
				if err := r.db.WithContext(ctx).Model(&domain.DeadLetterMessage{}).
					Where("replay_state = ?", domain.DeadLetterReplayed).
					Order("created_at ASC").Limit(remaining).Pluck("id", &capacityIDs).Error; err != nil {
					return nil, err
				}
				appendUnique(capacityIDs)
			}
		}
	}
	return ids, nil
}
