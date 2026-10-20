package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/verification"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrVerificationNotFound = errors.New("verification run not found")
var ErrVerificationConflict = errors.New("verification run UUID conflicts with execution fields")
var ErrVerificationLeaseLost = errors.New("verification generation no longer owned")

const verificationMigrationVersion = "20261019_verification_v1"

func (r *TaskRepository) MigrateVerificationExplicit() error {
	return r.withMigrationLock(func() error {
		if err := r.db.AutoMigrate(&domain.VerificationRun{}, &domain.VerificationAttempt{}); err != nil {
			return err
		}
		if err := r.db.Exec(`CREATE TABLE IF NOT EXISTS taskflow_schema_migrations (version VARCHAR(128) NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)) ENGINE=InnoDB`).Error; err != nil {
			return err
		}
		return r.db.Exec("INSERT INTO taskflow_schema_migrations(version,description) VALUES (?,?) ON DUPLICATE KEY UPDATE version=version", verificationMigrationVersion, "verification runs and generation-bound reports v1").Error
	})
}
func (r *TaskRepository) VerifyVerificationSchema() error {
	m := r.db.Migrator()
	if !m.HasTable(&domain.VerificationRun{}) || !m.HasTable(&domain.VerificationAttempt{}) {
		return errors.New("verification schema missing; run schema-migrate --phase=verification-expand")
	}
	if !m.HasIndex(&domain.VerificationRun{}, "ux_verification_task") || !m.HasIndex(&domain.VerificationAttempt{}, "PRIMARY") {
		return errors.New("verification schema missing uniqueness indexes; run schema-migrate --phase=verification-expand")
	}
	var n int64
	if err := r.db.Raw("SELECT COUNT(*) FROM taskflow_schema_migrations WHERE version=?", verificationMigrationVersion).Scan(&n).Error; err != nil {
		return err
	}
	if n != 1 {
		return errors.New("verification migration journal missing")
	}
	return nil
}

func requestMatches(run *domain.VerificationRun, in domain.VerificationRequest) (bool, error) {
	var ids []string
	if err := json.Unmarshal([]byte(run.CheckIDsJSON), &ids); err != nil {
		return false, fmt.Errorf("decode persisted check IDs: %w", err)
	}
	requested := append([]string(nil), in.CheckIDs...)
	sort.Strings(ids)
	sort.Strings(requested)
	return run.ProjectID == in.ProjectID && run.TargetCommit == in.TargetCommit && run.CheckProfileID == in.CheckProfileID && run.CheckProfileRevision == in.CheckProfileRevision && run.ExternalSnapshotID == in.ExternalSnapshotID && reflect.DeepEqual(ids, requested), nil
}

// CreateVerification commits the run, task and outbox as one intent. A unique
// run UUID is the concurrency boundary; duplicate transactions roll back fully.
func (r *TaskRepository) CreateVerification(ctx context.Context, in domain.VerificationRequest, profile verification.Profile) (*domain.VerificationRun, bool, error) {
	existing, err := r.VerificationRun(ctx, in.RunID)
	if err == nil {
		matches, err := requestMatches(existing, in)
		if err != nil {
			return nil, false, err
		}
		if !matches {
			return nil, false, ErrVerificationConflict
		}
		return existing, false, nil
	}
	if !errors.Is(err, ErrVerificationNotFound) {
		return nil, false, err
	}
	ids, err := json.Marshal(in.CheckIDs)
	if err != nil {
		return nil, false, err
	}
	payload, err := json.Marshal(verification.TaskPayload{VerificationRequest: in, RegisteredProfile: profile})
	if err != nil {
		return nil, false, err
	}
	run := &domain.VerificationRun{RunID: in.RunID, ProjectID: in.ProjectID, TargetCommit: in.TargetCommit, CheckProfileID: in.CheckProfileID, CheckProfileRevision: in.CheckProfileRevision, CheckIDsJSON: string(ids), ExternalSnapshotID: in.ExternalSnapshotID}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		task := domain.Task{Name: "verification " + in.RunID, Type: domain.VerificationTaskType, Payload: string(payload), Status: domain.TaskPending, NextRunAt: time.Now(), EnqueueGeneration: 1}
		if err := tx.Create(&task).Error; err != nil {
			return err
		}
		run.TaskID = task.ID
		if err := tx.Create(run).Error; err != nil {
			return err
		}
		return tx.Create(&domain.OutboxEvent{TaskID: task.ID, EventType: domain.OutboxTaskCreated}).Error
	})
	if err == nil {
		return run, true, nil
	}
	var duplicate *gomysql.MySQLError
	if errors.As(err, &duplicate) && duplicate.Number == 1062 {
		existing, getErr := r.VerificationRun(ctx, in.RunID)
		if getErr != nil {
			return nil, false, getErr
		}
		matches, err := requestMatches(existing, in)
		if err != nil {
			return nil, false, err
		}
		if !matches {
			return nil, false, ErrVerificationConflict
		}
		return existing, false, nil
	}
	return nil, false, err
}
func (r *TaskRepository) VerificationRun(ctx context.Context, id string) (*domain.VerificationRun, error) {
	var run domain.VerificationRun
	err := r.db.WithContext(ctx).First(&run, "run_id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrVerificationNotFound
	}
	return &run, err
}
func (r *TaskRepository) VerificationView(ctx context.Context, id string) (*domain.VerificationRunView, error) {
	var view domain.VerificationRunView
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&view.VerificationRun, "run_id = ?", id).Error; err != nil {
			return err
		}
		var task domain.Task
		if err := tx.First(&task, view.TaskID).Error; err != nil {
			return err
		}
		view.Status = task.Status
		view.Generation = task.EnqueueGeneration
		view.LastError = task.LastError
		if err := json.Unmarshal([]byte(view.CheckIDsJSON), &view.CheckIDs); err != nil {
			return err
		}
		var attempts []domain.VerificationAttempt
		if err := tx.Where("run_id = ?", id).Order("generation ASC").Find(&attempts).Error; err != nil {
			return err
		}
		view.Attempts = []domain.VerificationReport{}
		for _, a := range attempts {
			var report domain.VerificationReport
			if err := json.Unmarshal([]byte(a.ReportJSON), &report); err != nil {
				return fmt.Errorf("decode persisted verification report: %w", err)
			}
			view.Attempts = append(view.Attempts, report)
		}
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrVerificationNotFound
	}
	return &view, err
}
func putReport(tx *gorm.DB, runID string, report *domain.VerificationReport) error {
	b, err := json.Marshal(report)
	if err != nil {
		return err
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "run_id"}, {Name: "generation"}}, DoUpdates: clause.AssignmentColumns([]string{"report_json", "updated_at"})}).Create(&domain.VerificationAttempt{RunID: runID, Generation: report.Generation, ReportJSON: string(b)}).Error
}
func (r *TaskRepository) BeginVerificationAttempt(ctx context.Context, task *domain.Task, report *domain.VerificationReport) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current domain.Task
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, task.ID).Error; err != nil {
			return err
		}
		if current.Type != domain.VerificationTaskType || current.Status != domain.TaskRunning || current.EnqueueGeneration != report.Generation {
			return ErrVerificationLeaseLost
		}
		var run domain.VerificationRun
		if err := tx.First(&run, "task_id = ?", task.ID).Error; err != nil {
			return err
		}
		return putReport(tx, run.RunID, report)
	})
}
func (r *TaskRepository) RenewVerificationLease(ctx context.Context, id, generation uint64, until time.Time) (bool, error) {
	res := r.db.WithContext(ctx).Model(&domain.Task{}).Where("id = ? AND type = ? AND status = ? AND enqueue_generation = ?", id, domain.VerificationTaskType, domain.TaskRunning, generation).Update("lease_until", until)
	return res.RowsAffected == 1, res.Error
}
func (r *TaskRepository) SettleVerification(ctx context.Context, taskID uint64, report *domain.VerificationReport, recoverPending bool) (bool, error) {
	updated := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		status := domain.TaskFailed
		if report.Verdict == "passed" {
			status = domain.TaskSuccess
		}
		if recoverPending {
			status = domain.TaskPending
		}
		updates := map[string]any{"status": status, "last_error": report.Error, "lease_until": nil}
		if recoverPending {
			updates["enqueue_generation"] = gorm.Expr("enqueue_generation + 1")
			updates["enqueue_lease_until"] = nil
			updates["next_run_at"] = time.Now()
		}
		res := tx.Model(&domain.Task{}).Where("id = ? AND type = ? AND status = ? AND enqueue_generation = ?", taskID, domain.VerificationTaskType, domain.TaskRunning, report.Generation).Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return nil
		}
		var run domain.VerificationRun
		if err := tx.First(&run, "task_id = ?", taskID).Error; err != nil {
			return err
		}
		if err := putReport(tx, run.RunID, report); err != nil {
			return err
		}
		updated = true
		return nil
	})
	return updated, err
}
func (r *TaskRepository) recoverStaleVerifications(ctx context.Context, now, legacyBefore time.Time) (int64, error) {
	var recovered int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var tasks []domain.Task
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("type = ? AND status = ? AND ((lease_until IS NOT NULL AND lease_until < ?) OR (lease_until IS NULL AND updated_at < ?))", domain.VerificationTaskType, domain.TaskRunning, now, legacyBefore).Find(&tasks).Error; err != nil {
			return err
		}
		for _, task := range tasks {
			var run domain.VerificationRun
			if err := tx.First(&run, "task_id = ?", task.ID).Error; err != nil {
				return err
			}
			var attempt domain.VerificationAttempt
			err := tx.First(&attempt, "run_id = ? AND generation = ?", run.RunID, task.EnqueueGeneration).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			report := domain.VerificationReport{Generation: task.EnqueueGeneration, StartedAt: task.UpdatedAt, Checks: []domain.VerificationCheck{}}
			if err == nil {
				if err := json.Unmarshal([]byte(attempt.ReportJSON), &report); err != nil {
					return err
				}
			}
			report.FinishedAt = &now
			report.Verdict = "incomplete"
			report.Error = "recovered stale running verification"
			if err := putReport(tx, run.RunID, &report); err != nil {
				return err
			}
			if err := tx.Model(&domain.Task{}).Where("id = ?", task.ID).Updates(map[string]any{"status": domain.TaskPending, "last_error": report.Error, "next_run_at": now, "lease_until": nil, "enqueue_generation": gorm.Expr("enqueue_generation + 1"), "enqueue_lease_until": nil}).Error; err != nil {
				return err
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}
