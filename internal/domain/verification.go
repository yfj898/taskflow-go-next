package domain

import "time"

const VerificationTaskType = "verify_change"

type VerificationRequest struct {
	RunID                string   `json:"run_id"`
	ProjectID            string   `json:"project_id"`
	TargetCommit         string   `json:"target_commit"`
	CheckProfileID       string   `json:"check_profile_id"`
	CheckProfileRevision string   `json:"check_profile_revision"`
	CheckIDs             []string `json:"check_ids"`
	ExternalSnapshotID   string   `json:"external_snapshot_id"`
}

type VerificationRun struct {
	RunID                string    `gorm:"primaryKey;size:36" json:"run_id"`
	ProjectID            string    `gorm:"size:128;not null;index" json:"project_id"`
	TargetCommit         string    `gorm:"size:40;not null" json:"target_commit"`
	CheckProfileID       string    `gorm:"size:128;not null" json:"check_profile_id"`
	CheckProfileRevision string    `gorm:"size:128;not null" json:"check_profile_revision"`
	CheckIDsJSON         string    `gorm:"type:text;not null" json:"-"`
	ExternalSnapshotID   string    `gorm:"size:255" json:"external_snapshot_id"`
	TaskID               uint64    `gorm:"not null;uniqueIndex:ux_verification_task" json:"task_id"`
	CreatedAt            time.Time `json:"created_at"`
}

func (VerificationRun) TableName() string { return "verification_runs" }

type VerificationCheck struct {
	ID         string `json:"id"`
	Required   bool   `json:"required"`
	Status     string `json:"status"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	Error      string `json:"error,omitempty"`
}

type VerificationReport struct {
	Generation uint64              `json:"generation"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
	Verdict    string              `json:"verdict"`
	Error      string              `json:"error,omitempty"`
	Checks     []VerificationCheck `json:"checks"`
}

type VerificationAttempt struct {
	RunID      string `gorm:"primaryKey;size:36"`
	Generation uint64 `gorm:"primaryKey;autoIncrement:false"`
	ReportJSON string `gorm:"type:longtext;not null"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (VerificationAttempt) TableName() string { return "verification_attempts" }

type VerificationRunView struct {
	VerificationRun
	CheckIDs   []string             `json:"check_ids"`
	Status     TaskStatus           `json:"status"`
	Generation uint64               `json:"generation"`
	LastError  string               `json:"last_error,omitempty"`
	Attempts   []VerificationReport `json:"attempts"`
}

func VerificationVerdict(checks []VerificationCheck) string {
	incomplete := false
	for _, c := range checks {
		if !c.Required {
			continue
		}
		if c.Status == "failed" {
			return "failed"
		}
		if c.Status != "passed" {
			incomplete = true
		}
	}
	if incomplete {
		return "incomplete"
	}
	return "passed"
}
