package domain

import "time"

type DeadLetterState string

const (
	DeadLetterQuarantined DeadLetterState = "quarantined"
	DeadLetterReplaying   DeadLetterState = "replaying"
	DeadLetterReplayed    DeadLetterState = "replayed"
)

type DeadLetterMessage struct {
	ID                uint64          `gorm:"primaryKey;autoIncrement" json:"id"`
	SourceBackend     string          `gorm:"size:16;not null;uniqueIndex:idx_dead_letter_source,priority:1" json:"source_backend"`
	SourceQueue       string          `gorm:"size:255;not null;uniqueIndex:idx_dead_letter_source,priority:2" json:"source_queue"`
	SourceDeliveryID  string          `gorm:"size:128;not null;uniqueIndex:idx_dead_letter_source,priority:3" json:"source_delivery_id"`
	FailureClass      string          `gorm:"size:32;not null;index;index:idx_dlq_class_state,priority:1" json:"failure_class"`
	FailureReason     string          `gorm:"type:text;not null" json:"failure_reason"`
	RawEnvelope       string          `gorm:"type:longtext;not null" json:"raw_envelope"`
	TaskID            *uint64         `gorm:"index" json:"task_id,omitempty"`
	EnqueueGeneration *uint64         `json:"enqueue_generation,omitempty"`
	ReplayState       DeadLetterState `gorm:"size:20;not null;default:quarantined;index;index:idx_dlq_class_state,priority:2;index:idx_dlq_state_created,priority:1;index:idx_dlq_state_replayed,priority:1" json:"replay_state"`
	ReplayCount       int             `gorm:"not null;default:0" json:"replay_count"`
	ReplayLeaseUntil  *time.Time      `gorm:"index" json:"-"`
	LastReplayError   string          `gorm:"type:text" json:"last_replay_error,omitempty"`
	ReplayedAt        *time.Time      `gorm:"index:idx_dlq_state_replayed,priority:2" json:"replayed_at,omitempty"`
	CreatedAt         time.Time       `gorm:"index;index:idx_dlq_state_created,priority:2" json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

func (DeadLetterMessage) TableName() string { return "queue_dead_letters" }

// DeadLetterReplayAudit is an append-only forensic record of an operator/API
// replay attempt. It intentionally snapshots source metadata so the audit stays
// useful even after the parent dead-letter row is later purged by lifecycle
// retention.
type DeadLetterReplayAudit struct {
	ID                uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	DeadLetterID      uint64    `gorm:"not null;index:idx_dlq_replay_audit_dead_letter" json:"dead_letter_id"`
	SourceBackend     string    `gorm:"size:16;not null" json:"source_backend"`
	SourceQueue       string    `gorm:"size:255;not null" json:"source_queue"`
	SourceDeliveryID  string    `gorm:"size:128;not null" json:"source_delivery_id"`
	FailureClass      string    `gorm:"size:32;not null;index:idx_dlq_replay_audit_class" json:"failure_class"`
	TaskID            uint64    `gorm:"not null;index:idx_dlq_replay_audit_task" json:"task_id"`
	EnqueueGeneration uint64    `gorm:"not null" json:"enqueue_generation"`
	Outcome           string    `gorm:"size:32;not null;index:idx_dlq_replay_audit_outcome" json:"outcome"`
	FailureReason     string    `gorm:"type:text" json:"failure_reason,omitempty"`
	CreatedAt         time.Time `gorm:"index:idx_dlq_replay_audit_created" json:"created_at"`
}

func (DeadLetterReplayAudit) TableName() string { return "queue_dead_letter_replay_audits" }

type DeadLetterArchiveState string

const (
	DeadLetterArchiveArchiving DeadLetterArchiveState = "archiving"
	DeadLetterArchiveReady     DeadLetterArchiveState = "ready"
	DeadLetterArchiveFailed    DeadLetterArchiveState = "failed"
	DeadLetterArchiveExpiring  DeadLetterArchiveState = "expiring"
	DeadLetterArchiveExpired   DeadLetterArchiveState = "expired"
)

// DeadLetterArchive is durable metadata for immutable evidence stored outside
// the hot queue_dead_letters table. It deliberately has no foreign key to the
// hot row because that row is expected to be purged after a ready archive is
// confirmed.
type DeadLetterArchive struct {
	ID                       uint64                 `gorm:"primaryKey;autoIncrement" json:"id"`
	DeadLetterID             uint64                 `gorm:"not null;uniqueIndex:idx_dlq_archive_dead_letter" json:"dead_letter_id"`
	State                    DeadLetterArchiveState `gorm:"size:20;not null;index:idx_dlq_archive_state_created,priority:1" json:"state"`
	Sink                     string                 `gorm:"size:32;not null" json:"sink"`
	ObjectKey                string                 `gorm:"size:512;not null;default:''" json:"object_key,omitempty"`
	SHA256                   string                 `gorm:"size:64;not null;default:''" json:"sha256,omitempty"`
	SizeBytes                int64                  `gorm:"not null;default:0" json:"size_bytes"`
	FormatVersion            int                    `gorm:"not null;default:1" json:"format_version"`
	SourceBackend            string                 `gorm:"size:16;not null" json:"source_backend"`
	SourceQueue              string                 `gorm:"size:255;not null;index:idx_dlq_archive_source" json:"source_queue"`
	SourceDeliveryID         string                 `gorm:"size:128;not null" json:"source_delivery_id"`
	FailureClass             string                 `gorm:"size:32;not null" json:"failure_class"`
	ArchiveAttempts          int                    `gorm:"not null;default:0" json:"archive_attempts"`
	ArchiveLeaseToken        string                 `gorm:"size:64;not null;default:''" json:"-"`
	ArchiveLeaseUntil        *time.Time             `gorm:"index" json:"-"`
	LastArchiveError         string                 `gorm:"type:text" json:"last_archive_error,omitempty"`
	ArchivedAt               *time.Time             `gorm:"index" json:"archived_at,omitempty"`
	VerifiedAt               *time.Time             `gorm:"index" json:"verified_at,omitempty"`
	VerifiedCopies           int                    `gorm:"not null;default:0" json:"verified_copies"`
	RestoreCount             int                    `gorm:"not null;default:0" json:"restore_count"`
	LastRestoredAt           *time.Time             `json:"last_restored_at,omitempty"`
	LastRestoredDeadLetterID *uint64                `json:"last_restored_dead_letter_id,omitempty"`
	LastDrillAt              *time.Time             `gorm:"index" json:"last_drill_at,omitempty"`
	LastDrillError           string                 `gorm:"type:text" json:"last_drill_error,omitempty"`
	ExpiredAt                *time.Time             `gorm:"index" json:"expired_at,omitempty"`
	CreatedAt                time.Time              `gorm:"index:idx_dlq_archive_state_created,priority:2" json:"created_at"`
	UpdatedAt                time.Time              `json:"updated_at"`
}

func (DeadLetterArchive) TableName() string { return "queue_dead_letter_archives" }

type DeadLetterArchiveReplica struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	ArchiveID  uint64    `gorm:"not null;uniqueIndex:idx_dlq_archive_replica,priority:1;index" json:"archive_id"`
	Sink       string    `gorm:"size:64;not null;uniqueIndex:idx_dlq_archive_replica,priority:2" json:"sink"`
	ObjectKey  string    `gorm:"size:512;not null" json:"object_key"`
	SHA256     string    `gorm:"size:64;not null" json:"sha256"`
	SizeBytes  int64     `gorm:"not null" json:"size_bytes"`
	VerifiedAt time.Time `gorm:"index" json:"verified_at"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (DeadLetterArchiveReplica) TableName() string { return "queue_dead_letter_archive_replicas" }
