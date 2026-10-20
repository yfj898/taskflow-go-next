package domain

import "time"

type TaskStatus string

const (
	TaskPending TaskStatus = "pending"
	TaskRunning TaskStatus = "running"
	TaskSuccess TaskStatus = "success"
	TaskFailed  TaskStatus = "failed"
)

type Task struct {
	ID                 uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Name               string     `gorm:"size:128;not null" json:"name"`
	Type               string     `gorm:"size:32;not null;index:idx_type_status,priority:1" json:"type"`
	Payload            string     `gorm:"type:text" json:"payload"`
	Status             TaskStatus `gorm:"size:20;not null;index:idx_status_created,priority:1;index:idx_status_next_run,priority:1;index:idx_status_lease,priority:1;index:idx_type_status,priority:2;index:idx_status_enqueue,priority:1" json:"status"`
	Result             string     `gorm:"type:text" json:"result,omitempty"`
	RetryCount         int        `gorm:"not null;default:0" json:"retry_count"`
	LastError          string     `gorm:"type:text" json:"last_error,omitempty"`
	TraceParent        string     `gorm:"size:128" json:"-"`
	TraceState         string     `gorm:"size:512" json:"-"`
	EnqueueGeneration  uint64     `gorm:"not null;default:1;index:idx_status_enqueue,priority:3" json:"-"`
	EnqueuedGeneration uint64     `gorm:"not null;default:0" json:"-"`
	EnqueueLeaseUntil  *time.Time `gorm:"index:idx_status_enqueue,priority:4" json:"-"`
	// ExecutionMetadata is runtime-only metadata returned by an executor. The
	// worker persists executor-specific audit records separately.
	ExecutionMetadata string     `gorm:"-" json:"-"`
	NextRunAt         time.Time  `gorm:"not null;index:idx_status_next_run,priority:2;index:idx_status_enqueue,priority:2" json:"next_run_at"`
	LeaseUntil        *time.Time `gorm:"index:idx_status_lease,priority:2" json:"lease_until,omitempty"`
	CreatedAt         time.Time  `gorm:"index:idx_status_created,priority:2" json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (Task) TableName() string { return "tasks" }
