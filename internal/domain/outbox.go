package domain

import "time"

const OutboxTaskCreated = "task.created"

type OutboxEvent struct {
	ID          uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID      uint64     `gorm:"not null;uniqueIndex:idx_outbox_task_event,priority:1" json:"task_id"`
	EventType   string     `gorm:"size:64;not null;uniqueIndex:idx_outbox_task_event,priority:2" json:"event_type"`
	Attempts    int        `gorm:"not null;default:0" json:"attempts"`
	LastError   string     `gorm:"type:text" json:"last_error,omitempty"`
	PublishedAt *time.Time `gorm:"index:idx_outbox_publish,priority:1" json:"published_at,omitempty"`
	LeaseUntil  *time.Time `gorm:"index:idx_outbox_publish,priority:2" json:"lease_until,omitempty"`
	CreatedAt   time.Time  `gorm:"index:idx_outbox_publish,priority:3" json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (OutboxEvent) TableName() string { return "outbox_events" }
