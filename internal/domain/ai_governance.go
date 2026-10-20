package domain

import "time"

type AIRouteAudit struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID       uint64    `gorm:"not null;index:idx_ai_route_task_attempt,priority:1" json:"task_id"`
	Attempt      int       `gorm:"not null;index:idx_ai_route_task_attempt,priority:2" json:"attempt"`
	MetadataJSON string    `gorm:"type:json;not null" json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

func (AIRouteAudit) TableName() string { return "ai_route_audits" }

type AIRoutingPolicy struct {
	ID                      uint      `gorm:"primaryKey" json:"id"`
	Version                 uint64    `gorm:"not null" json:"version"`
	Strategy                string    `gorm:"size:32;not null" json:"strategy"`
	DefaultOutputTokens     int       `gorm:"not null" json:"default_output_tokens"`
	TenantQuotaManaged      bool      `gorm:"not null;default:false" json:"tenant_quota_managed"`
	TenantQuotaEnabled      bool      `gorm:"not null;default:false" json:"tenant_quota_enabled"`
	TenantQuotaDefaultLimit int       `gorm:"not null;default:0" json:"tenant_quota_default_limit"`
	TenantQuotaOverrides    string    `gorm:"size:4096;not null;default:''" json:"tenant_quota_overrides"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (AIRoutingPolicy) TableName() string { return "ai_routing_policies" }
