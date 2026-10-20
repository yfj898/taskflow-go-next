package domain

import "time"

const (
	AIBudgetScopeGlobal = "global"
	AIBudgetScopeTenant = "tenant"

	AIBudgetReservationReserved = "reserved"
	AIBudgetReservationSettled  = "settled"
	AIBudgetReservationReleased = "released"
)

type AIBudgetAccount struct {
	ID             uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Scope          string    `gorm:"size:16;not null;uniqueIndex:idx_ai_budget_scope_key,priority:1" json:"scope"`
	ScopeKey       string    `gorm:"size:128;not null;uniqueIndex:idx_ai_budget_scope_key,priority:2" json:"scope_key"`
	LimitUSD       float64   `gorm:"type:decimal(18,8);not null;default:0" json:"limit_usd"`
	LimitTokens    int64     `gorm:"not null;default:0" json:"limit_tokens"`
	SpentUSD       float64   `gorm:"type:decimal(18,8);not null;default:0" json:"spent_usd"`
	SpentTokens    int64     `gorm:"not null;default:0" json:"spent_tokens"`
	ReservedUSD    float64   `gorm:"type:decimal(18,8);not null;default:0" json:"reserved_usd"`
	ReservedTokens int64     `gorm:"not null;default:0" json:"reserved_tokens"`
	Version        uint64    `gorm:"not null;default:1" json:"version"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (AIBudgetAccount) TableName() string { return "ai_budget_accounts" }

type AIBudgetReservation struct {
	ID             uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID         uint64     `gorm:"not null;uniqueIndex:idx_ai_budget_task_attempt,priority:1" json:"task_id"`
	Attempt        int        `gorm:"not null;uniqueIndex:idx_ai_budget_task_attempt,priority:2" json:"attempt"`
	TenantID       string     `gorm:"size:128" json:"tenant_id,omitempty"`
	ApplyGlobal    bool       `gorm:"not null;default:false" json:"apply_global"`
	ApplyTenant    bool       `gorm:"not null;default:false" json:"apply_tenant"`
	ReservedUSD    float64    `gorm:"type:decimal(18,8);not null;default:0" json:"reserved_usd"`
	ReservedTokens int64      `gorm:"not null;default:0" json:"reserved_tokens"`
	ActualUSD      float64    `gorm:"type:decimal(18,8);not null;default:0" json:"actual_usd"`
	ActualTokens   int64      `gorm:"not null;default:0" json:"actual_tokens"`
	Status         string     `gorm:"size:16;not null;index" json:"status"`
	SettledAt      *time.Time `json:"settled_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (AIBudgetReservation) TableName() string { return "ai_budget_reservations" }
