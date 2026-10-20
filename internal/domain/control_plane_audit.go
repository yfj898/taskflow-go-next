package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type ControlPlaneAudit struct {
	ID             uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	OperatorID     string     `gorm:"size:128;not null;index:idx_control_plane_operator_created,priority:1" json:"operator_id"`
	CredentialSlot string     `gorm:"size:16;not null" json:"credential_slot"`
	Method         string     `gorm:"size:16;not null" json:"method"`
	Route          string     `gorm:"size:256;not null;index:idx_control_plane_route_created,priority:1" json:"route"`
	Target         string     `gorm:"size:512;not null" json:"target"`
	RequestID      string     `gorm:"size:128;not null;index" json:"request_id"`
	StatusCode     int        `gorm:"not null;default:0" json:"status_code"`
	Outcome        string     `gorm:"size:32;not null;index" json:"outcome"`
	ChainSequence  *uint64    `gorm:"uniqueIndex" json:"chain_sequence,omitempty"`
	PreviousHash   string     `gorm:"size:64;not null;default:''" json:"previous_hash,omitempty"`
	RecordHash     string     `gorm:"size:64;not null;default:'';index" json:"record_hash,omitempty"`
	CreatedAt      time.Time  `gorm:"index:idx_control_plane_operator_created,priority:2;index:idx_control_plane_route_created,priority:2" json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

func (ControlPlaneAudit) TableName() string { return "control_plane_audits" }

type ControlPlaneAuditChainState struct {
	ID                      uint      `gorm:"primaryKey" json:"id"`
	LastSequence            uint64    `gorm:"not null" json:"last_sequence"`
	HeadHash                string    `gorm:"size:64;not null;default:''" json:"head_hash"`
	RetentionAnchorSequence uint64    `gorm:"not null" json:"retention_anchor_sequence"`
	RetentionAnchorHash     string    `gorm:"size:64;not null;default:''" json:"retention_anchor_hash"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (ControlPlaneAuditChainState) TableName() string { return "control_plane_audit_chain_state" }

type ControlPlaneAuditIntegrityReport struct {
	Valid                   bool   `json:"valid"`
	Checked                 int    `json:"checked"`
	LastSequence            uint64 `json:"last_sequence"`
	HeadHash                string `json:"head_hash"`
	RetentionAnchorSequence uint64 `json:"retention_anchor_sequence"`
	RetentionAnchorHash     string `json:"retention_anchor_hash"`
	Failure                 string `json:"failure,omitempty"`
}

func ComputeControlPlaneAuditHash(audit ControlPlaneAudit) (string, error) {
	completedAt := ""
	if audit.CompletedAt != nil {
		completedAt = canonicalAuditTime(*audit.CompletedAt)
	}
	sequence := uint64(0)
	if audit.ChainSequence != nil {
		sequence = *audit.ChainSequence
	}
	payload := struct {
		Version        string `json:"version"`
		Sequence       uint64 `json:"sequence"`
		ID             uint64 `json:"id"`
		OperatorID     string `json:"operator_id"`
		CredentialSlot string `json:"credential_slot"`
		Method         string `json:"method"`
		Route          string `json:"route"`
		Target         string `json:"target"`
		RequestID      string `json:"request_id"`
		StatusCode     int    `json:"status_code"`
		Outcome        string `json:"outcome"`
		CreatedAt      string `json:"created_at"`
		CompletedAt    string `json:"completed_at"`
		PreviousHash   string `json:"previous_hash"`
	}{
		Version:        "taskflow-control-plane-audit-v1",
		Sequence:       sequence,
		ID:             audit.ID,
		OperatorID:     audit.OperatorID,
		CredentialSlot: audit.CredentialSlot,
		Method:         audit.Method,
		Route:          audit.Route,
		Target:         audit.Target,
		RequestID:      audit.RequestID,
		StatusCode:     audit.StatusCode,
		Outcome:        audit.Outcome,
		CreatedAt:      canonicalAuditTime(audit.CreatedAt),
		CompletedAt:    completedAt,
		PreviousHash:   audit.PreviousHash,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalAuditTime(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
}
