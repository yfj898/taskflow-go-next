package domain

import "time"

const (
	ControlPlaneDeliveryPending    = "pending"
	ControlPlaneDeliveryDelivering = "delivering"
	ControlPlaneDeliveryDelivered  = "delivered"
	ControlPlaneDeliveryDead       = "dead"

	ControlPlaneDeliveryDLQDead     = "dead"
	ControlPlaneDeliveryDLQReplayed = "replayed"

	ControlPlaneSIEMPromotionPending   = "pending"
	ControlPlaneSIEMPromotionApproved  = "approved"
	ControlPlaneSIEMPromotionExecuting = "executing"
	ControlPlaneSIEMPromotionExecuted  = "executed"
	ControlPlaneSIEMPromotionExpired   = "expired"
	ControlPlaneSIEMPromotionCancelled = "cancelled"
)

type ControlPlaneAuditDelivery struct {
	ID            uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	AuditID       uint64     `gorm:"not null;index" json:"audit_id"`
	ChainSequence uint64     `gorm:"not null;uniqueIndex" json:"chain_sequence"`
	RecordHash    string     `gorm:"size:64;not null" json:"record_hash"`
	PayloadJSON   string     `gorm:"type:longtext;not null" json:"-"`
	State         string     `gorm:"size:16;not null;index:idx_control_plane_delivery_ready,priority:1" json:"state"`
	Attempts      int        `gorm:"not null;default:0" json:"attempts"`
	NextAttemptAt time.Time  `gorm:"not null;index:idx_control_plane_delivery_ready,priority:2" json:"next_attempt_at"`
	LeaseToken    string     `gorm:"size:64;not null;default:''" json:"-"`
	LeaseUntil    *time.Time `gorm:"index" json:"-"`
	LeaderFence   uint64     `gorm:"not null;default:0;index" json:"leader_fence,omitempty"`
	LastError     string     `gorm:"size:1024;not null;default:''" json:"last_error,omitempty"`
	DeliveredAt   *time.Time `json:"delivered_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (ControlPlaneAuditDelivery) TableName() string { return "control_plane_audit_deliveries" }

type ControlPlaneAuditDeliveryReceipt struct {
	ID             uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	DeliveryID     uint64    `gorm:"not null;index" json:"delivery_id"`
	AuditID        uint64    `gorm:"not null;index" json:"audit_id"`
	ChainSequence  uint64    `gorm:"not null;uniqueIndex" json:"chain_sequence"`
	RecordHash     string    `gorm:"size:64;not null;index" json:"record_hash"`
	ReceiptID      string    `gorm:"size:128;not null;default:'';index" json:"receipt_id,omitempty"`
	IdempotencyKey string    `gorm:"size:96;not null;default:'';index" json:"idempotency_key,omitempty"`
	PayloadSHA256  string    `gorm:"size:64;not null;default:''" json:"payload_sha256,omitempty"`
	AckHash        string    `gorm:"size:64;not null;default:''" json:"ack_hash,omitempty"`
	AckStatus      string    `gorm:"size:16;not null;default:'legacy'" json:"ack_status,omitempty"`
	ReceiverID     string    `gorm:"size:64;not null;default:'';index" json:"receiver_id,omitempty"`
	ReceiverGen    uint64    `gorm:"not null;default:0" json:"receiver_generation,omitempty"`
	ReceiverTerm   uint64    `gorm:"not null;default:0" json:"receiver_term,omitempty"`
	ReplicaCopies  int       `gorm:"not null;default:0" json:"replica_copies,omitempty"`
	RecoverySource string    `gorm:"size:32;not null;default:'';index" json:"recovery_source,omitempty"`
	DeliveredAt    time.Time `gorm:"not null;index" json:"delivered_at"`
	CreatedAt      time.Time `json:"created_at"`
}

func (ControlPlaneAuditDeliveryReceipt) TableName() string {
	return "control_plane_audit_delivery_receipts"
}

type ControlPlaneSIEMAcknowledgement struct {
	ReceiptID      string
	IdempotencyKey string
	PayloadSHA256  string
	AckHash        string
	Status         string
	ReceiverID     string
	ReceiverGen    uint64
	ReceiverTerm   uint64
	ReplicaCopies  int
}

type ControlPlaneAuditDeliveryDLQ struct {
	ID            uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	DeliveryID    uint64     `gorm:"not null;uniqueIndex" json:"delivery_id"`
	AuditID       uint64     `gorm:"not null;index" json:"audit_id"`
	ChainSequence uint64     `gorm:"not null;index" json:"chain_sequence"`
	RecordHash    string     `gorm:"size:64;not null" json:"record_hash"`
	PayloadJSON   string     `gorm:"type:longtext;not null" json:"-"`
	State         string     `gorm:"size:16;not null;index" json:"state"`
	FailureCount  int        `gorm:"not null;default:1" json:"failure_count"`
	Attempts      int        `gorm:"not null" json:"attempts"`
	LastError     string     `gorm:"size:1024;not null" json:"last_error"`
	FailedAt      time.Time  `gorm:"not null;index" json:"failed_at"`
	ReplayedAt    *time.Time `json:"replayed_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (ControlPlaneAuditDeliveryDLQ) TableName() string {
	return "control_plane_audit_delivery_dlq"
}

type ControlPlaneSIEMDeliveryCoordinatorState struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	ActiveFence uint64    `gorm:"not null;default:0" json:"active_fence"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (ControlPlaneSIEMDeliveryCoordinatorState) TableName() string {
	return "control_plane_siem_delivery_coordinator_state"
}

type ControlPlaneSIEMReceiverRoutingState struct {
	ID                   uint       `gorm:"primaryKey" json:"id"`
	ActiveReceiver       string     `gorm:"size:64;not null" json:"active_receiver"`
	Term                 uint64     `gorm:"not null;default:1" json:"term"`
	PromotedBy           string     `gorm:"size:128;not null;default:''" json:"promoted_by,omitempty"`
	PromotedAt           time.Time  `gorm:"not null" json:"promoted_at"`
	TransitionIntentID   uint64     `gorm:"not null;default:0;index" json:"transition_intent_id,omitempty"`
	TransitionToken      string     `gorm:"size:64;not null;default:''" json:"-"`
	TransitionLeaseUntil *time.Time `gorm:"index" json:"transition_lease_until,omitempty"`
	RPOBreachSince       *time.Time `json:"rpo_breach_since,omitempty"`
	RPOFenced            bool       `gorm:"not null;default:false;index" json:"rpo_fenced"`
	RPOFenceReason       string     `gorm:"size:256;not null;default:''" json:"rpo_fence_reason,omitempty"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

func (ControlPlaneSIEMReceiverRoutingState) TableName() string {
	return "control_plane_siem_receiver_routing_state"
}

type ControlPlaneSIEMReceiverStatus struct {
	ActiveReceiver              string     `json:"active_receiver"`
	Term                        uint64     `json:"term"`
	PrimaryHighWatermark        uint64     `json:"primary_high_watermark"`
	SecondaryHighWatermark      uint64     `json:"secondary_high_watermark"`
	PrimaryReceiverGeneration   uint64     `json:"primary_receiver_generation"`
	SecondaryReceiverGeneration uint64     `json:"secondary_receiver_generation"`
	PrimaryReceiverTerm         uint64     `json:"primary_receiver_term"`
	SecondaryReceiverTerm       uint64     `json:"secondary_receiver_term"`
	RPOSequences                uint64     `json:"rpo_sequences"`
	RPOFenced                   bool       `json:"rpo_fenced"`
	RPOBreachSince              *time.Time `json:"rpo_breach_since,omitempty"`
	RPOFenceReason              string     `json:"rpo_fence_reason,omitempty"`
}

type ControlPlaneSIEMReceiverPromotionResult struct {
	ActiveReceiver string `json:"active_receiver"`
	Term           uint64 `json:"term"`
	PreviousTerm   uint64 `json:"previous_term"`
	PromotedBy     string `json:"promoted_by"`
	Emergency      bool   `json:"emergency,omitempty"`
	IncidentID     string `json:"incident_id,omitempty"`
	Anchored       bool   `json:"anchored,omitempty"`
	AnchorID       string `json:"anchor_id,omitempty"`
}

type ControlPlaneSIEMPromotionIntent struct {
	ID                uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TargetReceiver    string     `gorm:"size:64;not null;index" json:"target_receiver"`
	ExpectedTerm      uint64     `gorm:"not null;index" json:"expected_term"`
	State             string     `gorm:"size:16;not null;index" json:"state"`
	RequestedBy       string     `gorm:"size:128;not null;index" json:"requested_by"`
	ApprovedBy        string     `gorm:"size:512;not null;default:'';index" json:"approved_by,omitempty"`
	RequiredApprovals int        `gorm:"not null;default:1" json:"required_approvals"`
	ApprovalCount     int        `gorm:"not null;default:0" json:"approval_count"`
	Reason            string     `gorm:"size:512;not null;default:''" json:"reason,omitempty"`
	ApprovalNote      string     `gorm:"size:512;not null;default:''" json:"approval_note,omitempty"`
	ExpiresAt         time.Time  `gorm:"not null;index" json:"expires_at"`
	ApprovedAt        *time.Time `json:"approved_at,omitempty"`
	ExecutedAt        *time.Time `json:"executed_at,omitempty"`
	ResultTerm        uint64     `gorm:"not null;default:0" json:"result_term,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (ControlPlaneSIEMPromotionIntent) TableName() string {
	return "control_plane_siem_promotion_intents"
}

type ControlPlaneSIEMPromotionApproval struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	IntentID   uint64    `gorm:"not null;uniqueIndex:idx_siem_promotion_approval,priority:1;index" json:"intent_id"`
	OperatorID string    `gorm:"size:128;not null;uniqueIndex:idx_siem_promotion_approval,priority:2" json:"operator_id"`
	Role       string    `gorm:"size:64;not null" json:"role"`
	Note       string    `gorm:"size:512;not null;default:''" json:"note,omitempty"`
	ApprovedAt time.Time `gorm:"not null;index" json:"approved_at"`
}

func (ControlPlaneSIEMPromotionApproval) TableName() string {
	return "control_plane_siem_promotion_approvals"
}

type ControlPlaneSIEMReceiverTermHistory struct {
	ID               uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Term             uint64     `gorm:"not null;uniqueIndex" json:"term"`
	PreviousTerm     uint64     `gorm:"not null" json:"previous_term"`
	PreviousReceiver string     `gorm:"size:64;not null;default:''" json:"previous_receiver,omitempty"`
	ActiveReceiver   string     `gorm:"size:64;not null" json:"active_receiver"`
	Action           string     `gorm:"size:32;not null;index" json:"action"`
	IntentID         uint64     `gorm:"not null;default:0;index" json:"intent_id,omitempty"`
	RequestedBy      string     `gorm:"size:128;not null;default:''" json:"requested_by,omitempty"`
	ApprovedBy       string     `gorm:"size:512;not null;default:''" json:"approved_by,omitempty"`
	Reason           string     `gorm:"size:512;not null;default:''" json:"reason,omitempty"`
	RPOSequences     uint64     `gorm:"not null;default:0" json:"rpo_sequences"`
	HashVersion      uint       `gorm:"not null;default:1" json:"hash_version"`
	Emergency        bool       `gorm:"not null;default:false" json:"emergency,omitempty"`
	IncidentID       string     `gorm:"size:128;not null;default:'';index" json:"incident_id,omitempty"`
	PreviousHash     string     `gorm:"size:64;not null;default:''" json:"previous_hash,omitempty"`
	RecordHash       string     `gorm:"size:64;not null;uniqueIndex" json:"record_hash"`
	AnchorState      string     `gorm:"size:16;not null;default:'';index" json:"anchor_state,omitempty"`
	AnchorAttempts   int        `gorm:"not null;default:0" json:"anchor_attempts,omitempty"`
	AnchorNextAt     *time.Time `gorm:"index" json:"-"`
	AnchorLeaseToken string     `gorm:"size:128;not null;default:'';index" json:"-"`
	AnchorLeaseUntil *time.Time `gorm:"index" json:"-"`
	AnchorLastError  string     `gorm:"size:1024;not null;default:''" json:"anchor_last_error,omitempty"`
	CreatedAt        time.Time  `gorm:"not null;index" json:"created_at"`
}

func (ControlPlaneSIEMReceiverTermHistory) TableName() string {
	return "control_plane_siem_receiver_term_history"
}

type ControlPlaneSIEMTermLedgerAnchor struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Term        uint64    `gorm:"not null;uniqueIndex" json:"term"`
	RecordHash  string    `gorm:"size:64;not null;index" json:"record_hash"`
	AnchorID    string    `gorm:"size:128;not null;uniqueIndex" json:"anchor_id"`
	KeyID       string    `gorm:"size:128;not null" json:"key_id"`
	Algorithm   string    `gorm:"size:64;not null" json:"algorithm"`
	Signature   string    `gorm:"size:1024;not null" json:"signature"`
	ReceiptHash string    `gorm:"size:64;not null" json:"receipt_hash"`
	AnchoredAt  time.Time `gorm:"not null;index" json:"anchored_at"`
}

func (ControlPlaneSIEMTermLedgerAnchor) TableName() string {
	return "control_plane_siem_term_ledger_anchors"
}

type ControlPlaneSIEMIncidentEvidence struct {
	ID              uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	IncidentID      string     `gorm:"size:128;not null;index" json:"incident_id"`
	Kind            string     `gorm:"size:64;not null;index" json:"kind"`
	Severity        string     `gorm:"size:16;not null;index" json:"severity"`
	Term            uint64     `gorm:"not null;default:0;index" json:"term"`
	ActiveReceiver  string     `gorm:"size:64;not null;default:''" json:"active_receiver,omitempty"`
	RPOSequences    uint64     `gorm:"not null;default:0" json:"rpo_sequences"`
	RTOSeconds      float64    `gorm:"not null;default:0" json:"rto_seconds"`
	Reason          string     `gorm:"size:1024;not null;default:''" json:"reason,omitempty"`
	EvidenceJSON    string     `gorm:"type:longtext;not null" json:"-"`
	EvidenceHash    string     `gorm:"size:64;not null;uniqueIndex" json:"evidence_hash"`
	AlertState      string     `gorm:"size:16;not null;default:'';index" json:"alert_state,omitempty"`
	AlertAttempts   int        `gorm:"not null;default:0" json:"alert_attempts,omitempty"`
	AlertNextAt     *time.Time `gorm:"index" json:"-"`
	AlertLeaseToken string     `gorm:"size:128;not null;default:'';index" json:"-"`
	AlertLeaseUntil *time.Time `gorm:"index" json:"-"`
	AlertLastError  string     `gorm:"size:1024;not null;default:''" json:"alert_last_error,omitempty"`
	AlertedAt       *time.Time `gorm:"index" json:"alerted_at,omitempty"`
	CreatedAt       time.Time  `gorm:"not null;index" json:"created_at"`
}

func (ControlPlaneSIEMIncidentEvidence) TableName() string {
	return "control_plane_siem_incident_evidence"
}

type ControlPlaneSIEMTermHistoryVerification struct {
	Valid        bool   `json:"valid"`
	Entries      int    `json:"entries"`
	LastTerm     uint64 `json:"last_term"`
	LastHash     string `json:"last_hash"`
	FailedTerm   uint64 `json:"failed_term,omitempty"`
	FailureCause string `json:"failure_cause,omitempty"`
}

type ControlPlaneSIEMGovernanceDeliveryClassStats struct {
	Pending                  int64   `json:"pending"`
	Delivering               int64   `json:"delivering"`
	Delivered                int64   `json:"delivered"`
	Dead                     int64   `json:"dead"`
	Outstanding              int64   `json:"outstanding"`
	OldestOutstandingSeconds float64 `json:"oldest_outstanding_seconds"`
}

type ControlPlaneSIEMGovernanceStatus struct {
	Enabled                     bool                                         `json:"enabled"`
	PolicyEnabled               bool                                         `json:"policy_enabled"`
	HAEnabled                   bool                                         `json:"ha_enabled"`
	ReplicaCount                int                                          `json:"replica_count,omitempty"`
	ReplicaQuorum               int                                          `json:"replica_quorum,omitempty"`
	MembershipEpoch             uint64                                       `json:"membership_epoch,omitempty"`
	PreviousKeyGraceReplicas    int                                          `json:"previous_key_grace_replicas"`
	VerifiedReplicaReceipts     int64                                        `json:"verified_replica_receipts"`
	ReplicaReceiptDebt          int64                                        `json:"replica_receipt_debt"`
	Anchors                     ControlPlaneSIEMGovernanceDeliveryClassStats `json:"anchors"`
	Alerts                      ControlPlaneSIEMGovernanceDeliveryClassStats `json:"alerts"`
	Outstanding                 int64                                        `json:"outstanding"`
	Dead                        int64                                        `json:"dead"`
	OpenRemediations            int64                                        `json:"open_remediations"`
	PendingRemediationApprovals int64                                        `json:"pending_remediation_approvals"`
	OldestOutstandingSeconds    float64                                      `json:"oldest_outstanding_seconds"`
	MaxOutstanding              int                                          `json:"max_outstanding,omitempty"`
	MaxAgeSeconds               float64                                      `json:"max_age_seconds,omitempty"`
	Fenced                      bool                                         `json:"fenced"`
	FenceReason                 string                                       `json:"fence_reason,omitempty"`
	CheckedAt                   time.Time                                    `json:"checked_at"`
}

type ControlPlaneSIEMGovernanceReplicaReceipt struct {
	ID                   uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Kind                 string    `gorm:"size:16;not null;uniqueIndex:idx_governance_replica_receipt,priority:1;index" json:"kind"`
	TargetID             uint64    `gorm:"not null;uniqueIndex:idx_governance_replica_receipt,priority:2;index" json:"target_id"`
	ReplicaID            string    `gorm:"size:128;not null;uniqueIndex:idx_governance_replica_receipt,priority:3;index" json:"replica_id"`
	Identity             string    `gorm:"size:128;not null;index" json:"identity"`
	RemoteID             string    `gorm:"size:128;not null;default:''" json:"remote_id,omitempty"`
	ReceiptHash          string    `gorm:"size:64;not null;index" json:"receipt_hash"`
	ReceiptKeyID         string    `gorm:"size:128;not null;default:''" json:"receipt_key_id,omitempty"`
	ReceiptKeyGeneration uint64    `gorm:"not null;default:0;index" json:"receipt_key_generation,omitempty"`
	ReceiptKeyState      string    `gorm:"size:16;not null;default:'';index" json:"receipt_key_state,omitempty"`
	ReceiptAlgorithm     string    `gorm:"size:64;not null;default:''" json:"receipt_algorithm,omitempty"`
	ReceiptSignature     string    `gorm:"size:1024;not null;default:''" json:"receipt_signature,omitempty"`
	MembershipEpoch      uint64    `gorm:"not null;default:0;index" json:"membership_epoch,omitempty"`
	ReceivedAt           time.Time `gorm:"not null;index" json:"received_at"`
	CreatedAt            time.Time `gorm:"not null;index" json:"created_at"`
}

func (ControlPlaneSIEMGovernanceReplicaReceipt) TableName() string {
	return "control_plane_siem_governance_replica_receipts"
}

type ControlPlaneSIEMGovernanceReplicaKeyState struct {
	ReplicaID               string    `gorm:"primaryKey;size:128" json:"replica_id"`
	HighestActiveGeneration uint64    `gorm:"not null;default:0" json:"highest_active_generation"`
	ActiveKeyID             string    `gorm:"size:128;not null;default:''" json:"active_key_id,omitempty"`
	UpdatedAt               time.Time `gorm:"not null;index" json:"updated_at"`
}

func (ControlPlaneSIEMGovernanceReplicaKeyState) TableName() string {
	return "control_plane_siem_governance_replica_key_states"
}

type ControlPlaneSIEMGovernanceDeadItem struct {
	Kind       string    `json:"kind"`
	ID         uint64    `json:"id"`
	Term       uint64    `json:"term,omitempty"`
	IncidentID string    `json:"incident_id,omitempty"`
	Identity   string    `json:"identity"`
	Attempts   int       `json:"attempts"`
	LastError  string    `json:"last_error"`
	CreatedAt  time.Time `json:"created_at"`
}

type ControlPlaneSIEMGovernanceRemediation struct {
	ID             uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	OperationKey   *string    `gorm:"size:128;uniqueIndex" json:"operation_key,omitempty"`
	Kind           string     `gorm:"size:16;not null;index" json:"kind"`
	TargetID       uint64     `gorm:"not null;index" json:"target_id"`
	Action         string     `gorm:"size:32;not null;index" json:"action"`
	OperatorID     string     `gorm:"size:128;not null;index" json:"operator_id"`
	ApprovedBy     string     `gorm:"size:128;not null;default:'';index" json:"approved_by,omitempty"`
	ApprovalNote   string     `gorm:"size:1024;not null;default:''" json:"approval_note,omitempty"`
	ApprovedAt     *time.Time `gorm:"index" json:"approved_at,omitempty"`
	Reason         string     `gorm:"size:1024;not null" json:"reason"`
	PreviousState  string     `gorm:"size:16;not null" json:"previous_state"`
	PreviousError  string     `gorm:"size:1024;not null;default:''" json:"previous_error,omitempty"`
	TargetIdentity string     `gorm:"size:128;not null;default:''" json:"target_identity,omitempty"`
	State          string     `gorm:"size:16;not null;default:'open';index" json:"state"`
	Result         string     `gorm:"size:1024;not null;default:''" json:"result,omitempty"`
	RejectedBy     string     `gorm:"size:128;not null;default:'';index" json:"rejected_by,omitempty"`
	RejectionNote  string     `gorm:"size:1024;not null;default:''" json:"rejection_note,omitempty"`
	RejectedAt     *time.Time `gorm:"index" json:"rejected_at,omitempty"`
	CancelledBy    string     `gorm:"size:128;not null;default:'';index" json:"cancelled_by,omitempty"`
	CancelNote     string     `gorm:"size:1024;not null;default:''" json:"cancel_note,omitempty"`
	CancelledAt    *time.Time `gorm:"index" json:"cancelled_at,omitempty"`
	ExpiresAt      *time.Time `gorm:"index" json:"expires_at,omitempty"`
	ResolvedAt     *time.Time `gorm:"index" json:"resolved_at,omitempty"`
	CreatedAt      time.Time  `gorm:"not null;index" json:"created_at"`
}

func (ControlPlaneSIEMGovernanceRemediation) TableName() string {
	return "control_plane_siem_governance_remediations"
}

type ControlPlaneAuditDeliveryStats struct {
	Pending    int64 `json:"pending"`
	Delivering int64 `json:"delivering"`
	Delivered  int64 `json:"delivered"`
	Dead       int64 `json:"dead"`
	DLQ        int64 `json:"dlq"`
}
