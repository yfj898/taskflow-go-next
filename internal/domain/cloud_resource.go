package domain

import "time"

type CloudResourceDesiredState string

const (
	CloudResourceDesiredActive  CloudResourceDesiredState = "active"
	CloudResourceDesiredDeleted CloudResourceDesiredState = "deleted"
)

type CloudResourceActualState string

const (
	CloudResourcePending      CloudResourceActualState = "pending"
	CloudResourceProvisioning CloudResourceActualState = "provisioning"
	CloudResourceActive       CloudResourceActualState = "active"
	CloudResourceDeleting     CloudResourceActualState = "deleting"
	CloudResourceDeleted      CloudResourceActualState = "deleted"
	CloudResourceError        CloudResourceActualState = "error"
)

type CloudOrderType string

const (
	CloudOrderCreate CloudOrderType = "create"
	CloudOrderResize CloudOrderType = "resize"
	CloudOrderDelete CloudOrderType = "delete"
)

type CloudOrderStatus string

const (
	CloudOrderPending    CloudOrderStatus = "pending"
	CloudOrderRunning    CloudOrderStatus = "running"
	CloudOrderSucceeded  CloudOrderStatus = "succeeded"
	CloudOrderFailed     CloudOrderStatus = "failed"
	CloudOrderSuperseded CloudOrderStatus = "superseded"
)

type CloudOrchestrationMode string

const (
	CloudOrchestrationDirect CloudOrchestrationMode = "direct"
	CloudOrchestrationSaga   CloudOrchestrationMode = "saga"
)

// CloudResource is the durable desired/actual-state record for one managed
// resource. Generation is bumped for every user-visible mutation;
// ObservedGeneration is advanced only after the provider has converged.
type CloudResource struct {
	ID                  uint64                    `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantID            string                    `gorm:"size:128;not null;uniqueIndex:idx_cloud_resource_identity,priority:1" json:"tenant_id"`
	Name                string                    `gorm:"size:63;not null;uniqueIndex:idx_cloud_resource_identity,priority:3" json:"name"`
	Kind                string                    `gorm:"size:64;not null" json:"kind"`
	Provider            string                    `gorm:"size:32;not null;uniqueIndex:idx_cloud_resource_identity,priority:2;index" json:"provider"`
	OrchestrationMode   CloudOrchestrationMode    `gorm:"size:24;not null;default:direct;index" json:"orchestration_mode"`
	ProviderResourceID  string                    `gorm:"size:256" json:"provider_resource_id,omitempty"`
	DesiredSpec         string                    `gorm:"type:longtext;not null" json:"desired_spec"`
	ActualSpec          string                    `gorm:"type:longtext" json:"actual_spec,omitempty"`
	DesiredBundleSpec   string                    `gorm:"type:longtext" json:"desired_bundle_spec,omitempty"`
	ActualBundleSpec    string                    `gorm:"type:longtext" json:"actual_bundle_spec,omitempty"`
	DesiredState        CloudResourceDesiredState `gorm:"size:24;not null;index:idx_cloud_resource_reconcile,priority:2" json:"desired_state"`
	ActualState         CloudResourceActualState  `gorm:"size:24;not null;index:idx_cloud_resource_reconcile,priority:3" json:"actual_state"`
	Generation          uint64                    `gorm:"not null;default:1;index:idx_cloud_resource_reconcile,priority:1" json:"generation"`
	ObservedGeneration  uint64                    `gorm:"not null;default:0" json:"observed_generation"`
	QuotaCPUMilli       int64                     `gorm:"column:quota_cpu_milli;not null;default:0" json:"quota_cpu_milli"`
	QuotaMemoryMiB      int64                     `gorm:"column:quota_memory_mib;not null;default:0" json:"quota_memory_mib"`
	QuotaReplicas       int64                     `gorm:"not null;default:0" json:"quota_replicas"`
	QuotaBucketID       uint16                    `gorm:"column:quota_bucket_id;not null;default:0;index" json:"quota_bucket_id,omitempty"`
	ReconcileLeaseUntil *time.Time                `gorm:"index:idx_cloud_resource_reconcile_lease" json:"-"`
	LastReconcileAt     *time.Time                `json:"last_reconcile_at,omitempty"`
	LastError           string                    `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt           time.Time                 `json:"created_at"`
	UpdatedAt           time.Time                 `gorm:"index" json:"updated_at"`
}

func (CloudResource) TableName() string { return "cloud_resources" }

type CloudOrder struct {
	ID                  uint64           `gorm:"primaryKey;autoIncrement" json:"id"`
	ResourceID          uint64           `gorm:"not null;index:idx_cloud_order_resource_created,priority:1" json:"resource_id"`
	TenantID            string           `gorm:"size:128;not null;index;uniqueIndex:idx_cloud_orders_tenant_idempotency,priority:1" json:"tenant_id"`
	Type                CloudOrderType   `gorm:"size:24;not null" json:"type"`
	Status              CloudOrderStatus `gorm:"size:24;not null;index" json:"status"`
	IdempotencyKey      string           `gorm:"size:128;not null;uniqueIndex:idx_cloud_orders_tenant_idempotency,priority:2" json:"idempotency_key"`
	RequestedGeneration uint64           `gorm:"not null" json:"requested_generation"`
	RequestedSpec       string           `gorm:"type:longtext" json:"requested_spec,omitempty"`
	AttemptCount        int              `gorm:"not null;default:0" json:"attempt_count"`
	LastError           string           `gorm:"type:text" json:"last_error,omitempty"`
	LastAttemptAt       *time.Time       `json:"last_attempt_at,omitempty"`
	CompletedAt         *time.Time       `json:"completed_at,omitempty"`
	CreatedAt           time.Time        `gorm:"index:idx_cloud_order_resource_created,priority:2" json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
}

func (CloudOrder) TableName() string { return "cloud_orders" }

// CloudTenantQuota is a strongly consistent admission ledger. Used values
// represent quota currently charged to desired resources. Downscale/delete
// only release the charge after provider convergence so capacity is not
// accidentally overcommitted while the old workload still exists.
type CloudTenantQuota struct {
	TenantID       string    `gorm:"primaryKey;size:128" json:"tenant_id"`
	AccountingMode string    `gorm:"size:24;not null;default:single" json:"accounting_mode"`
	CPUMilliLimit  int64     `gorm:"column:cpu_milli_limit;not null" json:"cpu_milli_limit"`
	MemoryMiBLimit int64     `gorm:"column:memory_mib_limit;not null" json:"memory_mib_limit"`
	ReplicaLimit   int64     `gorm:"not null" json:"replica_limit"`
	CPUMilliUsed   int64     `gorm:"column:cpu_milli_used;not null;default:0" json:"cpu_milli_used"`
	MemoryMiBUsed  int64     `gorm:"column:memory_mib_used;not null;default:0" json:"memory_mib_used"`
	ReplicaUsed    int64     `gorm:"not null;default:0" json:"replica_used"`
	Version        uint64    `gorm:"not null;default:1" json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (CloudTenantQuota) TableName() string { return "cloud_tenant_quotas" }

// CloudTenantQuotaBucket partitions one tenant's hard quota into independent
// reservation rows. Bucket limits sum exactly to the tenant limit, so
// concurrent reservations can update different rows without weakening the
// global no-oversubscription invariant.
type CloudTenantQuotaBucket struct {
	TenantID       string    `gorm:"primaryKey;size:128" json:"tenant_id"`
	BucketID       uint16    `gorm:"primaryKey;column:bucket_id" json:"bucket_id"`
	CPUMilliLimit  int64     `gorm:"column:cpu_milli_limit;not null" json:"cpu_milli_limit"`
	MemoryMiBLimit int64     `gorm:"column:memory_mib_limit;not null" json:"memory_mib_limit"`
	ReplicaLimit   int64     `gorm:"not null" json:"replica_limit"`
	CPUMilliUsed   int64     `gorm:"column:cpu_milli_used;not null;default:0" json:"cpu_milli_used"`
	MemoryMiBUsed  int64     `gorm:"column:memory_mib_used;not null;default:0" json:"memory_mib_used"`
	ReplicaUsed    int64     `gorm:"not null;default:0" json:"replica_used"`
	Version        uint64    `gorm:"not null;default:1" json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (CloudTenantQuotaBucket) TableName() string { return "cloud_tenant_quota_buckets" }

type CloudWorkflowStatus string

const (
	CloudWorkflowPending      CloudWorkflowStatus = "pending"
	CloudWorkflowRunning      CloudWorkflowStatus = "running"
	CloudWorkflowCompensating CloudWorkflowStatus = "compensating"
	CloudWorkflowSucceeded    CloudWorkflowStatus = "succeeded"
	CloudWorkflowCompensated  CloudWorkflowStatus = "compensated"
)

type CloudWorkflowStepStatus string

const (
	CloudWorkflowStepPending      CloudWorkflowStepStatus = "pending"
	CloudWorkflowStepRunning      CloudWorkflowStepStatus = "running"
	CloudWorkflowStepSucceeded    CloudWorkflowStepStatus = "succeeded"
	CloudWorkflowStepFailed       CloudWorkflowStepStatus = "failed"
	CloudWorkflowStepCompensating CloudWorkflowStepStatus = "compensating"
	CloudWorkflowStepCompensated  CloudWorkflowStepStatus = "compensated"
	CloudWorkflowStepSkipped      CloudWorkflowStepStatus = "skipped"
)

const (
	CloudWorkflowStepQuota   = "quota"
	CloudWorkflowStepNetwork = "network"
	CloudWorkflowStepStorage = "storage"
	CloudWorkflowStepCompute = "compute"
	CloudWorkflowStepCommit  = "commit"
)

const (
	CloudWorkflowKindProvision = "provision_bundle"
	CloudWorkflowKindResize    = "resize_bundle"
	CloudWorkflowKindDelete    = "delete_bundle"
)

// CloudWorkflow is the durable Saga coordinator for a multi-resource order.
// A lease lets multiple API/controller replicas safely scan the same table;
// step state is persisted independently so a new process can resume after a
// crash without replaying already-completed side effects blindly.
type CloudWorkflow struct {
	ID               uint64              `gorm:"primaryKey;autoIncrement" json:"id"`
	ResourceID       uint64              `gorm:"not null;index:idx_cloud_workflow_resource_created,priority:1" json:"resource_id"`
	OrderID          uint64              `gorm:"not null;uniqueIndex:ux_cloud_workflow_order" json:"order_id"`
	TenantID         string              `gorm:"size:128;not null;index" json:"tenant_id"`
	Kind             string              `gorm:"size:48;not null" json:"kind"`
	Status           CloudWorkflowStatus `gorm:"size:24;not null;index:idx_cloud_workflow_runnable,priority:1" json:"status"`
	Spec             string              `gorm:"type:longtext;not null" json:"spec"`
	PreviousSpec     string              `gorm:"type:longtext" json:"previous_spec,omitempty"`
	FailureReason    string              `gorm:"type:text" json:"failure_reason,omitempty"`
	LeaseUntil       *time.Time          `gorm:"index:idx_cloud_workflow_runnable,priority:2" json:"-"`
	NextReconcileAt  *time.Time          `gorm:"index:idx_cloud_workflow_next_reconcile" json:"next_reconcile_at,omitempty"`
	LastTransitionAt *time.Time          `json:"last_transition_at,omitempty"`
	CompletedAt      *time.Time          `json:"completed_at,omitempty"`
	CreatedAt        time.Time           `gorm:"index:idx_cloud_workflow_resource_created,priority:2" json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
}

func (CloudWorkflow) TableName() string { return "cloud_workflows" }

type CloudWorkflowStep struct {
	ID                       uint64                  `gorm:"primaryKey;autoIncrement" json:"id"`
	WorkflowID               uint64                  `gorm:"not null;uniqueIndex:idx_cloud_workflow_step,priority:1;index" json:"workflow_id"`
	Name                     string                  `gorm:"size:48;not null;uniqueIndex:idx_cloud_workflow_step,priority:2" json:"name"`
	Position                 int                     `gorm:"not null;index" json:"position"`
	Status                   CloudWorkflowStepStatus `gorm:"size:24;not null;index" json:"status"`
	AttemptCount             int                     `gorm:"not null;default:0" json:"attempt_count"`
	CompensationAttemptCount int                     `gorm:"not null;default:0" json:"compensation_attempt_count"`
	Output                   string                  `gorm:"type:longtext" json:"output,omitempty"`
	LastError                string                  `gorm:"type:text" json:"last_error,omitempty"`
	StartedAt                *time.Time              `json:"started_at,omitempty"`
	CompletedAt              *time.Time              `json:"completed_at,omitempty"`
	CreatedAt                time.Time               `json:"created_at"`
	UpdatedAt                time.Time               `json:"updated_at"`
}

func (CloudWorkflowStep) TableName() string { return "cloud_workflow_steps" }
