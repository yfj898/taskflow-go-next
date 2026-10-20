package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr                  string
	PprofAddr                 string
	MySQLDSN                  string
	MySQLFailoverDSNs         string
	MySQLConnectTimeoutMS     int
	MySQLMaxOpenConns         int
	MySQLMaxIdleConns         int
	MySQLSkipDefaultTx        bool
	RedisAddr                 string
	RedisPassword             string
	RedisDB                   int
	RedisQueueKey             string
	QueueStatsIntervalMS      int
	QueueCompactionEnabled    bool
	QueueCompactionIntervalMS int
	QueueAckRetentionMS       int
	QueueAckRetentionMax      int
	QueueDLQReplayLeaseMS     int
	DLQLifecycleEnabled       bool
	DLQLifecycleIntervalMS    int
	DLQReplayedRetentionMS    int
	DLQQuarantineRetentionMS  int
	DLQAuditRetentionMS       int
	DLQMaxEntries             int
	DLQArchiveEnabled         bool
	DLQArchiveSink            string
	DLQArchiveDir             string
	DLQArchiveLeaseMS         int
	DLQArchiveHTTPBaseURL     string
	DLQArchiveHTTPToken       string
	DLQArchiveHTTPTimeoutMS   int
	DLQArchiveS3Endpoint      string
	DLQArchiveS3AccessKey     string
	DLQArchiveS3SecretKey     string
	DLQArchiveS3Bucket        string
	DLQArchiveS3Region        string
	DLQArchiveS3Secure        bool
	DLQReplicaEnabled         bool
	DLQReplicaSink            string
	DLQReplicaDir             string
	DLQReplicaHTTPBaseURL     string
	DLQReplicaHTTPToken       string
	DLQReplicaHTTPTimeoutMS   int
	DLQReplicaS3Endpoint      string
	DLQReplicaS3AccessKey     string
	DLQReplicaS3SecretKey     string
	DLQReplicaS3Bucket        string
	DLQReplicaS3Region        string
	DLQReplicaS3Secure        bool
	DLQArchiveMinCopies       int
	DLQObjLifecycleEnabled    bool
	DLQObjLifecycleIntervalMS int
	DLQObjRetentionMS         int
	DLQOrphanGraceMS          int
	DLQObjGCBatch             int
	DLQOrphanBatch            int
	DLQDrillIntervalMS        int
	DLQDrillBatch             int
	QueueBackend              string
	KafkaBrokers              string
	KafkaTopic                string
	KafkaGroup                string
	ExecutionBackend          string
	ExecutorGRPCTarget        string
	ExecutorGRPCListen        string
	ExecutorMetricsAddr       string
	ExecutorTimeoutMS         int
	CircuitFailures           int
	CircuitOpenMS             int
	AIProvider                string
	AIProviderKind            string
	AIEndpoint                string
	AIBearerToken             string
	AIModel                   string
	AICapabilities            string
	AIInputCostPer1M          float64
	AIOutputCostPer1M         float64
	AIMaxConcurrency          int
	AIGlobalConcurrencyLimit  int
	AIFallbackProvider        string
	AIFallbackKind            string
	AIFallbackEndpoint        string
	AIFallbackToken           string
	AIFallbackModel           string
	AIFallbackCaps            string
	AIFallbackInputCostPer1M  float64
	AIFallbackOutputCostPer1M float64
	AIFallbackMaxConcurrency  int
	AIFallbackGlobalLimit     int
	AIRoutingStrategy         string
	AIProviderCircuitFailures int
	AIProviderCircuitOpenMS   int
	AIDefaultOutputTokens     int
	AISLORealtimeQueueMS      int
	AISLORealtimeTimeoutMS    int
	AISLOStandardQueueMS      int
	AISLOStandardTimeoutMS    int
	AISLOBatchQueueMS         int
	AISLOBatchTimeoutMS       int
	AITenantFairScheduling    bool
	AITenantMaxConsecutive    int
	AITenantStarvationMS      int
	AITenantQuotaEnabled      bool
	AITenantQuotaLimit        int
	AITenantQuotaOverrides    string
	AIDistributedLeaseMS      int
	AIConcurrencyRedisPrefix  string
	AIAdaptiveLoadShedding    bool
	AIPressureQueueWarn       int
	AIPressureQueueHigh       int
	AIPressureFailWarn        float64
	AIPressureFailHigh        float64
	AIPressureLatencyWarnMS   int
	AIPressureLatencyHighMS   int
	AIHedgeEnabled            bool
	AIHedgeDelayMS            int
	AIHedgeCancelWaitMS       int
	AIHedgeMaxExtraCostUSD    float64
	AIBudgetDefaultReserveUSD float64
	AITimeoutMS               int
	ControlPlaneAuthEnabled   bool
	ControlPlaneBearerToken   string
	ControlPlanePrevToken     string
	ControlPlaneAuthMode      string
	ControlPlaneOIDCIssuer    string
	ControlPlaneOIDCAudience  string
	ControlPlaneOIDCJWKSURL   string
	ControlPlaneOIDCGroups    string
	ControlPlaneOIDCRoleMap   string
	ControlPlaneOIDCJWKSTTLMS int
	ControlPlaneOIDCSkewMS    int
	ControlPlaneOIDCRBAC      bool
	CloudTenantAuthzEnabled   bool

	CloudWorkflowSchemaManaged bool

	ControlPlaneExtraCredentials string

	ControlPlaneOperatorID    string
	ControlPlanePreviousID    string
	ControlPlaneAuditEnabled  bool
	ControlAuditRetention     bool
	ControlAuditLifecycleMS   int
	ControlAuditRetentionMS   int
	ControlAuditRetainBatch   int
	ControlSIEMEnabled        bool
	ControlSIEMEndpoint       string
	ControlSIEMToken          string
	ControlSIEMSignKey        string
	ControlSIEMSignKeyID      string
	ControlSIEMIntervalMS     int
	ControlSIEMBatch          int
	ControlSIEMLeaseMS        int
	ControlSIEMMaxAttempts    int
	ControlSIEMRetryBaseMS    int
	ControlSIEMRetryMaxMS     int
	ControlSIEMHTTPTimeoutMS  int
	SIEMSignProvider          string
	SIEMPrevSignKey           string
	SIEMPrevSignKeyID         string
	SIEMRemoteEndpoint        string
	SIEMRemoteToken           string
	SIEMRemoteAlgo            string
	SIEMRemoteTimeoutMS       int
	SIEMAWSKMSEndpoint        string
	SIEMAWSKMSRegion          string
	SIEMAWSKMSAccessKey       string
	SIEMAWSKMSSecretKey       string
	SIEMAWSKMSSessionToken    string
	SIEMAWSKMSKeyID           string
	SIEMAWSKMSTimeoutMS       int
	SIEMAWSCredentialsSource  string
	SIEMAWSWebIdentityFile    string
	SIEMAWSWebIdentityRoleARN string
	SIEMAWSWebIdentitySession string
	SIEMAWSSTSEndpoint        string
	SIEMAWSIMDSEndpoint       string
	SIEMAWSIMDSTimeoutMS      int
	SIEMVaultEndpoint         string
	SIEMVaultToken            string
	SIEMVaultNamespace        string
	SIEMVaultMount            string
	SIEMVaultKeyName          string
	SIEMVaultKeyVersion       int
	SIEMVaultTimeoutMS        int
	SIEMCoordEnabled          bool
	SIEMCoordKey              string
	SIEMCoordLeaseMS          int
	SIEMGCEnabled             bool
	SIEMGCIntervalMS          int
	SIEMDeliveredKeepMS       int
	SIEMDLQKeepMS             int
	SIEMGCBatch               int
	SIEMDREnabled             bool
	SIEMDRMinCopies           int
	SIEMDRPrimaryEndpoint     string
	SIEMDRPrimaryAccessKey    string
	SIEMDRPrimarySecretKey    string
	SIEMDRPrimaryBucket       string
	SIEMDRPrimaryRegion       string
	SIEMDRPrimarySecure       bool
	SIEMDRReplicaEnabled      bool
	SIEMDRReplicaEndpoint     string
	SIEMDRReplicaAccessKey    string
	SIEMDRReplicaSecretKey    string
	SIEMDRReplicaBucket       string
	SIEMDRReplicaRegion       string
	SIEMDRReplicaSecure       bool
	SIEMReconcileEnabled      bool
	SIEMReconcileEndpoint     string
	SIEMReconcileTimeoutMS    int
	SIEMReconcilePageSize     int
	SIEMReconcileMinEvidence  int
	SIEMReconcileReceiverID   string
	SIEMPrimaryReceiverID     string
	SIEMPrimaryCheckpoint     string
	SIEMSecondaryEndpoint     string
	SIEMSecondaryReceiverID   string
	SIEMSecondaryCheckpoint   string
	SIEMFailoverEnabled       bool
	SIEMShadowSecondary       bool
	SIEMCheckpointHMACKey     string
	SIEMCheckpointHMACKeyID   string
	SIEMClusterGuardEnabled   bool
	SIEMTermFencingEnabled    bool
	SIEMPrimaryTermEndpoint   string
	SIEMSecondaryTermEndpoint string
	SIEMAntiEntropyEnabled    bool
	SIEMAntiEntropyIntervalMS int
	SIEMPromotionWorkflow     bool
	SIEMPromotionIntentTTLMS  int
	SIEMPromotionLeaseMS      int

	SIEMPromotionApprovalQuorum                     int
	SIEMPromotionRolePolicy                         string
	SIEMBreakGlassEnabled                           bool
	SIEMGovernanceEndpoint                          string
	SIEMGovernanceToken                             string
	SIEMGovernanceSignKey                           string
	SIEMGovernanceSignKeyID                         string
	SIEMGovernanceTimeoutMS                         int
	SIEMGovernanceHAEnabled                         bool
	SIEMGovernancePrimaryReplicaID                  string
	SIEMGovernancePrimaryReceiptKey                 string
	SIEMGovernancePrimaryReceiptKeyID               string
	SIEMGovernancePrimaryReceiptKeyGeneration       int
	SIEMGovernancePrimaryPrevReceiptKey             string
	SIEMGovernancePrimaryPrevReceiptKeyID           string
	SIEMGovernancePrimaryPrevReceiptKeyGeneration   int
	SIEMGovernancePrimaryPrevReceiptValidUntilMS    int64
	SIEMGovernanceSecondaryEndpoint                 string
	SIEMGovernanceSecondaryToken                    string
	SIEMGovernanceSecondaryReplicaID                string
	SIEMGovernanceSecondaryReceiptKey               string
	SIEMGovernanceSecondaryReceiptKeyID             string
	SIEMGovernanceSecondaryReceiptKeyGeneration     int
	SIEMGovernanceSecondaryPrevReceiptKey           string
	SIEMGovernanceSecondaryPrevReceiptKeyID         string
	SIEMGovernanceSecondaryPrevReceiptKeyGeneration int
	SIEMGovernanceSecondaryPrevReceiptValidUntilMS  int64
	SIEMGovernanceReceiptQuorum                     int
	SIEMGovernanceMembershipEpoch                   uint64
	SIEMGovernanceDebtPolicy                        bool
	SIEMGovernanceMaxOutstanding                    int
	SIEMGovernanceMaxAgeMS                          int
	SIEMGovernanceRemediationApproval               bool
	SIEMGovernanceRemediationTTLMS                  int
	SIEMGovernanceStaleTargetFence                  bool
	SIEMRTOAlertThresholdMS                         int

	SIEMRPOPolicyEnabled bool
	SIEMRPOMaxSequences  int
	SIEMRPOGraceMS       int

	AccessLog            bool
	OTelLogSpans         bool
	OutboxIntervalMS     int
	WorkerCount          int
	MaxRetries           int
	ReconcileSec         int
	ReconcileDistributed bool
	ReconcileLockKey     string
	ReconcileLeaseMS     int
	EnqueueLeaseMS       int
	StaleRunSec          int
	RateLimitRPS         int
	RateLimitBurst       int
	DrainFenceMS         int
	DrainTimeoutMS       int
}

func Load() Config {
	return Config{
		HTTPAddr:                  getenv("HTTP_ADDR", ":8080"),
		PprofAddr:                 getenv("PPROF_ADDR", "127.0.0.1:6060"),
		MySQLDSN:                  getenv("MYSQL_DSN", "taskflow:taskflow@tcp(127.0.0.1:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local"),
		MySQLFailoverDSNs:         getenv("MYSQL_FAILOVER_DSNS", ""),
		MySQLConnectTimeoutMS:     getenvInt("MYSQL_CONNECT_TIMEOUT_MS", 3000),
		MySQLMaxOpenConns:         getenvInt("MYSQL_MAX_OPEN_CONNS", 50),
		MySQLMaxIdleConns:         getenvInt("MYSQL_MAX_IDLE_CONNS", 10),
		MySQLSkipDefaultTx:        getenvBool("MYSQL_SKIP_DEFAULT_TX", true),
		RedisAddr:                 getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:             getenv("REDIS_PASSWORD", ""),
		RedisDB:                   getenvInt("REDIS_DB", 0),
		RedisQueueKey:             getenv("REDIS_QUEUE_KEY", "taskflow:queue"),
		QueueStatsIntervalMS:      getenvInt("QUEUE_STATS_INTERVAL_MS", 5000),
		QueueCompactionEnabled:    getenvBool("QUEUE_COMPACTION_ENABLED", true),
		QueueCompactionIntervalMS: getenvInt("QUEUE_COMPACTION_INTERVAL_MS", 30000),
		QueueAckRetentionMS:       getenvNonNegativeInt("QUEUE_ACK_RETENTION_MS", 86400000),
		QueueAckRetentionMax:      getenvNonNegativeInt("QUEUE_ACK_RETENTION_MAX_ENTRIES", 100000),
		QueueDLQReplayLeaseMS:     getenvInt("QUEUE_DLQ_REPLAY_LEASE_MS", 30000),
		DLQLifecycleEnabled:       getenvBool("QUEUE_DLQ_LIFECYCLE_ENABLED", true),
		DLQLifecycleIntervalMS:    getenvInt("QUEUE_DLQ_LIFECYCLE_INTERVAL_MS", 60000),
		DLQReplayedRetentionMS:    getenvNonNegativeInt("QUEUE_DLQ_REPLAYED_RETENTION_MS", 604800000),
		DLQQuarantineRetentionMS:  getenvNonNegativeInt("QUEUE_DLQ_QUARANTINED_RETENTION_MS", 2592000000),
		DLQAuditRetentionMS:       getenvNonNegativeInt("QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS", 7776000000),
		DLQMaxEntries:             getenvNonNegativeInt("QUEUE_DLQ_MAX_ENTRIES", 100000),
		DLQArchiveEnabled:         getenvBool("QUEUE_DLQ_ARCHIVE_ENABLED", true),
		DLQArchiveSink:            getenv("QUEUE_DLQ_ARCHIVE_SINK", "file"),
		DLQArchiveDir:             getenv("QUEUE_DLQ_ARCHIVE_DIR", "./data/dlq-archive"),
		DLQArchiveLeaseMS:         getenvInt("QUEUE_DLQ_ARCHIVE_LEASE_MS", 30000),
		DLQArchiveHTTPBaseURL:     getenv("QUEUE_DLQ_ARCHIVE_HTTP_BASE_URL", ""),
		DLQArchiveHTTPToken:       getenv("QUEUE_DLQ_ARCHIVE_HTTP_BEARER_TOKEN", ""),
		DLQArchiveHTTPTimeoutMS:   getenvInt("QUEUE_DLQ_ARCHIVE_HTTP_TIMEOUT_MS", 10000),
		DLQArchiveS3Endpoint:      getenv("QUEUE_DLQ_ARCHIVE_S3_ENDPOINT", ""),
		DLQArchiveS3AccessKey:     getenv("QUEUE_DLQ_ARCHIVE_S3_ACCESS_KEY", ""),
		DLQArchiveS3SecretKey:     getenv("QUEUE_DLQ_ARCHIVE_S3_SECRET_KEY", ""),
		DLQArchiveS3Bucket:        getenv("QUEUE_DLQ_ARCHIVE_S3_BUCKET", "taskflow-dlq"),
		DLQArchiveS3Region:        getenv("QUEUE_DLQ_ARCHIVE_S3_REGION", "us-east-1"),
		DLQArchiveS3Secure:        getenvBool("QUEUE_DLQ_ARCHIVE_S3_SECURE", true),
		DLQReplicaEnabled:         getenvBool("QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED", false),
		DLQReplicaSink:            getenv("QUEUE_DLQ_ARCHIVE_REPLICA_SINK", "file"),
		DLQReplicaDir:             getenv("QUEUE_DLQ_ARCHIVE_REPLICA_DIR", "./data/dlq-archive-replica"),
		DLQReplicaHTTPBaseURL:     getenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BASE_URL", ""),
		DLQReplicaHTTPToken:       getenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BEARER_TOKEN", ""),
		DLQReplicaHTTPTimeoutMS:   getenvInt("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_TIMEOUT_MS", 10000),
		DLQReplicaS3Endpoint:      getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT", ""),
		DLQReplicaS3AccessKey:     getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ACCESS_KEY", ""),
		DLQReplicaS3SecretKey:     getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECRET_KEY", ""),
		DLQReplicaS3Bucket:        getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_BUCKET", "taskflow-dlq"),
		DLQReplicaS3Region:        getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_REGION", "us-east-1"),
		DLQReplicaS3Secure:        getenvBool("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECURE", true),
		DLQArchiveMinCopies:       getenvInt("QUEUE_DLQ_ARCHIVE_MIN_COPIES", 1),
		DLQObjLifecycleEnabled:    getenvBool("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED", true),
		DLQObjLifecycleIntervalMS: getenvInt("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_INTERVAL_MS", 3600000),
		DLQObjRetentionMS:         getenvNonNegativeInt("QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS", 31536000000),
		DLQOrphanGraceMS:          getenvNonNegativeInt("QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS", 300000),
		DLQObjGCBatch:             getenvInt("QUEUE_DLQ_ARCHIVE_GC_BATCH", 50),
		DLQOrphanBatch:            getenvInt("QUEUE_DLQ_ARCHIVE_ORPHAN_BATCH", 100),
		DLQDrillIntervalMS:        getenvNonNegativeInt("QUEUE_DLQ_ARCHIVE_DRILL_INTERVAL_MS", 86400000),
		DLQDrillBatch:             getenvInt("QUEUE_DLQ_ARCHIVE_DRILL_BATCH", 3),
		QueueBackend:              getenv("QUEUE_BACKEND", "redis"),
		KafkaBrokers:              getenv("KAFKA_BROKERS", "127.0.0.1:9092"),
		KafkaTopic:                getenv("KAFKA_TOPIC", "taskflow.tasks"),
		KafkaGroup:                getenv("KAFKA_GROUP", "taskflow-workers"),
		ExecutionBackend:          getenv("EXECUTION_BACKEND", "local"),
		ExecutorGRPCTarget:        getenv("EXECUTOR_GRPC_TARGET", "127.0.0.1:9091"),
		ExecutorGRPCListen:        getenv("EXECUTOR_GRPC_LISTEN", ":9091"),
		ExecutorMetricsAddr:       getenv("EXECUTOR_METRICS_ADDR", ":9093"),
		ExecutorTimeoutMS:         getenvInt("EXECUTOR_TIMEOUT_MS", 5000),
		CircuitFailures:           getenvInt("CIRCUIT_FAILURES", 5),
		CircuitOpenMS:             getenvInt("CIRCUIT_OPEN_MS", 5000),
		AIProvider:                getenv("AI_PROVIDER", "primary"),
		AIProviderKind:            getenv("AI_PROVIDER_KIND", "gateway"),
		AIEndpoint:                getenv("AI_ENDPOINT", ""),
		AIBearerToken:             getenv("AI_BEARER_TOKEN", ""),
		AIModel:                   getenv("AI_MODEL", ""),
		AICapabilities:            getenv("AI_CAPABILITIES", ""),
		AIInputCostPer1M:          getenvFloat("AI_INPUT_COST_PER_1M", 0),
		AIOutputCostPer1M:         getenvFloat("AI_OUTPUT_COST_PER_1M", 0),
		AIMaxConcurrency:          getenvInt("AI_MAX_CONCURRENCY", 4),
		AIGlobalConcurrencyLimit:  getenvNonNegativeInt("AI_GLOBAL_MAX_CONCURRENCY", 0),
		AIFallbackProvider:        getenv("AI_FALLBACK_PROVIDER", "fallback"),
		AIFallbackKind:            getenv("AI_FALLBACK_KIND", "gateway"),
		AIFallbackEndpoint:        getenv("AI_FALLBACK_ENDPOINT", ""),
		AIFallbackToken:           getenv("AI_FALLBACK_BEARER_TOKEN", ""),
		AIFallbackModel:           getenv("AI_FALLBACK_MODEL", ""),
		AIFallbackCaps:            getenv("AI_FALLBACK_CAPABILITIES", ""),
		AIFallbackInputCostPer1M:  getenvFloat("AI_FALLBACK_INPUT_COST_PER_1M", 0),
		AIFallbackOutputCostPer1M: getenvFloat("AI_FALLBACK_OUTPUT_COST_PER_1M", 0),
		AIFallbackMaxConcurrency:  getenvInt("AI_FALLBACK_MAX_CONCURRENCY", 4),
		AIFallbackGlobalLimit:     getenvNonNegativeInt("AI_FALLBACK_GLOBAL_MAX_CONCURRENCY", 0),
		AIRoutingStrategy:         getenv("AI_ROUTING_STRATEGY", "balanced"),
		AIProviderCircuitFailures: getenvInt("AI_PROVIDER_CIRCUIT_FAILURES", 3),
		AIProviderCircuitOpenMS:   getenvInt("AI_PROVIDER_CIRCUIT_OPEN_MS", 10000),
		AIDefaultOutputTokens:     getenvInt("AI_DEFAULT_OUTPUT_TOKENS", 256),
		AISLORealtimeQueueMS:      getenvInt("AI_SLO_REALTIME_QUEUE_MS", 50),
		AISLORealtimeTimeoutMS:    getenvInt("AI_SLO_REALTIME_TIMEOUT_MS", 3000),
		AISLOStandardQueueMS:      getenvInt("AI_SLO_STANDARD_QUEUE_MS", 1000),
		AISLOStandardTimeoutMS:    getenvInt("AI_SLO_STANDARD_TIMEOUT_MS", 15000),
		AISLOBatchQueueMS:         getenvInt("AI_SLO_BATCH_QUEUE_MS", 5000),
		AISLOBatchTimeoutMS:       getenvInt("AI_SLO_BATCH_TIMEOUT_MS", 30000),
		AITenantFairScheduling:    getenvBool("AI_TENANT_FAIR_SCHEDULING", false),
		AITenantMaxConsecutive:    getenvInt("AI_TENANT_MAX_CONSECUTIVE", 2),
		AITenantStarvationMS:      getenvInt("AI_TENANT_STARVATION_MS", 5000),
		AITenantQuotaEnabled:      getenvBool("AI_DISTRIBUTED_TENANT_QUOTA", false),
		AITenantQuotaLimit:        getenvNonNegativeInt("AI_TENANT_GLOBAL_MAX_CONCURRENCY", 0),
		AITenantQuotaOverrides:    getenv("AI_TENANT_CONCURRENCY_OVERRIDES", ""),
		AIDistributedLeaseMS:      getenvInt("AI_DISTRIBUTED_LEASE_MS", 30000),
		AIConcurrencyRedisPrefix:  getenv("AI_CONCURRENCY_REDIS_PREFIX", "taskflow:ai:concurrency"),
		AIAdaptiveLoadShedding:    getenvBool("AI_ADAPTIVE_LOAD_SHEDDING", true),
		AIPressureQueueWarn:       getenvInt("AI_PRESSURE_DEGRADED_QUEUE", 4),
		AIPressureQueueHigh:       getenvInt("AI_PRESSURE_OVERLOADED_QUEUE", 16),
		AIPressureFailWarn:        getenvFloat("AI_PRESSURE_DEGRADED_FAILURE_RATE", 0.20),
		AIPressureFailHigh:        getenvFloat("AI_PRESSURE_OVERLOADED_FAILURE_RATE", 0.50),
		AIPressureLatencyWarnMS:   getenvInt("AI_PRESSURE_DEGRADED_LATENCY_MS", 2000),
		AIPressureLatencyHighMS:   getenvInt("AI_PRESSURE_OVERLOADED_LATENCY_MS", 5000),
		AIHedgeEnabled:            getenvBool("AI_HEDGE_ENABLED", false),
		AIHedgeDelayMS:            getenvInt("AI_HEDGE_DELAY_MS", 100),
		AIHedgeCancelWaitMS:       getenvInt("AI_HEDGE_CANCEL_WAIT_MS", 100),
		AIHedgeMaxExtraCostUSD:    getenvFloat("AI_HEDGE_MAX_EXTRA_COST_USD", 0.01),
		AIBudgetDefaultReserveUSD: getenvFloat("AI_BUDGET_DEFAULT_RESERVE_USD", 0.01),
		AITimeoutMS:               getenvInt("AI_TIMEOUT_MS", 30000),
		ControlPlaneAuthEnabled:   getenvBool("CONTROL_PLANE_AUTH_ENABLED", false),
		ControlPlaneBearerToken:   getenv("CONTROL_PLANE_BEARER_TOKEN", ""),
		ControlPlanePrevToken:     getenv("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN", ""),
		ControlPlaneAuthMode:      getenv("CONTROL_PLANE_AUTH_MODE", "bearer"),
		ControlPlaneOIDCIssuer:    getenv("CONTROL_PLANE_OIDC_ISSUER", ""),
		ControlPlaneOIDCAudience:  getenv("CONTROL_PLANE_OIDC_AUDIENCE", ""),
		ControlPlaneOIDCJWKSURL:   getenv("CONTROL_PLANE_OIDC_JWKS_URL", ""),
		ControlPlaneOIDCGroups:    getenv("CONTROL_PLANE_OIDC_GROUPS_CLAIM", "groups"),
		ControlPlaneOIDCRoleMap:   getenv("CONTROL_PLANE_OIDC_ROLE_MAPPINGS", ""),
		ControlPlaneOIDCJWKSTTLMS: getenvInt("CONTROL_PLANE_OIDC_JWKS_CACHE_TTL_MS", 300000),
		ControlPlaneOIDCSkewMS:    getenvInt("CONTROL_PLANE_OIDC_CLOCK_SKEW_MS", 30000),
		ControlPlaneOIDCRBAC:      getenvBool("CONTROL_PLANE_OIDC_RBAC_ENABLED", false),
		CloudTenantAuthzEnabled:   getenvBool("CLOUD_TENANT_AUTHZ_ENABLED", false),

		CloudWorkflowSchemaManaged: getenvBool("CLOUD_WORKFLOW_SCHEMA_MANAGED", false),

		ControlPlaneExtraCredentials: getenv("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS", ""),

		ControlPlaneOperatorID:    getenv("CONTROL_PLANE_OPERATOR_ID", "operator"),
		ControlPlanePreviousID:    getenv("CONTROL_PLANE_PREVIOUS_OPERATOR_ID", ""),
		ControlPlaneAuditEnabled:  getenvBool("CONTROL_PLANE_AUDIT_ENABLED", true),
		ControlAuditRetention:     getenvBool("CONTROL_PLANE_AUDIT_RETENTION_ENABLED", true),
		ControlAuditLifecycleMS:   getenvInt("CONTROL_PLANE_AUDIT_LIFECYCLE_INTERVAL_MS", 3600000),
		ControlAuditRetentionMS:   getenvInt("CONTROL_PLANE_AUDIT_RETENTION_MS", 7776000000),
		ControlAuditRetainBatch:   getenvInt("CONTROL_PLANE_AUDIT_RETENTION_BATCH", 1000),
		ControlSIEMEnabled:        getenvBool("CONTROL_PLANE_SIEM_DELIVERY_ENABLED", false),
		ControlSIEMEndpoint:       getenv("CONTROL_PLANE_SIEM_ENDPOINT", ""),
		ControlSIEMToken:          getenv("CONTROL_PLANE_SIEM_BEARER_TOKEN", ""),
		ControlSIEMSignKey:        getenv("CONTROL_PLANE_SIEM_SIGNING_KEY", ""),
		ControlSIEMSignKeyID:      getenv("CONTROL_PLANE_SIEM_SIGNING_KEY_ID", "taskflow-hmac-v1"),
		ControlSIEMIntervalMS:     getenvInt("CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS", 1000),
		ControlSIEMBatch:          getenvInt("CONTROL_PLANE_SIEM_DELIVERY_BATCH", 100),
		ControlSIEMLeaseMS:        getenvInt("CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS", 30000),
		ControlSIEMMaxAttempts:    getenvInt("CONTROL_PLANE_SIEM_MAX_ATTEMPTS", 8),
		ControlSIEMRetryBaseMS:    getenvInt("CONTROL_PLANE_SIEM_RETRY_BASE_MS", 1000),
		ControlSIEMRetryMaxMS:     getenvInt("CONTROL_PLANE_SIEM_RETRY_MAX_MS", 60000),
		ControlSIEMHTTPTimeoutMS:  getenvInt("CONTROL_PLANE_SIEM_HTTP_TIMEOUT_MS", 5000),
		SIEMSignProvider:          getenv("CONTROL_PLANE_SIEM_SIGNING_PROVIDER", "hmac"),
		SIEMPrevSignKey:           getenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY", ""),
		SIEMPrevSignKeyID:         getenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID", ""),
		SIEMRemoteEndpoint:        getenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT", ""),
		SIEMRemoteToken:           getenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN", ""),
		SIEMRemoteAlgo:            getenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM", "hmac-sha256-digest"),
		SIEMRemoteTimeoutMS:       getenvInt("CONTROL_PLANE_SIEM_REMOTE_SIGNER_TIMEOUT_MS", 3000),
		SIEMAWSKMSEndpoint:        getenv("CONTROL_PLANE_SIEM_AWS_KMS_ENDPOINT", ""),
		SIEMAWSKMSRegion:          getenv("CONTROL_PLANE_SIEM_AWS_KMS_REGION", ""),
		SIEMAWSKMSAccessKey:       getenv("CONTROL_PLANE_SIEM_AWS_ACCESS_KEY_ID", ""),
		SIEMAWSKMSSecretKey:       getenv("CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY", ""),
		SIEMAWSKMSSessionToken:    getenv("CONTROL_PLANE_SIEM_AWS_SESSION_TOKEN", ""),
		SIEMAWSKMSKeyID:           getenv("CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID", ""),
		SIEMAWSKMSTimeoutMS:       getenvInt("CONTROL_PLANE_SIEM_AWS_KMS_TIMEOUT_MS", 5000),
		SIEMAWSCredentialsSource:  getenv("CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE", "static"),
		SIEMAWSWebIdentityFile:    getenv("AWS_WEB_IDENTITY_TOKEN_FILE", ""),
		SIEMAWSWebIdentityRoleARN: getenv("AWS_ROLE_ARN", ""),
		SIEMAWSWebIdentitySession: getenv("AWS_ROLE_SESSION_NAME", "taskflow-siem"),
		SIEMAWSSTSEndpoint:        getenv("CONTROL_PLANE_SIEM_AWS_STS_ENDPOINT", "https://sts.amazonaws.com"),
		SIEMAWSIMDSEndpoint:       getenv("CONTROL_PLANE_SIEM_AWS_IMDS_ENDPOINT", "http://169.254.169.254"),
		SIEMAWSIMDSTimeoutMS:      getenvInt("CONTROL_PLANE_SIEM_AWS_IMDS_TIMEOUT_MS", 1000),
		SIEMVaultEndpoint:         getenv("CONTROL_PLANE_SIEM_VAULT_ENDPOINT", ""),
		SIEMVaultToken:            getenv("CONTROL_PLANE_SIEM_VAULT_TOKEN", ""),
		SIEMVaultNamespace:        getenv("CONTROL_PLANE_SIEM_VAULT_NAMESPACE", ""),
		SIEMVaultMount:            getenv("CONTROL_PLANE_SIEM_VAULT_TRANSIT_MOUNT", "transit"),
		SIEMVaultKeyName:          getenv("CONTROL_PLANE_SIEM_VAULT_TRANSIT_KEY", ""),
		SIEMVaultKeyVersion:       getenvNonNegativeInt("CONTROL_PLANE_SIEM_VAULT_TRANSIT_KEY_VERSION", 0),
		SIEMVaultTimeoutMS:        getenvInt("CONTROL_PLANE_SIEM_VAULT_TIMEOUT_MS", 5000),
		SIEMCoordEnabled:          getenvBool("CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION", false),
		SIEMCoordKey:              getenv("CONTROL_PLANE_SIEM_COORDINATION_KEY", "taskflow:control-plane:siem:leader"),
		SIEMCoordLeaseMS:          getenvInt("CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS", 15000),
		SIEMGCEnabled:             getenvBool("CONTROL_PLANE_SIEM_GC_ENABLED", true),
		SIEMGCIntervalMS:          getenvInt("CONTROL_PLANE_SIEM_GC_INTERVAL_MS", 3600000),
		SIEMDeliveredKeepMS:       getenvInt("CONTROL_PLANE_SIEM_DELIVERED_RETENTION_MS", 2592000000),
		SIEMDLQKeepMS:             getenvInt("CONTROL_PLANE_SIEM_REPLAYED_DLQ_RETENTION_MS", 7776000000),
		SIEMGCBatch:               getenvInt("CONTROL_PLANE_SIEM_GC_BATCH", 1000),
		SIEMDREnabled:             getenvBool("CONTROL_PLANE_SIEM_DR_ENABLED", false),
		SIEMDRMinCopies:           getenvInt("CONTROL_PLANE_SIEM_DR_MIN_COPIES", 2),
		SIEMDRPrimaryEndpoint:     getenv("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_ENDPOINT", ""),
		SIEMDRPrimaryAccessKey:    getenv("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_ACCESS_KEY", ""),
		SIEMDRPrimarySecretKey:    getenv("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_SECRET_KEY", ""),
		SIEMDRPrimaryBucket:       getenv("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_BUCKET", "taskflow-audit-dr"),
		SIEMDRPrimaryRegion:       getenv("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_REGION", "us-east-1"),
		SIEMDRPrimarySecure:       getenvBool("CONTROL_PLANE_SIEM_DR_PRIMARY_S3_SECURE", true),
		SIEMDRReplicaEnabled:      getenvBool("CONTROL_PLANE_SIEM_DR_REPLICA_ENABLED", false),
		SIEMDRReplicaEndpoint:     getenv("CONTROL_PLANE_SIEM_DR_REPLICA_S3_ENDPOINT", ""),
		SIEMDRReplicaAccessKey:    getenv("CONTROL_PLANE_SIEM_DR_REPLICA_S3_ACCESS_KEY", ""),
		SIEMDRReplicaSecretKey:    getenv("CONTROL_PLANE_SIEM_DR_REPLICA_S3_SECRET_KEY", ""),
		SIEMDRReplicaBucket:       getenv("CONTROL_PLANE_SIEM_DR_REPLICA_S3_BUCKET", "taskflow-audit-dr"),
		SIEMDRReplicaRegion:       getenv("CONTROL_PLANE_SIEM_DR_REPLICA_S3_REGION", "us-east-1"),
		SIEMDRReplicaSecure:       getenvBool("CONTROL_PLANE_SIEM_DR_REPLICA_S3_SECURE", true),
		SIEMReconcileEnabled:      getenvBool("CONTROL_PLANE_SIEM_RECONCILE_ENABLED", false),
		SIEMReconcileEndpoint:     getenv("CONTROL_PLANE_SIEM_RECONCILE_ENDPOINT", ""),
		SIEMReconcileTimeoutMS:    getenvInt("CONTROL_PLANE_SIEM_RECONCILE_TIMEOUT_MS", 5000),
		SIEMReconcilePageSize:     getenvInt("CONTROL_PLANE_SIEM_RECONCILE_PAGE_SIZE", 500),
		SIEMReconcileMinEvidence:  getenvInt("CONTROL_PLANE_SIEM_RECONCILE_MIN_EVIDENCE_COPIES", 1),
		SIEMReconcileReceiverID:   getenv("CONTROL_PLANE_SIEM_RECONCILE_RECEIVER_ID", ""),
		SIEMPrimaryReceiverID:     getenv("CONTROL_PLANE_SIEM_PRIMARY_RECEIVER_ID", ""),
		SIEMPrimaryCheckpoint:     getenv("CONTROL_PLANE_SIEM_PRIMARY_CHECKPOINT_ENDPOINT", ""),
		SIEMSecondaryEndpoint:     getenv("CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT", ""),
		SIEMSecondaryReceiverID:   getenv("CONTROL_PLANE_SIEM_SECONDARY_RECEIVER_ID", ""),
		SIEMSecondaryCheckpoint:   getenv("CONTROL_PLANE_SIEM_SECONDARY_CHECKPOINT_ENDPOINT", ""),
		SIEMFailoverEnabled:       getenvBool("CONTROL_PLANE_SIEM_FAILOVER_ENABLED", false),
		SIEMShadowSecondary:       getenvBool("CONTROL_PLANE_SIEM_SHADOW_SECONDARY", false),
		SIEMCheckpointHMACKey:     getenv("CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY", ""),
		SIEMCheckpointHMACKeyID:   getenv("CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY_ID", ""),
		SIEMClusterGuardEnabled:   getenvBool("CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED", false),
		SIEMTermFencingEnabled:    getenvBool("CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED", false),
		SIEMPrimaryTermEndpoint:   getenv("CONTROL_PLANE_SIEM_PRIMARY_TERM_ENDPOINT", ""),
		SIEMSecondaryTermEndpoint: getenv("CONTROL_PLANE_SIEM_SECONDARY_TERM_ENDPOINT", ""),
		SIEMAntiEntropyEnabled:    getenvBool("CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED", false),
		SIEMAntiEntropyIntervalMS: getenvInt("CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS", 2000),
		SIEMPromotionWorkflow:     getenvBool("CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED", false),
		SIEMPromotionIntentTTLMS:  getenvInt("CONTROL_PLANE_SIEM_PROMOTION_INTENT_TTL_MS", 300000),
		SIEMPromotionLeaseMS:      getenvInt("CONTROL_PLANE_SIEM_PROMOTION_LEASE_MS", 15000),

		SIEMPromotionApprovalQuorum:                     getenvInt("CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM", 1),
		SIEMPromotionRolePolicy:                         getenv("CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY", ""),
		SIEMBreakGlassEnabled:                           getenvBool("CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED", false),
		SIEMGovernanceEndpoint:                          getenv("CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT", ""),
		SIEMGovernanceToken:                             getenv("CONTROL_PLANE_SIEM_GOVERNANCE_BEARER_TOKEN", ""),
		SIEMGovernanceSignKey:                           getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY", ""),
		SIEMGovernanceSignKeyID:                         getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SIGNING_KEY_ID", "taskflow-governance-v1"),
		SIEMGovernanceTimeoutMS:                         getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_TIMEOUT_MS", 3000),
		SIEMGovernanceHAEnabled:                         getenvBool("CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED", false),
		SIEMGovernancePrimaryReplicaID:                  getenv("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_REPLICA_ID", "governance-primary"),
		SIEMGovernancePrimaryReceiptKey:                 getenv("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY", ""),
		SIEMGovernancePrimaryReceiptKeyID:               getenv("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_HMAC_KEY_ID", ""),
		SIEMGovernancePrimaryReceiptKeyGeneration:       getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION", 1),
		SIEMGovernancePrimaryPrevReceiptKey:             getenv("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY", ""),
		SIEMGovernancePrimaryPrevReceiptKeyID:           getenv("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY_ID", ""),
		SIEMGovernancePrimaryPrevReceiptKeyGeneration:   getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_KEY_GENERATION", 0),
		SIEMGovernancePrimaryPrevReceiptValidUntilMS:    int64(getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS", 0)),
		SIEMGovernanceSecondaryEndpoint:                 getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_ENDPOINT", ""),
		SIEMGovernanceSecondaryToken:                    getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_BEARER_TOKEN", ""),
		SIEMGovernanceSecondaryReplicaID:                getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_REPLICA_ID", "governance-secondary"),
		SIEMGovernanceSecondaryReceiptKey:               getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY", ""),
		SIEMGovernanceSecondaryReceiptKeyID:             getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY_ID", ""),
		SIEMGovernanceSecondaryReceiptKeyGeneration:     getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_KEY_GENERATION", 1),
		SIEMGovernanceSecondaryPrevReceiptKey:           getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY", ""),
		SIEMGovernanceSecondaryPrevReceiptKeyID:         getenv("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY_ID", ""),
		SIEMGovernanceSecondaryPrevReceiptKeyGeneration: getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_KEY_GENERATION", 0),
		SIEMGovernanceSecondaryPrevReceiptValidUntilMS:  int64(getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS", 0)),
		SIEMGovernanceReceiptQuorum:                     getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM", 1),
		SIEMGovernanceMembershipEpoch:                   uint64(getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH", 1)),
		SIEMGovernanceDebtPolicy:                        getenvBool("CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED", false),
		SIEMGovernanceMaxOutstanding:                    getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING", 100),
		SIEMGovernanceMaxAgeMS:                          getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS", 300000),
		SIEMGovernanceRemediationApproval:               getenvBool("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED", false),
		SIEMGovernanceRemediationTTLMS:                  getenvInt("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_TTL_MS", 300000),
		SIEMGovernanceStaleTargetFence:                  getenvBool("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_STALE_TARGET_FENCING_ENABLED", false),
		SIEMRTOAlertThresholdMS:                         getenvInt("CONTROL_PLANE_SIEM_RTO_ALERT_THRESHOLD_MS", 30000),

		SIEMRPOPolicyEnabled: getenvBool("CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED", false),
		SIEMRPOMaxSequences:  getenvInt("CONTROL_PLANE_SIEM_RPO_MAX_SEQUENCES", 1),
		SIEMRPOGraceMS:       getenvInt("CONTROL_PLANE_SIEM_RPO_GRACE_MS", 5000),

		AccessLog:            getenvBool("ACCESS_LOG", true),
		OTelLogSpans:         getenvBool("OTEL_LOG_SPANS", false),
		OutboxIntervalMS:     getenvInt("OUTBOX_INTERVAL_MS", 200),
		WorkerCount:          getenvInt("WORKER_COUNT", 8),
		MaxRetries:           getenvInt("MAX_RETRIES", 3),
		ReconcileSec:         getenvInt("RECONCILE_SEC", 5),
		ReconcileDistributed: getenvBool("RECONCILE_DISTRIBUTED_COORDINATION", true),
		ReconcileLockKey:     getenv("RECONCILE_LOCK_KEY", "taskflow:reconciler:leader"),
		ReconcileLeaseMS:     getenvInt("RECONCILE_LEASE_MS", 15000),
		EnqueueLeaseMS:       getenvInt("ENQUEUE_VISIBILITY_LEASE_MS", 10000),
		StaleRunSec:          getenvInt("STALE_RUNNING_SEC", 60),
		RateLimitRPS:         getenvInt("RATE_LIMIT_RPS", 100),
		RateLimitBurst:       getenvInt("RATE_LIMIT_BURST", 200),
		DrainFenceMS:         getenvNonNegativeInt("DRAIN_FENCE_MS", 1000),
		DrainTimeoutMS:       getenvInt("DRAIN_TIMEOUT_MS", 30000),
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.MySQLFailoverDSNs) != "" && c.MySQLConnectTimeoutMS <= 0 {
		return fmt.Errorf("MYSQL_CONNECT_TIMEOUT_MS must be positive when MYSQL_FAILOVER_DSNS is configured")
	}
	activeToken := strings.TrimSpace(c.ControlPlaneBearerToken)
	previousToken := strings.TrimSpace(c.ControlPlanePrevToken)
	operatorID := strings.TrimSpace(c.ControlPlaneOperatorID)
	previousID := strings.TrimSpace(c.ControlPlanePreviousID)
	authMode := strings.ToLower(strings.TrimSpace(c.ControlPlaneAuthMode))
	if authMode == "" {
		authMode = "bearer"
	}
	if authMode != "bearer" && authMode != "oidc" && authMode != "hybrid" {
		return fmt.Errorf("CONTROL_PLANE_AUTH_MODE must be bearer, oidc or hybrid")
	}
	if c.CloudTenantAuthzEnabled && (!c.ControlPlaneAuthEnabled || authMode != "oidc" || !c.ControlPlaneOIDCRBAC) {
		return fmt.Errorf("CLOUD_TENANT_AUTHZ_ENABLED requires CONTROL_PLANE_AUTH_ENABLED=true, CONTROL_PLANE_AUTH_MODE=oidc, and CONTROL_PLANE_OIDC_RBAC_ENABLED=true")
	}
	bearerAuth := authMode == "bearer" || authMode == "hybrid"
	oidcAuth := authMode == "oidc" || authMode == "hybrid"
	additionalCredentials, additionalErr := parseAdditionalControlPlaneCredentials(c.ControlPlaneExtraCredentials)
	if additionalErr != nil {
		return additionalErr
	}
	if c.ControlPlaneAuthEnabled && bearerAuth && activeToken == "" {
		return fmt.Errorf("CONTROL_PLANE_AUTH_ENABLED=true requires CONTROL_PLANE_BEARER_TOKEN")
	}
	if c.ControlPlaneAuthEnabled && bearerAuth && operatorID == "" {
		return fmt.Errorf("CONTROL_PLANE_AUTH_ENABLED=true requires CONTROL_PLANE_OPERATOR_ID")
	}
	if c.ControlPlaneAuthEnabled && oidcAuth {
		if strings.TrimSpace(c.ControlPlaneOIDCIssuer) == "" || strings.TrimSpace(c.ControlPlaneOIDCAudience) == "" || strings.TrimSpace(c.ControlPlaneOIDCJWKSURL) == "" {
			return fmt.Errorf("OIDC control-plane authentication requires issuer, audience and JWKS URL")
		}
		if c.ControlPlaneOIDCJWKSTTLMS <= 0 || c.ControlPlaneOIDCSkewMS < 0 {
			return fmt.Errorf("OIDC JWKS cache TTL must be positive and clock skew must be non-negative")
		}
		if strings.TrimSpace(c.ControlPlaneOIDCGroups) == "" {
			return fmt.Errorf("CONTROL_PLANE_OIDC_GROUPS_CLAIM must not be empty")
		}
		if c.ControlPlaneOIDCRBAC && strings.TrimSpace(c.ControlPlaneOIDCRoleMap) == "" {
			return fmt.Errorf("CONTROL_PLANE_OIDC_RBAC_ENABLED=true requires CONTROL_PLANE_OIDC_ROLE_MAPPINGS")
		}
	}
	if c.ControlPlaneAuthEnabled && authMode == "oidc" && (activeToken != "" || previousToken != "" || len(additionalCredentials) > 0) {
		return fmt.Errorf("bearer credentials are not accepted when CONTROL_PLANE_AUTH_MODE=oidc")
	}
	if previousToken != "" && !c.ControlPlaneAuthEnabled {
		return fmt.Errorf("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN requires CONTROL_PLANE_AUTH_ENABLED=true")
	}
	if previousToken != "" && previousToken == activeToken {
		return fmt.Errorf("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN must differ from CONTROL_PLANE_BEARER_TOKEN")
	}
	if previousID != "" && previousToken == "" {
		return fmt.Errorf("CONTROL_PLANE_PREVIOUS_OPERATOR_ID requires CONTROL_PLANE_PREVIOUS_BEARER_TOKEN")
	}
	if len(additionalCredentials) > 0 && !c.ControlPlaneAuthEnabled {
		return fmt.Errorf("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS requires CONTROL_PLANE_AUTH_ENABLED=true")
	}
	seenTokens := map[string]string{}
	if activeToken != "" {
		seenTokens[activeToken] = operatorID
	}
	if previousToken != "" {
		seenTokens[previousToken] = previousID
	}
	for additionalOperator, token := range additionalCredentials {
		if additionalOperator == operatorID || (previousID != "" && additionalOperator == previousID) {
			return fmt.Errorf("additional control-plane operator %q duplicates an active/previous operator id", additionalOperator)
		}
		if owner, exists := seenTokens[token]; exists {
			return fmt.Errorf("additional control-plane credential for %q reuses credential assigned to %q", additionalOperator, owner)
		}
		seenTokens[token] = additionalOperator
	}
	if c.ControlSIEMEnabled {
		endpoint := strings.TrimSpace(c.ControlSIEMEndpoint)
		if endpoint == "" {
			return fmt.Errorf("CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true requires CONTROL_PLANE_SIEM_ENDPOINT")
		}
		parsed, err := url.Parse(endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("CONTROL_PLANE_SIEM_ENDPOINT must be an absolute http(s) URL")
		}
		if strings.TrimSpace(c.ControlSIEMSignKeyID) == "" {
			return fmt.Errorf("CONTROL_PLANE_SIEM_DELIVERY_ENABLED=true requires CONTROL_PLANE_SIEM_SIGNING_KEY_ID")
		}
		provider := strings.ToLower(strings.TrimSpace(c.SIEMSignProvider))
		if provider == "" {
			provider = "hmac"
		}
		switch provider {
		case "hmac", "hmac-sha256":
			if strings.TrimSpace(c.ControlSIEMSignKey) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=hmac requires CONTROL_PLANE_SIEM_SIGNING_KEY")
			}
			previousKey := strings.TrimSpace(c.SIEMPrevSignKey)
			previousKeyID := strings.TrimSpace(c.SIEMPrevSignKeyID)
			if (previousKey == "") != (previousKeyID == "") {
				return fmt.Errorf("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY and CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID must be configured together")
			}
			if previousKey != "" && previousKey == strings.TrimSpace(c.ControlSIEMSignKey) {
				return fmt.Errorf("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY must differ from CONTROL_PLANE_SIEM_SIGNING_KEY")
			}
			if previousKeyID != "" && previousKeyID == strings.TrimSpace(c.ControlSIEMSignKeyID) {
				return fmt.Errorf("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID must differ from CONTROL_PLANE_SIEM_SIGNING_KEY_ID")
			}
		case "remote", "kms", "vault":
			if strings.TrimSpace(c.SIEMRemoteEndpoint) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=remote requires CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT")
			}
			if strings.TrimSpace(c.SIEMRemoteAlgo) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM must not be empty")
			}
			if strings.TrimSpace(c.SIEMPrevSignKey) != "" || strings.TrimSpace(c.SIEMPrevSignKeyID) != "" {
				return fmt.Errorf("previous local HMAC signing key is not supported with remote SIEM signer")
			}
		case "aws-kms", "aws-kms-hmac":
			if strings.TrimSpace(c.SIEMAWSKMSRegion) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=aws-kms requires CONTROL_PLANE_SIEM_AWS_KMS_REGION")
			}
			if strings.TrimSpace(c.SIEMAWSKMSKeyID) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=aws-kms requires CONTROL_PLANE_SIEM_AWS_KMS_KEY_ID")
			}
			credentialSource := strings.ToLower(strings.TrimSpace(c.SIEMAWSCredentialsSource))
			if credentialSource == "" {
				credentialSource = "static"
			}
			switch credentialSource {
			case "static":
				if strings.TrimSpace(c.SIEMAWSKMSAccessKey) == "" || strings.TrimSpace(c.SIEMAWSKMSSecretKey) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=static requires AWS access credentials")
				}
			case "web-identity":
				if strings.TrimSpace(c.SIEMAWSWebIdentityFile) == "" || strings.TrimSpace(c.SIEMAWSWebIdentityRoleARN) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=web-identity requires AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN")
				}
			case "imds", "imds-v2":
				if strings.TrimSpace(c.SIEMAWSIMDSEndpoint) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=imds requires CONTROL_PLANE_SIEM_AWS_IMDS_ENDPOINT")
				}
			case "auto":
				if strings.TrimSpace(c.SIEMAWSKMSAccessKey) == "" && strings.TrimSpace(c.SIEMAWSWebIdentityFile) == "" && strings.TrimSpace(c.SIEMAWSIMDSEndpoint) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE=auto has no configured credential provider")
				}
			default:
				return fmt.Errorf("unsupported CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE %q", c.SIEMAWSCredentialsSource)
			}
			if strings.TrimSpace(c.SIEMPrevSignKey) != "" || strings.TrimSpace(c.SIEMPrevSignKeyID) != "" {
				return fmt.Errorf("previous local HMAC signing key is not supported with AWS KMS signer")
			}
		case "vault-transit":
			if strings.TrimSpace(c.SIEMVaultEndpoint) == "" || strings.TrimSpace(c.SIEMVaultToken) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=vault-transit requires Vault endpoint and token")
			}
			if strings.TrimSpace(c.SIEMVaultKeyName) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_SIGNING_PROVIDER=vault-transit requires CONTROL_PLANE_SIEM_VAULT_TRANSIT_KEY")
			}
			if strings.TrimSpace(c.SIEMPrevSignKey) != "" || strings.TrimSpace(c.SIEMPrevSignKeyID) != "" {
				return fmt.Errorf("previous local HMAC signing key is not supported with Vault Transit signer")
			}
		default:
			return fmt.Errorf("unsupported CONTROL_PLANE_SIEM_SIGNING_PROVIDER %q", c.SIEMSignProvider)
		}
		if c.SIEMCoordEnabled && strings.TrimSpace(c.SIEMCoordKey) == "" {
			return fmt.Errorf("CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION=true requires CONTROL_PLANE_SIEM_COORDINATION_KEY")
		}
		if c.ControlSIEMRetryMaxMS < c.ControlSIEMRetryBaseMS {
			return fmt.Errorf("CONTROL_PLANE_SIEM_RETRY_MAX_MS must be >= CONTROL_PLANE_SIEM_RETRY_BASE_MS")
		}
		if c.SIEMDREnabled {
			if strings.TrimSpace(c.SIEMDRPrimaryEndpoint) == "" || strings.TrimSpace(c.SIEMDRPrimaryAccessKey) == "" || strings.TrimSpace(c.SIEMDRPrimarySecretKey) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_DR_ENABLED=true requires primary S3 endpoint and credentials")
			}
			sinks := 1
			if c.SIEMDRReplicaEnabled {
				sinks++
				if strings.TrimSpace(c.SIEMDRReplicaEndpoint) == "" || strings.TrimSpace(c.SIEMDRReplicaAccessKey) == "" || strings.TrimSpace(c.SIEMDRReplicaSecretKey) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_DR_REPLICA_ENABLED=true requires replica S3 endpoint and credentials")
				}
			}
			if c.SIEMDRMinCopies > sinks {
				return fmt.Errorf("CONTROL_PLANE_SIEM_DR_MIN_COPIES=%d exceeds configured sinks=%d", c.SIEMDRMinCopies, sinks)
			}
			if c.SIEMReconcileEnabled && c.SIEMReconcileMinEvidence > sinks {
				return fmt.Errorf("CONTROL_PLANE_SIEM_RECONCILE_MIN_EVIDENCE_COPIES=%d exceeds configured sinks=%d", c.SIEMReconcileMinEvidence, sinks)
			}
		}
		if c.SIEMReconcileEnabled {
			endpoint := strings.TrimSpace(c.SIEMReconcileEndpoint)
			parsed, err := url.Parse(endpoint)
			if endpoint == "" || err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				return fmt.Errorf("CONTROL_PLANE_SIEM_RECONCILE_ENABLED=true requires an absolute http(s) CONTROL_PLANE_SIEM_RECONCILE_ENDPOINT")
			}
			if c.SIEMReconcilePageSize <= 0 || c.SIEMReconcilePageSize > 500 {
				return fmt.Errorf("CONTROL_PLANE_SIEM_RECONCILE_PAGE_SIZE must be between 1 and 500")
			}
		}
		if c.SIEMFailoverEnabled || c.SIEMShadowSecondary || c.SIEMClusterGuardEnabled || c.SIEMTermFencingEnabled || c.SIEMAntiEntropyEnabled || c.SIEMPromotionWorkflow || c.SIEMRPOPolicyEnabled {
			if strings.TrimSpace(c.SIEMPrimaryReceiverID) == "" || strings.TrimSpace(c.SIEMSecondaryReceiverID) == "" {
				return fmt.Errorf("receiver cluster mode requires primary and secondary receiver ids")
			}
			if strings.TrimSpace(c.SIEMPrimaryReceiverID) == strings.TrimSpace(c.SIEMSecondaryReceiverID) {
				return fmt.Errorf("receiver cluster mode requires distinct primary and secondary receiver ids")
			}
			secondary, err := url.Parse(strings.TrimSpace(c.SIEMSecondaryEndpoint))
			if err != nil || secondary.Host == "" || (secondary.Scheme != "http" && secondary.Scheme != "https") {
				return fmt.Errorf("receiver cluster mode requires an absolute http(s) CONTROL_PLANE_SIEM_SECONDARY_ENDPOINT")
			}
		}
		if c.SIEMFailoverEnabled || c.SIEMClusterGuardEnabled || c.SIEMTermFencingEnabled || c.SIEMAntiEntropyEnabled || c.SIEMPromotionWorkflow || c.SIEMRPOPolicyEnabled {
			if strings.TrimSpace(c.SIEMCheckpointHMACKey) == "" || strings.TrimSpace(c.SIEMCheckpointHMACKeyID) == "" {
				return fmt.Errorf("receiver failover/cluster guard requires checkpoint HMAC key and key id")
			}
			secondaryCheckpoint, err := url.Parse(strings.TrimSpace(c.SIEMSecondaryCheckpoint))
			if err != nil || secondaryCheckpoint.Host == "" || (secondaryCheckpoint.Scheme != "http" && secondaryCheckpoint.Scheme != "https") {
				return fmt.Errorf("receiver failover/cluster guard requires an absolute secondary checkpoint endpoint")
			}
		}
		if c.SIEMClusterGuardEnabled {
			primaryCheckpoint, err := url.Parse(strings.TrimSpace(c.SIEMPrimaryCheckpoint))
			if err != nil || primaryCheckpoint.Host == "" || (primaryCheckpoint.Scheme != "http" && primaryCheckpoint.Scheme != "https") {
				return fmt.Errorf("receiver cluster guard requires an absolute primary checkpoint endpoint")
			}
		}
		if c.SIEMTermFencingEnabled {
			primaryTerm, err := url.Parse(strings.TrimSpace(c.SIEMPrimaryTermEndpoint))
			if err != nil || primaryTerm.Host == "" || (primaryTerm.Scheme != "http" && primaryTerm.Scheme != "https") {
				return fmt.Errorf("receiver term fencing requires an absolute primary term endpoint")
			}
			secondaryTerm, err := url.Parse(strings.TrimSpace(c.SIEMSecondaryTermEndpoint))
			if err != nil || secondaryTerm.Host == "" || (secondaryTerm.Scheme != "http" && secondaryTerm.Scheme != "https") {
				return fmt.Errorf("receiver term fencing requires an absolute secondary term endpoint")
			}
		}
		if c.SIEMAntiEntropyEnabled && c.SIEMAntiEntropyIntervalMS <= 0 {
			return fmt.Errorf("CONTROL_PLANE_SIEM_ANTI_ENTROPY_INTERVAL_MS must be positive")
		}
		if c.SIEMPromotionWorkflow {
			if !c.SIEMTermFencingEnabled {
				return fmt.Errorf("CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED=true requires receiver term fencing")
			}
			if !c.ControlPlaneAuthEnabled {
				return fmt.Errorf("receiver promotion workflow requires control-plane authentication")
			}
			if bearerAuth {
				if activeToken == "" || previousToken == "" {
					return fmt.Errorf("receiver promotion workflow requires active and previous control-plane credentials in bearer/hybrid mode")
				}
				if operatorID == "" || previousID == "" || operatorID == previousID {
					return fmt.Errorf("receiver promotion workflow requires two distinct control-plane operator ids in bearer/hybrid mode")
				}
			} else if !c.ControlPlaneOIDCRBAC {
				return fmt.Errorf("receiver promotion workflow in OIDC mode requires CONTROL_PLANE_OIDC_RBAC_ENABLED=true")
			}
			if c.SIEMPromotionIntentTTLMS <= 0 || c.SIEMPromotionLeaseMS <= 0 {
				return fmt.Errorf("receiver promotion workflow TTL and execution lease must be positive")
			}
			if c.SIEMPromotionApprovalQuorum <= 0 || c.SIEMPromotionApprovalQuorum > 5 {
				return fmt.Errorf("CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM must be between 1 and 5")
			}
			if c.SIEMPromotionApprovalQuorum > 1 && strings.TrimSpace(c.SIEMPromotionRolePolicy) == "" && !c.ControlPlaneOIDCRBAC {
				return fmt.Errorf("approval quorum greater than 1 requires CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY")
			}
			if (c.SIEMBreakGlassEnabled || c.SIEMGovernanceRemediationApproval) && strings.TrimSpace(c.SIEMPromotionRolePolicy) == "" && !c.ControlPlaneOIDCRBAC {
				return fmt.Errorf("break-glass or governance remediation approval requires CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY")
			}
			if strings.TrimSpace(c.SIEMPromotionRolePolicy) != "" {
				allowedOperators := map[string]struct{}{operatorID: {}}
				if previousID != "" {
					allowedOperators[previousID] = struct{}{}
				}
				for additionalOperator := range additionalCredentials {
					allowedOperators[additionalOperator] = struct{}{}
				}
				if err := validateSIEMPromotionRolePolicy(
					c.SIEMPromotionRolePolicy,
					allowedOperators,
					c.SIEMPromotionApprovalQuorum,
					c.SIEMBreakGlassEnabled || c.SIEMGovernanceRemediationApproval,
					c.SIEMGovernanceRemediationApproval,
				); err != nil {
					return err
				}
			}
			if strings.TrimSpace(c.SIEMGovernanceEndpoint) != "" {
				governanceEndpoint, err := url.Parse(strings.TrimSpace(c.SIEMGovernanceEndpoint))
				if err != nil || governanceEndpoint.Host == "" || (governanceEndpoint.Scheme != "http" && governanceEndpoint.Scheme != "https") {
					return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_ENDPOINT must be an absolute http(s) URL")
				}
				if strings.TrimSpace(c.SIEMGovernanceSignKey) == "" || strings.TrimSpace(c.SIEMGovernanceSignKeyID) == "" {
					return fmt.Errorf("SIEM governance endpoint requires dedicated signing key and key id")
				}
				if c.SIEMGovernanceTimeoutMS <= 0 || c.SIEMRTOAlertThresholdMS <= 0 {
					return fmt.Errorf("SIEM governance timeout and RTO alert threshold must be positive")
				}
			}
			if c.SIEMGovernanceHAEnabled {
				if strings.TrimSpace(c.SIEMGovernanceEndpoint) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED=true requires the primary governance endpoint")
				}
				secondaryEndpoint, err := url.Parse(strings.TrimSpace(c.SIEMGovernanceSecondaryEndpoint))
				if err != nil || secondaryEndpoint.Host == "" || (secondaryEndpoint.Scheme != "http" && secondaryEndpoint.Scheme != "https") {
					return fmt.Errorf("governance HA requires an absolute secondary governance endpoint")
				}
				if strings.TrimRight(strings.TrimSpace(c.SIEMGovernanceEndpoint), "/") == strings.TrimRight(strings.TrimSpace(c.SIEMGovernanceSecondaryEndpoint), "/") {
					return fmt.Errorf("governance HA primary and secondary endpoints must be distinct")
				}
				primaryReplica := strings.TrimSpace(c.SIEMGovernancePrimaryReplicaID)
				secondaryReplica := strings.TrimSpace(c.SIEMGovernanceSecondaryReplicaID)
				if primaryReplica == "" || secondaryReplica == "" || primaryReplica == secondaryReplica {
					return fmt.Errorf("governance HA requires two distinct non-empty replica ids")
				}
				if strings.TrimSpace(c.SIEMGovernancePrimaryReceiptKey) == "" || strings.TrimSpace(c.SIEMGovernancePrimaryReceiptKeyID) == "" ||
					strings.TrimSpace(c.SIEMGovernanceSecondaryReceiptKey) == "" || strings.TrimSpace(c.SIEMGovernanceSecondaryReceiptKeyID) == "" {
					return fmt.Errorf("governance HA requires dedicated receipt HMAC keys and key ids for both replicas")
				}
				if c.SIEMGovernanceReceiptQuorum != 2 {
					return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM must be 2 while governance HA is enabled")
				}
				if c.SIEMGovernanceMembershipEpoch == 0 {
					return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH must be positive while governance HA is enabled")
				}
				if c.SIEMGovernancePrimaryReceiptKeyGeneration <= 0 || c.SIEMGovernanceSecondaryReceiptKeyGeneration <= 0 {
					return fmt.Errorf("governance HA active receipt key generations must be positive")
				}
				if (strings.TrimSpace(c.SIEMGovernancePrimaryPrevReceiptKey) == "") != (strings.TrimSpace(c.SIEMGovernancePrimaryPrevReceiptKeyID) == "") ||
					(strings.TrimSpace(c.SIEMGovernanceSecondaryPrevReceiptKey) == "") != (strings.TrimSpace(c.SIEMGovernanceSecondaryPrevReceiptKeyID) == "") {
					return fmt.Errorf("governance HA previous receipt HMAC key and key id must be configured together per replica")
				}
				primaryPreviousConfigured := strings.TrimSpace(c.SIEMGovernancePrimaryPrevReceiptKeyID) != ""
				secondaryPreviousConfigured := strings.TrimSpace(c.SIEMGovernanceSecondaryPrevReceiptKeyID) != ""
				if primaryPreviousConfigured {
					if c.SIEMGovernancePrimaryPrevReceiptKeyGeneration <= 0 || c.SIEMGovernancePrimaryPrevReceiptKeyGeneration >= c.SIEMGovernancePrimaryReceiptKeyGeneration || c.SIEMGovernancePrimaryPrevReceiptValidUntilMS <= 0 {
						return fmt.Errorf("primary governance previous receipt key requires an older positive generation and explicit valid-until unix milliseconds")
					}
				} else if c.SIEMGovernancePrimaryPrevReceiptKeyGeneration != 0 || c.SIEMGovernancePrimaryPrevReceiptValidUntilMS != 0 {
					return fmt.Errorf("primary governance previous receipt generation/valid-until require a previous receipt key")
				}
				if secondaryPreviousConfigured {
					if c.SIEMGovernanceSecondaryPrevReceiptKeyGeneration <= 0 || c.SIEMGovernanceSecondaryPrevReceiptKeyGeneration >= c.SIEMGovernanceSecondaryReceiptKeyGeneration || c.SIEMGovernanceSecondaryPrevReceiptValidUntilMS <= 0 {
						return fmt.Errorf("secondary governance previous receipt key requires an older positive generation and explicit valid-until unix milliseconds")
					}
				} else if c.SIEMGovernanceSecondaryPrevReceiptKeyGeneration != 0 || c.SIEMGovernanceSecondaryPrevReceiptValidUntilMS != 0 {
					return fmt.Errorf("secondary governance previous receipt generation/valid-until require a previous receipt key")
				}
				if strings.TrimSpace(c.SIEMGovernancePrimaryPrevReceiptKeyID) != "" && strings.TrimSpace(c.SIEMGovernancePrimaryPrevReceiptKeyID) == strings.TrimSpace(c.SIEMGovernancePrimaryReceiptKeyID) {
					return fmt.Errorf("primary governance previous receipt key id must differ from active key id")
				}
				if strings.TrimSpace(c.SIEMGovernanceSecondaryPrevReceiptKeyID) != "" && strings.TrimSpace(c.SIEMGovernanceSecondaryPrevReceiptKeyID) == strings.TrimSpace(c.SIEMGovernanceSecondaryReceiptKeyID) {
					return fmt.Errorf("secondary governance previous receipt key id must differ from active key id")
				}
			} else if c.SIEMGovernanceReceiptQuorum != 1 {
				return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM must be 1 while governance HA is disabled")
			}
			if c.SIEMGovernanceDebtPolicy {
				if strings.TrimSpace(c.SIEMGovernanceEndpoint) == "" {
					return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED=true requires a governance endpoint")
				}
				if c.SIEMGovernanceMaxOutstanding <= 0 || c.SIEMGovernanceMaxAgeMS <= 0 {
					return fmt.Errorf("governance debt max outstanding and max age must be positive")
				}
			}
			if c.SIEMGovernanceRemediationApproval && strings.TrimSpace(c.SIEMGovernanceEndpoint) == "" {
				return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED=true requires a governance endpoint")
			}
			if c.SIEMGovernanceRemediationApproval && c.SIEMGovernanceRemediationTTLMS <= 0 {
				return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_TTL_MS must be positive")
			}
			if c.SIEMGovernanceStaleTargetFence && !c.SIEMGovernanceRemediationApproval {
				return fmt.Errorf("CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_STALE_TARGET_FENCING_ENABLED=true requires remediation approval workflow")
			}
		}
		if c.SIEMRPOPolicyEnabled {
			if !c.SIEMPromotionWorkflow || !c.SIEMAntiEntropyEnabled {
				return fmt.Errorf("CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED=true requires promotion workflow and anti-entropy")
			}
			if c.SIEMRPOMaxSequences <= 0 || c.SIEMRPOGraceMS <= 0 {
				return fmt.Errorf("receiver RPO policy max sequences and grace must be positive")
			}
		}
	}
	return nil
}

func parseAdditionalControlPlaneCredentials(raw string) (map[string]string, error) {
	credentials := map[string]string{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return credentials, nil
	}
	seenTokens := map[string]struct{}{}
	for _, item := range strings.Split(raw, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS must use operator=token entries separated by semicolons")
		}
		operator := strings.TrimSpace(parts[0])
		token := strings.TrimSpace(parts[1])
		if operator == "" || token == "" {
			return nil, fmt.Errorf("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS contains an empty operator or token")
		}
		if _, exists := credentials[operator]; exists {
			return nil, fmt.Errorf("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS duplicates operator %q", operator)
		}
		if _, exists := seenTokens[token]; exists {
			return nil, fmt.Errorf("CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS reuses a bearer token")
		}
		credentials[operator] = token
		seenTokens[token] = struct{}{}
	}
	return credentials, nil
}

func validateSIEMPromotionRolePolicy(
	policy string,
	allowedOperators map[string]struct{},
	quorum int,
	requireIncidentCommander bool,
	requireGovernanceReviewer bool,
) error {
	policy = strings.TrimSpace(policy)
	if policy == "" {
		if quorum <= 1 && !requireIncidentCommander && !requireGovernanceReviewer {
			return nil
		}
		return fmt.Errorf("CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY is required")
	}
	seenOperators := map[string]struct{}{}
	requesters := 0
	approvers := 0
	commanders := 0
	governanceReviewers := 0
	for _, assignment := range strings.Split(policy, ";") {
		parts := strings.SplitN(strings.TrimSpace(assignment), "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY must use operator=role|role entries separated by semicolons")
		}
		operator := strings.TrimSpace(parts[0])
		if operator == "" {
			return fmt.Errorf("CONTROL_PLANE_SIEM_PROMOTION_ROLE_POLICY contains an empty operator")
		}
		if _, exists := allowedOperators[operator]; !exists {
			return fmt.Errorf("promotion role policy references unauthenticated operator %q", operator)
		}
		if _, exists := seenOperators[operator]; exists {
			return fmt.Errorf("promotion role policy duplicates operator %q", operator)
		}
		seenOperators[operator] = struct{}{}
		seenRoles := map[string]struct{}{}
		for _, rawRole := range strings.Split(parts[1], "|") {
			role := strings.TrimSpace(rawRole)
			if role == "" {
				return fmt.Errorf("promotion role policy contains an empty role for %q", operator)
			}
			if _, exists := seenRoles[role]; exists {
				continue
			}
			seenRoles[role] = struct{}{}
			switch role {
			case "requester":
				requesters++
			case "approver":
				approvers++
			case "incident-commander":
				commanders++
			case "governance-reviewer":
				governanceReviewers++
			default:
				return fmt.Errorf("unsupported SIEM promotion role %q", role)
			}
		}
	}
	if requesters == 0 {
		return fmt.Errorf("promotion role policy requires at least one requester")
	}
	if approvers < quorum {
		return fmt.Errorf("promotion role policy has %d approvers but quorum requires %d", approvers, quorum)
	}
	if requireIncidentCommander && commanders == 0 {
		return fmt.Errorf("break-glass or governance remediation approval requires at least one incident-commander")
	}
	if requireGovernanceReviewer && governanceReviewers == 0 {
		return fmt.Errorf("governance remediation approval requires at least one governance-reviewer")
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func getenvNonNegativeInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func getenvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getenvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
