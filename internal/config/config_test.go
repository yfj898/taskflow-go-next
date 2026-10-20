package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("WORKER_COUNT", "")
	t.Setenv("QUEUE_STATS_INTERVAL_MS", "")
	t.Setenv("QUEUE_COMPACTION_ENABLED", "")
	t.Setenv("QUEUE_COMPACTION_INTERVAL_MS", "")
	t.Setenv("QUEUE_ACK_RETENTION_MS", "")
	t.Setenv("QUEUE_ACK_RETENTION_MAX_ENTRIES", "")
	t.Setenv("QUEUE_DLQ_REPLAY_LEASE_MS", "")
	t.Setenv("QUEUE_DLQ_LIFECYCLE_ENABLED", "")
	t.Setenv("QUEUE_DLQ_LIFECYCLE_INTERVAL_MS", "")
	t.Setenv("QUEUE_DLQ_REPLAYED_RETENTION_MS", "")
	t.Setenv("QUEUE_DLQ_QUARANTINED_RETENTION_MS", "")
	t.Setenv("QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS", "")
	t.Setenv("QUEUE_DLQ_MAX_ENTRIES", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ENABLED", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_SINK", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DIR", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_LEASE_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_BASE_URL", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_BEARER_TOKEN", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_TIMEOUT_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_ENDPOINT", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_ACCESS_KEY", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_SECRET_KEY", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_BUCKET", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_REGION", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_SECURE", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_SINK", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_DIR", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BASE_URL", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BEARER_TOKEN", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_TIMEOUT_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ACCESS_KEY", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECRET_KEY", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_BUCKET", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_REGION", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECURE", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_MIN_COPIES", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_INTERVAL_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_GC_BATCH", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ORPHAN_BATCH", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DRILL_INTERVAL_MS", "")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DRILL_BATCH", "")
	t.Setenv("DRAIN_FENCE_MS", "")
	t.Setenv("DRAIN_TIMEOUT_MS", "")
	t.Setenv("RECONCILE_DISTRIBUTED_COORDINATION", "")
	t.Setenv("RECONCILE_LOCK_KEY", "")
	t.Setenv("RECONCILE_LEASE_MS", "")
	t.Setenv("ENQUEUE_VISIBILITY_LEASE_MS", "")
	t.Setenv("MYSQL_MAX_OPEN_CONNS", "")
	t.Setenv("MYSQL_MAX_IDLE_CONNS", "")
	t.Setenv("MYSQL_SKIP_DEFAULT_TX", "")
	t.Setenv("EXECUTION_BACKEND", "")
	t.Setenv("EXECUTOR_METRICS_ADDR", "")
	t.Setenv("AI_PROVIDER", "")
	t.Setenv("AI_PROVIDER_KIND", "")
	t.Setenv("AI_ENDPOINT", "")
	t.Setenv("AI_BEARER_TOKEN", "")
	t.Setenv("AI_MODEL", "")
	t.Setenv("AI_CAPABILITIES", "")
	t.Setenv("AI_INPUT_COST_PER_1M", "")
	t.Setenv("AI_OUTPUT_COST_PER_1M", "")
	t.Setenv("AI_GLOBAL_MAX_CONCURRENCY", "")
	t.Setenv("AI_MAX_CONCURRENCY", "")
	t.Setenv("AI_FALLBACK_PROVIDER", "")
	t.Setenv("AI_FALLBACK_KIND", "")
	t.Setenv("AI_FALLBACK_ENDPOINT", "")
	t.Setenv("AI_FALLBACK_BEARER_TOKEN", "")
	t.Setenv("AI_FALLBACK_MODEL", "")
	t.Setenv("AI_FALLBACK_CAPABILITIES", "")
	t.Setenv("AI_FALLBACK_INPUT_COST_PER_1M", "")
	t.Setenv("AI_FALLBACK_OUTPUT_COST_PER_1M", "")
	t.Setenv("AI_FALLBACK_GLOBAL_MAX_CONCURRENCY", "")
	t.Setenv("AI_FALLBACK_MAX_CONCURRENCY", "")
	t.Setenv("AI_ROUTING_STRATEGY", "")
	t.Setenv("AI_PROVIDER_CIRCUIT_FAILURES", "")
	t.Setenv("AI_PROVIDER_CIRCUIT_OPEN_MS", "")
	t.Setenv("AI_DEFAULT_OUTPUT_TOKENS", "")
	t.Setenv("AI_DISTRIBUTED_LEASE_MS", "")
	t.Setenv("AI_CONCURRENCY_REDIS_PREFIX", "")
	t.Setenv("AI_ADAPTIVE_LOAD_SHEDDING", "")
	t.Setenv("AI_PRESSURE_DEGRADED_QUEUE", "")
	t.Setenv("AI_PRESSURE_OVERLOADED_QUEUE", "")
	t.Setenv("AI_PRESSURE_DEGRADED_FAILURE_RATE", "")
	t.Setenv("AI_PRESSURE_OVERLOADED_FAILURE_RATE", "")
	t.Setenv("AI_PRESSURE_DEGRADED_LATENCY_MS", "")
	t.Setenv("AI_PRESSURE_OVERLOADED_LATENCY_MS", "")
	t.Setenv("AI_HEDGE_ENABLED", "")
	t.Setenv("AI_HEDGE_DELAY_MS", "")
	t.Setenv("AI_HEDGE_CANCEL_WAIT_MS", "")
	t.Setenv("AI_HEDGE_MAX_EXTRA_COST_USD", "")
	t.Setenv("AI_SLO_REALTIME_QUEUE_MS", "")
	t.Setenv("AI_SLO_REALTIME_TIMEOUT_MS", "")
	t.Setenv("AI_SLO_STANDARD_QUEUE_MS", "")
	t.Setenv("AI_SLO_STANDARD_TIMEOUT_MS", "")
	t.Setenv("AI_SLO_BATCH_QUEUE_MS", "")
	t.Setenv("AI_SLO_BATCH_TIMEOUT_MS", "")
	t.Setenv("AI_TENANT_FAIR_SCHEDULING", "")
	t.Setenv("AI_TENANT_MAX_CONSECUTIVE", "")
	t.Setenv("AI_TENANT_STARVATION_MS", "")
	t.Setenv("AI_DISTRIBUTED_TENANT_QUOTA", "")
	t.Setenv("AI_TENANT_GLOBAL_MAX_CONCURRENCY", "")
	t.Setenv("AI_TENANT_CONCURRENCY_OVERRIDES", "")
	t.Setenv("AI_BUDGET_DEFAULT_RESERVE_USD", "")
	t.Setenv("AI_TIMEOUT_MS", "")
	t.Setenv("CONTROL_PLANE_AUTH_ENABLED", "")
	t.Setenv("CONTROL_PLANE_BEARER_TOKEN", "")
	t.Setenv("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN", "")
	t.Setenv("CONTROL_PLANE_OPERATOR_ID", "")
	t.Setenv("CONTROL_PLANE_PREVIOUS_OPERATOR_ID", "")
	t.Setenv("CONTROL_PLANE_AUDIT_ENABLED", "")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_ENABLED", "")
	t.Setenv("CONTROL_PLANE_AUDIT_LIFECYCLE_INTERVAL_MS", "")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_MS", "")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_BATCH", "")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_ENABLED", "")
	t.Setenv("CONTROL_PLANE_SIEM_ENDPOINT", "")
	t.Setenv("CONTROL_PLANE_SIEM_BEARER_TOKEN", "")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_KEY", "")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_KEY_ID", "")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_BATCH", "")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_MAX_ATTEMPTS", "")
	t.Setenv("CONTROL_PLANE_SIEM_RETRY_BASE_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_RETRY_MAX_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_HTTP_TIMEOUT_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_PROVIDER", "")
	t.Setenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY", "")
	t.Setenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID", "")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT", "")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN", "")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM", "")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_TIMEOUT_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION", "")
	t.Setenv("CONTROL_PLANE_SIEM_COORDINATION_KEY", "")
	t.Setenv("CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_GC_ENABLED", "")
	t.Setenv("CONTROL_PLANE_SIEM_GC_INTERVAL_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERED_RETENTION_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_REPLAYED_DLQ_RETENTION_MS", "")
	t.Setenv("CONTROL_PLANE_SIEM_GC_BATCH", "")

	cfg := Load()
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.WorkerCount != 8 {
		t.Fatalf("WorkerCount = %d, want 8", cfg.WorkerCount)
	}
	if cfg.QueueStatsIntervalMS != 5000 {
		t.Fatalf("QueueStatsIntervalMS = %d, want 5000", cfg.QueueStatsIntervalMS)
	}
	if !cfg.QueueCompactionEnabled || cfg.QueueCompactionIntervalMS != 30000 || cfg.QueueAckRetentionMS != 86400000 || cfg.QueueAckRetentionMax != 100000 {
		t.Fatalf("unexpected queue compaction defaults: enabled=%v interval=%d retention=%d max=%d", cfg.QueueCompactionEnabled, cfg.QueueCompactionIntervalMS, cfg.QueueAckRetentionMS, cfg.QueueAckRetentionMax)
	}
	if cfg.QueueDLQReplayLeaseMS != 30000 {
		t.Fatalf("unexpected DLQ replay lease default: %d", cfg.QueueDLQReplayLeaseMS)
	}
	if !cfg.DLQLifecycleEnabled || cfg.DLQLifecycleIntervalMS != 60000 || cfg.DLQReplayedRetentionMS != 604800000 || cfg.DLQQuarantineRetentionMS != 2592000000 || cfg.DLQAuditRetentionMS != 7776000000 || cfg.DLQMaxEntries != 100000 {
		t.Fatalf("unexpected DLQ lifecycle defaults: %+v", cfg)
	}
	if !cfg.DLQArchiveEnabled || cfg.DLQArchiveSink != "file" || cfg.DLQArchiveDir != "./data/dlq-archive" || cfg.DLQArchiveLeaseMS != 30000 || cfg.DLQArchiveHTTPBaseURL != "" || cfg.DLQArchiveHTTPToken != "" || cfg.DLQArchiveHTTPTimeoutMS != 10000 || cfg.DLQArchiveS3Endpoint != "" || cfg.DLQArchiveS3AccessKey != "" || cfg.DLQArchiveS3SecretKey != "" || cfg.DLQArchiveS3Bucket != "taskflow-dlq" || cfg.DLQArchiveS3Region != "us-east-1" || !cfg.DLQArchiveS3Secure {
		t.Fatalf("unexpected DLQ archive defaults: %+v", cfg)
	}
	if cfg.DLQReplicaEnabled || cfg.DLQReplicaSink != "file" || cfg.DLQReplicaDir != "./data/dlq-archive-replica" || cfg.DLQReplicaS3Endpoint != "" || cfg.DLQReplicaS3AccessKey != "" || cfg.DLQReplicaS3SecretKey != "" || cfg.DLQReplicaS3Bucket != "taskflow-dlq" || cfg.DLQReplicaS3Region != "us-east-1" || !cfg.DLQReplicaS3Secure || cfg.DLQArchiveMinCopies != 1 || !cfg.DLQObjLifecycleEnabled || cfg.DLQObjLifecycleIntervalMS != 3600000 || cfg.DLQObjRetentionMS != 31536000000 || cfg.DLQOrphanGraceMS != 300000 || cfg.DLQObjGCBatch != 50 || cfg.DLQOrphanBatch != 100 || cfg.DLQDrillIntervalMS != 86400000 || cfg.DLQDrillBatch != 3 {
		t.Fatalf("unexpected Phase 21 archive object lifecycle defaults: %+v", cfg)
	}
	if cfg.DrainFenceMS != 1000 || cfg.DrainTimeoutMS != 30000 {
		t.Fatalf("unexpected drain defaults: fence=%d timeout=%d", cfg.DrainFenceMS, cfg.DrainTimeoutMS)
	}
	if !cfg.ReconcileDistributed || cfg.ReconcileLockKey != "taskflow:reconciler:leader" || cfg.ReconcileLeaseMS != 15000 {
		t.Fatalf("unexpected reconciler coordination defaults: enabled=%v key=%q lease=%d", cfg.ReconcileDistributed, cfg.ReconcileLockKey, cfg.ReconcileLeaseMS)
	}
	if cfg.EnqueueLeaseMS != 10000 {
		t.Fatalf("unexpected enqueue visibility lease default: %d", cfg.EnqueueLeaseMS)
	}
	if cfg.MySQLMaxOpenConns != 50 || cfg.MySQLMaxIdleConns != 10 {
		t.Fatalf("unexpected mysql pool defaults: open=%d idle=%d", cfg.MySQLMaxOpenConns, cfg.MySQLMaxIdleConns)
	}
	if !cfg.MySQLSkipDefaultTx {
		t.Fatal("MySQLSkipDefaultTx = false, want true")
	}
	if cfg.ExecutionBackend != "local" {
		t.Fatalf("ExecutionBackend = %q, want local", cfg.ExecutionBackend)
	}
	if cfg.ExecutorMetricsAddr != ":9093" {
		t.Fatalf("ExecutorMetricsAddr = %q, want :9093", cfg.ExecutorMetricsAddr)
	}
	if cfg.AIMaxConcurrency != 4 || cfg.AIFallbackMaxConcurrency != 4 {
		t.Fatalf("unexpected AI concurrency defaults: primary=%d fallback=%d", cfg.AIMaxConcurrency, cfg.AIFallbackMaxConcurrency)
	}
	if cfg.AISLORealtimeQueueMS != 50 || cfg.AISLORealtimeTimeoutMS != 3000 || cfg.AISLOStandardQueueMS != 1000 || cfg.AISLOStandardTimeoutMS != 15000 || cfg.AISLOBatchQueueMS != 5000 || cfg.AISLOBatchTimeoutMS != 30000 {
		t.Fatalf("unexpected AI SLO defaults: %+v", cfg)
	}
	if cfg.AIProvider != "primary" || cfg.AIProviderKind != "gateway" || cfg.AIEndpoint != "" || cfg.AIBearerToken != "" || cfg.AIModel != "" || cfg.AICapabilities != "" || cfg.AIInputCostPer1M != 0 || cfg.AIOutputCostPer1M != 0 || cfg.AIFallbackProvider != "fallback" || cfg.AIFallbackKind != "gateway" || cfg.AIFallbackEndpoint != "" || cfg.AIFallbackToken != "" || cfg.AIFallbackModel != "" || cfg.AIFallbackCaps != "" || cfg.AIFallbackInputCostPer1M != 0 || cfg.AIFallbackOutputCostPer1M != 0 || cfg.AIRoutingStrategy != "balanced" || cfg.AIProviderCircuitFailures != 3 || cfg.AIProviderCircuitOpenMS != 10000 || cfg.AIDefaultOutputTokens != 256 || cfg.AIBudgetDefaultReserveUSD != 0.01 || cfg.AITimeoutMS != 30000 {
		t.Fatalf("unexpected AI defaults: %+v", cfg)
	}
	if cfg.AIGlobalConcurrencyLimit != 0 || cfg.AIFallbackGlobalLimit != 0 || cfg.AIDistributedLeaseMS != 30000 || cfg.AIConcurrencyRedisPrefix != "taskflow:ai:concurrency" || !cfg.AIAdaptiveLoadShedding || cfg.AIPressureQueueWarn != 4 || cfg.AIPressureQueueHigh != 16 || cfg.AIPressureFailWarn != 0.20 || cfg.AIPressureFailHigh != 0.50 || cfg.AIPressureLatencyWarnMS != 2000 || cfg.AIPressureLatencyHighMS != 5000 {
		t.Fatalf("unexpected Phase 7 defaults: %+v", cfg)
	}
	if cfg.AIHedgeEnabled || cfg.AIHedgeDelayMS != 100 || cfg.AIHedgeCancelWaitMS != 100 || cfg.AIHedgeMaxExtraCostUSD != 0.01 {
		t.Fatalf("unexpected Phase 8 defaults: %+v", cfg)
	}
	if cfg.AITenantFairScheduling || cfg.AITenantMaxConsecutive != 2 || cfg.AITenantStarvationMS != 5000 {
		t.Fatalf("unexpected Phase 9 defaults: %+v", cfg)
	}
	if cfg.AITenantQuotaEnabled || cfg.AITenantQuotaLimit != 0 || cfg.AITenantQuotaOverrides != "" {
		t.Fatalf("unexpected Phase 10 defaults: %+v", cfg)
	}
	if cfg.ControlPlaneAuthEnabled || cfg.ControlPlaneBearerToken != "" || cfg.ControlPlanePrevToken != "" || cfg.ControlPlaneOperatorID != "operator" || cfg.ControlPlanePreviousID != "" || !cfg.ControlPlaneAuditEnabled || !cfg.ControlAuditRetention || cfg.ControlAuditLifecycleMS != 3600000 || cfg.ControlAuditRetentionMS != 7776000000 || cfg.ControlAuditRetainBatch != 1000 || cfg.ControlSIEMEnabled || cfg.ControlSIEMEndpoint != "" || cfg.ControlSIEMToken != "" || cfg.ControlSIEMSignKey != "" || cfg.ControlSIEMSignKeyID != "taskflow-hmac-v1" || cfg.ControlSIEMIntervalMS != 1000 || cfg.ControlSIEMBatch != 100 || cfg.ControlSIEMLeaseMS != 30000 || cfg.ControlSIEMMaxAttempts != 8 || cfg.ControlSIEMRetryBaseMS != 1000 || cfg.ControlSIEMRetryMaxMS != 60000 || cfg.ControlSIEMHTTPTimeoutMS != 5000 || cfg.SIEMSignProvider != "hmac" || cfg.SIEMPrevSignKey != "" || cfg.SIEMPrevSignKeyID != "" || cfg.SIEMRemoteEndpoint != "" || cfg.SIEMRemoteToken != "" || cfg.SIEMRemoteAlgo != "hmac-sha256-digest" || cfg.SIEMRemoteTimeoutMS != 3000 || cfg.SIEMCoordEnabled || cfg.SIEMCoordKey != "taskflow:control-plane:siem:leader" || cfg.SIEMCoordLeaseMS != 15000 || !cfg.SIEMGCEnabled || cfg.SIEMGCIntervalMS != 3600000 || cfg.SIEMDeliveredKeepMS != 2592000000 || cfg.SIEMDLQKeepMS != 7776000000 || cfg.SIEMGCBatch != 1000 {
		t.Fatalf("unexpected Phase 26 control-plane defaults: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config validation failed: %v", err)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("WORKER_COUNT", "16")
	t.Setenv("QUEUE_STATS_INTERVAL_MS", "750")
	t.Setenv("QUEUE_COMPACTION_ENABLED", "false")
	t.Setenv("QUEUE_COMPACTION_INTERVAL_MS", "450")
	t.Setenv("QUEUE_ACK_RETENTION_MS", "1500")
	t.Setenv("QUEUE_ACK_RETENTION_MAX_ENTRIES", "7")
	t.Setenv("QUEUE_DLQ_REPLAY_LEASE_MS", "4200")
	t.Setenv("QUEUE_DLQ_LIFECYCLE_ENABLED", "false")
	t.Setenv("QUEUE_DLQ_LIFECYCLE_INTERVAL_MS", "650")
	t.Setenv("QUEUE_DLQ_REPLAYED_RETENTION_MS", "1200")
	t.Setenv("QUEUE_DLQ_QUARANTINED_RETENTION_MS", "2400")
	t.Setenv("QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS", "3600")
	t.Setenv("QUEUE_DLQ_MAX_ENTRIES", "9")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ENABLED", "false")
	t.Setenv("QUEUE_DLQ_ARCHIVE_SINK", "http")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DIR", "/tmp/phase20-archive")
	t.Setenv("QUEUE_DLQ_ARCHIVE_LEASE_MS", "4200")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_BASE_URL", "http://archive:9000/bucket")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_BEARER_TOKEN", "phase20-token")
	t.Setenv("QUEUE_DLQ_ARCHIVE_HTTP_TIMEOUT_MS", "2500")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_ENDPOINT", "primary:9000")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_ACCESS_KEY", "primary-key")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_SECRET_KEY", "primary-secret")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_BUCKET", "primary-bucket")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_REGION", "eu-west-1")
	t.Setenv("QUEUE_DLQ_ARCHIVE_S3_SECURE", "false")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED", "true")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_SINK", "http")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_DIR", "/tmp/phase21-replica")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BASE_URL", "http://archive-replica:9000/bucket")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BEARER_TOKEN", "phase21-token")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_TIMEOUT_MS", "2600")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT", "replica:9000")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ACCESS_KEY", "replica-key")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECRET_KEY", "replica-secret")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_BUCKET", "replica-bucket")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_REGION", "ap-southeast-1")
	t.Setenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_SECURE", "false")
	t.Setenv("QUEUE_DLQ_ARCHIVE_MIN_COPIES", "2")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED", "false")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_INTERVAL_MS", "700")
	t.Setenv("QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS", "1200")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS", "1300")
	t.Setenv("QUEUE_DLQ_ARCHIVE_GC_BATCH", "4")
	t.Setenv("QUEUE_DLQ_ARCHIVE_ORPHAN_BATCH", "5")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DRILL_INTERVAL_MS", "1400")
	t.Setenv("QUEUE_DLQ_ARCHIVE_DRILL_BATCH", "6")
	t.Setenv("DRAIN_FENCE_MS", "250")
	t.Setenv("DRAIN_TIMEOUT_MS", "9000")
	t.Setenv("RECONCILE_DISTRIBUTED_COORDINATION", "false")
	t.Setenv("RECONCILE_LOCK_KEY", "custom:reconciler")
	t.Setenv("RECONCILE_LEASE_MS", "4500")
	t.Setenv("ENQUEUE_VISIBILITY_LEASE_MS", "2750")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("EXECUTION_BACKEND", "grpc")
	t.Setenv("EXECUTOR_GRPC_TARGET", "worker:9091")
	t.Setenv("EXECUTOR_TIMEOUT_MS", "2500")
	t.Setenv("EXECUTOR_METRICS_ADDR", ":9193")
	t.Setenv("MYSQL_SKIP_DEFAULT_TX", "false")
	t.Setenv("AI_PROVIDER", "mock-primary")
	t.Setenv("AI_PROVIDER_KIND", "openai")
	t.Setenv("AI_ENDPOINT", "http://ai-gateway:9000/generate")
	t.Setenv("AI_BEARER_TOKEN", "test-token")
	t.Setenv("AI_MODEL", "model-a")
	t.Setenv("AI_CAPABILITIES", "text,reasoning")
	t.Setenv("AI_INPUT_COST_PER_1M", "1.25")
	t.Setenv("AI_OUTPUT_COST_PER_1M", "5.5")
	t.Setenv("AI_GLOBAL_MAX_CONCURRENCY", "7")
	t.Setenv("AI_MAX_CONCURRENCY", "7")
	t.Setenv("AI_FALLBACK_PROVIDER", "mock-fallback")
	t.Setenv("AI_FALLBACK_KIND", "gateway")
	t.Setenv("AI_FALLBACK_ENDPOINT", "http://ai-gateway-fallback:9000/generate")
	t.Setenv("AI_FALLBACK_BEARER_TOKEN", "fallback-token")
	t.Setenv("AI_FALLBACK_MODEL", "model-b")
	t.Setenv("AI_FALLBACK_CAPABILITIES", "text")
	t.Setenv("AI_FALLBACK_INPUT_COST_PER_1M", "0.2")
	t.Setenv("AI_FALLBACK_OUTPUT_COST_PER_1M", "1.1")
	t.Setenv("AI_FALLBACK_GLOBAL_MAX_CONCURRENCY", "3")
	t.Setenv("AI_FALLBACK_MAX_CONCURRENCY", "3")
	t.Setenv("AI_ROUTING_STRATEGY", "cost")
	t.Setenv("AI_PROVIDER_CIRCUIT_FAILURES", "4")
	t.Setenv("AI_PROVIDER_CIRCUIT_OPEN_MS", "15000")
	t.Setenv("AI_DEFAULT_OUTPUT_TOKENS", "512")
	t.Setenv("AI_DISTRIBUTED_LEASE_MS", "4500")
	t.Setenv("AI_CONCURRENCY_REDIS_PREFIX", "custom:leases")
	t.Setenv("AI_ADAPTIVE_LOAD_SHEDDING", "false")
	t.Setenv("AI_PRESSURE_DEGRADED_QUEUE", "2")
	t.Setenv("AI_PRESSURE_OVERLOADED_QUEUE", "8")
	t.Setenv("AI_PRESSURE_DEGRADED_FAILURE_RATE", "0.1")
	t.Setenv("AI_PRESSURE_OVERLOADED_FAILURE_RATE", "0.4")
	t.Setenv("AI_PRESSURE_DEGRADED_LATENCY_MS", "900")
	t.Setenv("AI_PRESSURE_OVERLOADED_LATENCY_MS", "2500")
	t.Setenv("AI_HEDGE_ENABLED", "true")
	t.Setenv("AI_HEDGE_DELAY_MS", "75")
	t.Setenv("AI_HEDGE_CANCEL_WAIT_MS", "125")
	t.Setenv("AI_HEDGE_MAX_EXTRA_COST_USD", "0.02")
	t.Setenv("AI_SLO_REALTIME_QUEUE_MS", "25")
	t.Setenv("AI_SLO_REALTIME_TIMEOUT_MS", "1500")
	t.Setenv("AI_SLO_STANDARD_QUEUE_MS", "750")
	t.Setenv("AI_SLO_STANDARD_TIMEOUT_MS", "9000")
	t.Setenv("AI_SLO_BATCH_QUEUE_MS", "7000")
	t.Setenv("AI_SLO_BATCH_TIMEOUT_MS", "45000")
	t.Setenv("AI_TENANT_FAIR_SCHEDULING", "true")
	t.Setenv("AI_TENANT_MAX_CONSECUTIVE", "3")
	t.Setenv("AI_TENANT_STARVATION_MS", "750")
	t.Setenv("AI_DISTRIBUTED_TENANT_QUOTA", "true")
	t.Setenv("AI_TENANT_GLOBAL_MAX_CONCURRENCY", "2")
	t.Setenv("AI_TENANT_CONCURRENCY_OVERRIDES", "gold=4,silver=2")
	t.Setenv("AI_BUDGET_DEFAULT_RESERVE_USD", "0.025")
	t.Setenv("AI_TIMEOUT_MS", "45000")
	t.Setenv("CONTROL_PLANE_AUTH_ENABLED", "true")
	t.Setenv("CONTROL_PLANE_BEARER_TOKEN", "phase23-active-token")
	t.Setenv("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN", "phase23-previous-token")
	t.Setenv("CONTROL_PLANE_OPERATOR_ID", "platform-admin-v2")
	t.Setenv("CONTROL_PLANE_PREVIOUS_OPERATOR_ID", "platform-admin-v1")
	t.Setenv("CONTROL_PLANE_AUDIT_ENABLED", "false")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_ENABLED", "false")
	t.Setenv("CONTROL_PLANE_AUDIT_LIFECYCLE_INTERVAL_MS", "900")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_MS", "5000")
	t.Setenv("CONTROL_PLANE_AUDIT_RETENTION_BATCH", "17")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_ENABLED", "true")
	t.Setenv("CONTROL_PLANE_SIEM_ENDPOINT", "https://siem.example.test/events")
	t.Setenv("CONTROL_PLANE_SIEM_BEARER_TOKEN", "phase25-sink-token")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_KEY", "phase25-signing-key")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_KEY_ID", "phase25-key-v1")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_INTERVAL_MS", "250")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_BATCH", "23")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERY_LEASE_MS", "4500")
	t.Setenv("CONTROL_PLANE_SIEM_MAX_ATTEMPTS", "5")
	t.Setenv("CONTROL_PLANE_SIEM_RETRY_BASE_MS", "300")
	t.Setenv("CONTROL_PLANE_SIEM_RETRY_MAX_MS", "2400")
	t.Setenv("CONTROL_PLANE_SIEM_HTTP_TIMEOUT_MS", "1800")
	t.Setenv("CONTROL_PLANE_SIEM_SIGNING_PROVIDER", "hmac")
	t.Setenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY", "phase26-previous-signing-key")
	t.Setenv("CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY_ID", "phase26-key-v0")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ENDPOINT", "http://kms-signer:9701/sign")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_BEARER_TOKEN", "phase26-kms-token")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_ALGORITHM", "hmac-sha256-digest")
	t.Setenv("CONTROL_PLANE_SIEM_REMOTE_SIGNER_TIMEOUT_MS", "1700")
	t.Setenv("CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION", "true")
	t.Setenv("CONTROL_PLANE_SIEM_COORDINATION_KEY", "custom:siem:leader")
	t.Setenv("CONTROL_PLANE_SIEM_COORDINATION_LEASE_MS", "4100")
	t.Setenv("CONTROL_PLANE_SIEM_GC_ENABLED", "false")
	t.Setenv("CONTROL_PLANE_SIEM_GC_INTERVAL_MS", "1300")
	t.Setenv("CONTROL_PLANE_SIEM_DELIVERED_RETENTION_MS", "6100")
	t.Setenv("CONTROL_PLANE_SIEM_REPLAYED_DLQ_RETENTION_MS", "7200")
	t.Setenv("CONTROL_PLANE_SIEM_GC_BATCH", "19")

	cfg := Load()
	if cfg.AIMaxConcurrency != 7 || cfg.AIFallbackMaxConcurrency != 3 || cfg.AISLORealtimeQueueMS != 25 || cfg.AISLORealtimeTimeoutMS != 1500 || cfg.AISLOStandardQueueMS != 750 || cfg.AISLOStandardTimeoutMS != 9000 || cfg.AISLOBatchQueueMS != 7000 || cfg.AISLOBatchTimeoutMS != 45000 {
		t.Fatalf("unexpected AI SLO/concurrency overrides: %+v", cfg)
	}
	if cfg.HTTPAddr != ":9090" || cfg.WorkerCount != 16 || cfg.QueueStatsIntervalMS != 750 || cfg.DrainFenceMS != 250 || cfg.DrainTimeoutMS != 9000 || cfg.RedisDB != 2 || cfg.ExecutionBackend != "grpc" || cfg.ExecutorGRPCTarget != "worker:9091" || cfg.ExecutorTimeoutMS != 2500 || cfg.ExecutorMetricsAddr != ":9193" || cfg.MySQLSkipDefaultTx || cfg.AIProvider != "mock-primary" || cfg.AIProviderKind != "openai" || cfg.AIEndpoint != "http://ai-gateway:9000/generate" || cfg.AIBearerToken != "test-token" || cfg.AIModel != "model-a" || cfg.AICapabilities != "text,reasoning" || cfg.AIInputCostPer1M != 1.25 || cfg.AIOutputCostPer1M != 5.5 || cfg.AIFallbackProvider != "mock-fallback" || cfg.AIFallbackKind != "gateway" || cfg.AIFallbackEndpoint != "http://ai-gateway-fallback:9000/generate" || cfg.AIFallbackToken != "fallback-token" || cfg.AIFallbackModel != "model-b" || cfg.AIFallbackCaps != "text" || cfg.AIFallbackInputCostPer1M != 0.2 || cfg.AIFallbackOutputCostPer1M != 1.1 || cfg.AIRoutingStrategy != "cost" || cfg.AIProviderCircuitFailures != 4 || cfg.AIProviderCircuitOpenMS != 15000 || cfg.AIDefaultOutputTokens != 512 || cfg.AIBudgetDefaultReserveUSD != 0.025 || cfg.AITimeoutMS != 45000 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.ReconcileDistributed || cfg.ReconcileLockKey != "custom:reconciler" || cfg.ReconcileLeaseMS != 4500 {
		t.Fatalf("unexpected reconciler coordination overrides: enabled=%v key=%q lease=%d", cfg.ReconcileDistributed, cfg.ReconcileLockKey, cfg.ReconcileLeaseMS)
	}
	if cfg.EnqueueLeaseMS != 2750 {
		t.Fatalf("unexpected enqueue visibility lease override: %d", cfg.EnqueueLeaseMS)
	}
	if cfg.QueueCompactionEnabled || cfg.QueueCompactionIntervalMS != 450 || cfg.QueueAckRetentionMS != 1500 || cfg.QueueAckRetentionMax != 7 {
		t.Fatalf("unexpected queue compaction overrides: enabled=%v interval=%d retention=%d max=%d", cfg.QueueCompactionEnabled, cfg.QueueCompactionIntervalMS, cfg.QueueAckRetentionMS, cfg.QueueAckRetentionMax)
	}
	if cfg.QueueDLQReplayLeaseMS != 4200 {
		t.Fatalf("unexpected DLQ replay lease override: %d", cfg.QueueDLQReplayLeaseMS)
	}
	if cfg.DLQLifecycleEnabled || cfg.DLQLifecycleIntervalMS != 650 || cfg.DLQReplayedRetentionMS != 1200 || cfg.DLQQuarantineRetentionMS != 2400 || cfg.DLQAuditRetentionMS != 3600 || cfg.DLQMaxEntries != 9 {
		t.Fatalf("unexpected DLQ lifecycle overrides: %+v", cfg)
	}
	if cfg.DLQArchiveEnabled || cfg.DLQArchiveSink != "http" || cfg.DLQArchiveDir != "/tmp/phase20-archive" || cfg.DLQArchiveLeaseMS != 4200 || cfg.DLQArchiveHTTPBaseURL != "http://archive:9000/bucket" || cfg.DLQArchiveHTTPToken != "phase20-token" || cfg.DLQArchiveHTTPTimeoutMS != 2500 || cfg.DLQArchiveS3Endpoint != "primary:9000" || cfg.DLQArchiveS3AccessKey != "primary-key" || cfg.DLQArchiveS3SecretKey != "primary-secret" || cfg.DLQArchiveS3Bucket != "primary-bucket" || cfg.DLQArchiveS3Region != "eu-west-1" || cfg.DLQArchiveS3Secure {
		t.Fatalf("unexpected DLQ archive overrides: %+v", cfg)
	}
	if !cfg.DLQReplicaEnabled || cfg.DLQReplicaSink != "http" || cfg.DLQReplicaDir != "/tmp/phase21-replica" || cfg.DLQReplicaHTTPBaseURL != "http://archive-replica:9000/bucket" || cfg.DLQReplicaHTTPToken != "phase21-token" || cfg.DLQReplicaHTTPTimeoutMS != 2600 || cfg.DLQReplicaS3Endpoint != "replica:9000" || cfg.DLQReplicaS3AccessKey != "replica-key" || cfg.DLQReplicaS3SecretKey != "replica-secret" || cfg.DLQReplicaS3Bucket != "replica-bucket" || cfg.DLQReplicaS3Region != "ap-southeast-1" || cfg.DLQReplicaS3Secure || cfg.DLQArchiveMinCopies != 2 || cfg.DLQObjLifecycleEnabled || cfg.DLQObjLifecycleIntervalMS != 700 || cfg.DLQObjRetentionMS != 1200 || cfg.DLQOrphanGraceMS != 1300 || cfg.DLQObjGCBatch != 4 || cfg.DLQOrphanBatch != 5 || cfg.DLQDrillIntervalMS != 1400 || cfg.DLQDrillBatch != 6 {
		t.Fatalf("unexpected Phase 21 archive object lifecycle overrides: %+v", cfg)
	}
	if cfg.AIGlobalConcurrencyLimit != 7 || cfg.AIFallbackGlobalLimit != 3 || cfg.AIDistributedLeaseMS != 4500 || cfg.AIConcurrencyRedisPrefix != "custom:leases" || cfg.AIAdaptiveLoadShedding || cfg.AIPressureQueueWarn != 2 || cfg.AIPressureQueueHigh != 8 || cfg.AIPressureFailWarn != 0.1 || cfg.AIPressureFailHigh != 0.4 || cfg.AIPressureLatencyWarnMS != 900 || cfg.AIPressureLatencyHighMS != 2500 {
		t.Fatalf("unexpected Phase 7 config: %+v", cfg)
	}
	if !cfg.AIHedgeEnabled || cfg.AIHedgeDelayMS != 75 || cfg.AIHedgeCancelWaitMS != 125 || cfg.AIHedgeMaxExtraCostUSD != 0.02 {
		t.Fatalf("unexpected Phase 8 config: %+v", cfg)
	}
	if !cfg.AITenantFairScheduling || cfg.AITenantMaxConsecutive != 3 || cfg.AITenantStarvationMS != 750 {
		t.Fatalf("unexpected Phase 9 config: %+v", cfg)
	}
	if !cfg.AITenantQuotaEnabled || cfg.AITenantQuotaLimit != 2 || cfg.AITenantQuotaOverrides != "gold=4,silver=2" {
		t.Fatalf("unexpected Phase 10 config: %+v", cfg)
	}
	if !cfg.ControlPlaneAuthEnabled || cfg.ControlPlaneBearerToken != "phase23-active-token" || cfg.ControlPlanePrevToken != "phase23-previous-token" || cfg.ControlPlaneOperatorID != "platform-admin-v2" || cfg.ControlPlanePreviousID != "platform-admin-v1" || cfg.ControlPlaneAuditEnabled || cfg.ControlAuditRetention || cfg.ControlAuditLifecycleMS != 900 || cfg.ControlAuditRetentionMS != 5000 || cfg.ControlAuditRetainBatch != 17 || !cfg.ControlSIEMEnabled || cfg.ControlSIEMEndpoint != "https://siem.example.test/events" || cfg.ControlSIEMToken != "phase25-sink-token" || cfg.ControlSIEMSignKey != "phase25-signing-key" || cfg.ControlSIEMSignKeyID != "phase25-key-v1" || cfg.ControlSIEMIntervalMS != 250 || cfg.ControlSIEMBatch != 23 || cfg.ControlSIEMLeaseMS != 4500 || cfg.ControlSIEMMaxAttempts != 5 || cfg.ControlSIEMRetryBaseMS != 300 || cfg.ControlSIEMRetryMaxMS != 2400 || cfg.ControlSIEMHTTPTimeoutMS != 1800 || cfg.SIEMSignProvider != "hmac" || cfg.SIEMPrevSignKey != "phase26-previous-signing-key" || cfg.SIEMPrevSignKeyID != "phase26-key-v0" || cfg.SIEMRemoteEndpoint != "http://kms-signer:9701/sign" || cfg.SIEMRemoteToken != "phase26-kms-token" || cfg.SIEMRemoteAlgo != "hmac-sha256-digest" || cfg.SIEMRemoteTimeoutMS != 1700 || !cfg.SIEMCoordEnabled || cfg.SIEMCoordKey != "custom:siem:leader" || cfg.SIEMCoordLeaseMS != 4100 || cfg.SIEMGCEnabled || cfg.SIEMGCIntervalMS != 1300 || cfg.SIEMDeliveredKeepMS != 6100 || cfg.SIEMDLQKeepMS != 7200 || cfg.SIEMGCBatch != 19 {
		t.Fatalf("unexpected Phase 26 control-plane config: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("override config validation failed: %v", err)
	}
}

func TestValidateControlPlaneAuthRequiresToken(t *testing.T) {
	cfg := Config{ControlPlaneAuthEnabled: true, ControlPlaneOperatorID: "operator"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing control-plane token error")
	}

	cfg.ControlPlaneBearerToken = "   "
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil for whitespace token")
	}

	cfg.ControlPlaneBearerToken = "secret"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with token error = %v", err)
	}
}

func TestValidateControlPlaneRotation(t *testing.T) {
	cfg := Config{
		ControlPlaneAuthEnabled: true,
		ControlPlaneBearerToken: "active",
		ControlPlanePrevToken:   "previous",
		ControlPlaneOperatorID:  "operator-v2",
		ControlPlanePreviousID:  "operator-v1",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid rotation config rejected: %v", err)
	}

	cfg.ControlPlanePrevToken = "active"
	if err := cfg.Validate(); err == nil {
		t.Fatal("same active/previous token accepted")
	}

	cfg.ControlPlanePrevToken = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("previous operator without previous token accepted")
	}

	cfg = Config{ControlPlanePrevToken: "previous", ControlPlaneOperatorID: "operator"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("previous token accepted while auth disabled")
	}
}

func TestValidateControlPlaneSIEMDelivery(t *testing.T) {
	cfg := Config{
		ControlSIEMEnabled:     true,
		ControlSIEMEndpoint:    "http://siem:8080/events",
		ControlSIEMSignKey:     "secret",
		ControlSIEMSignKeyID:   "key-v1",
		ControlSIEMRetryBaseMS: 100,
		ControlSIEMRetryMaxMS:  1000,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid SIEM config rejected: %v", err)
	}

	bad := cfg
	bad.ControlSIEMEndpoint = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("missing SIEM endpoint unexpectedly accepted")
	}
	bad = cfg
	bad.ControlSIEMEndpoint = "siem:8080/events"
	if err := bad.Validate(); err == nil {
		t.Fatal("relative SIEM endpoint unexpectedly accepted")
	}
	bad = cfg
	bad.ControlSIEMSignKey = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("missing SIEM signing key unexpectedly accepted")
	}
	bad = cfg
	bad.ControlSIEMRetryMaxMS = 50
	if err := bad.Validate(); err == nil {
		t.Fatal("SIEM retry max below base unexpectedly accepted")
	}

	rotating := cfg
	rotating.SIEMSignProvider = "hmac"
	rotating.SIEMPrevSignKey = "previous-secret"
	rotating.SIEMPrevSignKeyID = "key-v0"
	if err := rotating.Validate(); err != nil {
		t.Fatalf("valid SIEM HMAC rotation rejected: %v", err)
	}
	rotating.SIEMPrevSignKey = "secret"
	if err := rotating.Validate(); err == nil {
		t.Fatal("identical active/previous SIEM signing keys unexpectedly accepted")
	}

	remote := cfg
	remote.SIEMSignProvider = "remote"
	remote.ControlSIEMSignKey = ""
	remote.SIEMRemoteEndpoint = "http://kms-signer:9701/sign"
	remote.SIEMRemoteAlgo = "rsa-pss-sha256-digest"
	if err := remote.Validate(); err != nil {
		t.Fatalf("valid remote SIEM signer rejected: %v", err)
	}
	remote.SIEMRemoteEndpoint = ""
	if err := remote.Validate(); err == nil {
		t.Fatal("missing remote SIEM signer endpoint unexpectedly accepted")
	}

	awsKMS := cfg
	awsKMS.SIEMSignProvider = "aws-kms"
	awsKMS.ControlSIEMSignKey = ""
	awsKMS.SIEMAWSKMSRegion = "ap-southeast-1"
	awsKMS.SIEMAWSKMSAccessKey = "AKID"
	awsKMS.SIEMAWSKMSSecretKey = "SECRET"
	awsKMS.SIEMAWSKMSKeyID = "alias/taskflow-audit"
	if err := awsKMS.Validate(); err != nil {
		t.Fatalf("valid AWS KMS SIEM signer rejected: %v", err)
	}
	awsKMS.SIEMAWSKMSRegion = ""
	if err := awsKMS.Validate(); err == nil {
		t.Fatal("missing AWS KMS region unexpectedly accepted")
	}

	dr := cfg
	dr.SIEMDREnabled = true
	dr.SIEMDRMinCopies = 2
	dr.SIEMDRPrimaryEndpoint = "minio-primary:9000"
	dr.SIEMDRPrimaryAccessKey = "primary"
	dr.SIEMDRPrimarySecretKey = "primary-secret"
	dr.SIEMDRReplicaEnabled = true
	dr.SIEMDRReplicaEndpoint = "minio-replica:9000"
	dr.SIEMDRReplicaAccessKey = "replica"
	dr.SIEMDRReplicaSecretKey = "replica-secret"
	if err := dr.Validate(); err != nil {
		t.Fatalf("valid SIEM DR configuration rejected: %v", err)
	}
	dr.SIEMDRReplicaEnabled = false
	if err := dr.Validate(); err == nil {
		t.Fatal("DR min copies above configured sinks unexpectedly accepted")
	}

	coordinated := cfg
	coordinated.SIEMCoordEnabled = true
	coordinated.SIEMCoordKey = ""
	if err := coordinated.Validate(); err == nil {
		t.Fatal("empty SIEM coordination key unexpectedly accepted")
	}
}

func TestInvalidPositiveIntegerFallsBack(t *testing.T) {
	t.Setenv("WORKER_COUNT", "0")
	if got := Load().WorkerCount; got != 8 {
		t.Fatalf("WorkerCount = %d, want fallback 8", got)
	}
}
