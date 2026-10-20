package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "http_requests_total", Help: "HTTP requests by method, path and status.",
	}, []string{"method", "path", "status"})

	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "http_request_duration_seconds", Help: "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	TasksProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "tasks_processed_total", Help: "Settled task executions by result.",
	}, []string{"result"})

	ProcessDraining = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "process_draining", Help: "Whether this process is draining and refusing new execution work (1=draining).",
	})
	WorkerInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "worker_inflight", Help: "TaskFlow queue tasks currently executing in this process.",
	})
	ExecutorInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "executor_inflight", Help: "Executor gRPC requests currently executing in this process.",
	})
	DrainOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "drain_outcomes_total", Help: "Graceful drain completion outcomes by process role.",
	}, []string{"role", "outcome"})
	ReconcilerCoordination = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "reconciler_coordination_total", Help: "Distributed reconciler leadership outcomes.",
	}, []string{"outcome"})
	ReconcilerRecovered = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "reconciler_recovered_tasks_total", Help: "Stale running tasks recovered by the active reconciler leader.",
	})
	ReconcilerEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "reconciler_enqueued_tasks_total", Help: "Ready pending tasks enqueued by the active reconciler leader.",
	})
	CloudSagaRunnable = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "cloud_saga_runnable_workflows", Help: "Runnable cloud Saga workflows observed by the latest controller scan.",
	})
	CloudSagaClaims = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "cloud_saga_workflow_claims_total", Help: "Cloud Saga workflow lease claim outcomes across controller/API replicas.",
	}, []string{"outcome"})
	CloudSagaStepOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "cloud_saga_step_operations_total", Help: "Instrumented cloud Saga provider-step outcomes with bounded workflow/step/direction labels.",
	}, []string{"kind", "step", "direction", "outcome"})
	CloudSagaStepDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "cloud_saga_step_duration_seconds", Help: "Instrumented cloud Saga provider-step latency with bounded labels.",
		Buckets: prometheus.DefBuckets,
	}, []string{"kind", "step", "direction"})

	GRPCExecutorCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "grpc_executor_calls_total", Help: "gRPC executor calls by result.",
	}, []string{"result"})

	GRPCExecutorDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "grpc_executor_duration_seconds", Help: "gRPC executor call latency.",
		Buckets: prometheus.DefBuckets,
	})

	AIProviderCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_provider_calls_total", Help: "AI provider calls by provider and result.",
	}, []string{"provider", "result"})

	AIProviderDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "ai_provider_duration_seconds", Help: "AI provider call latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"provider"})

	AITokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_tokens_total", Help: "AI token usage reported by providers.",
	}, []string{"provider", "kind"})

	AICostUSD = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_cost_usd_total", Help: "AI cost in USD reported by providers.",
	}, []string{"provider"})

	AIFallbacks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_fallback_total", Help: "AI provider fallback attempts.",
	}, []string{"from", "to"})

	AIRouteSelections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_route_selections_total", Help: "AI routing selections by provider, strategy and reason.",
	}, []string{"provider", "strategy", "reason"})

	AIProviderCircuitSkips = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_provider_circuit_skips_total", Help: "AI provider attempts skipped because the provider circuit is open.",
	}, []string{"provider"})

	AIModelConcurrencyActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_model_concurrency_active", Help: "Active AI requests per provider/model.",
	}, []string{"provider", "model"})
	AIModelConcurrencyQueued = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_model_concurrency_queued", Help: "Queued AI requests per provider/model.",
	}, []string{"provider", "model"})
	AIModelQueueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "ai_model_queue_wait_seconds", Help: "Time spent waiting for AI provider/model concurrency slots.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"provider", "model", "slo_class"})
	AIBackpressure = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_backpressure_total", Help: "AI backpressure outcomes by provider/model and SLO class.",
	}, []string{"provider", "model", "slo_class", "outcome"})
	AISLORequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_slo_requests_total", Help: "AI request outcomes by SLO class.",
	}, []string{"slo_class", "result"})
	AIGlobalConcurrencyActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_global_concurrency_active", Help: "Last observed global active AI leases per provider/model.",
	}, []string{"provider", "model"})
	AIGlobalConcurrencyLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_global_concurrency_limit", Help: "Configured distributed AI concurrency limit per provider/model.",
	}, []string{"provider", "model"})
	AIGlobalConcurrency = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_global_concurrency_total", Help: "Distributed AI concurrency admission outcomes.",
	}, []string{"provider", "model", "slo_class", "outcome"})
	AILoadShedding = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_load_shedding_total", Help: "Adaptive AI load shedding decisions.",
	}, []string{"slo_class", "pressure", "action"})
	AISystemPressureState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_system_pressure_state", Help: "One-hot executor AI pressure state.",
	}, []string{"state"})
	AIExecutorQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "ai_executor_queue_depth", Help: "Total local AI provider/model concurrency waiters in this executor instance.",
	})
	AIHedgeRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_hedge_requests_total", Help: "Hedged AI request outcomes.",
	}, []string{"outcome"})
	AIHedgeDelay = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "taskflow", Name: "ai_hedge_delay_seconds", Help: "Configured delay before launching a hedge request.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 10),
	})
	AIHedgeExtraCostUSD = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_hedge_extra_cost_usd_total", Help: "Known extra AI cost caused by completed losing hedge attempts.",
	})
	AITenantFairScheduling = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_tenant_fair_scheduling_total", Help: "Tenant-aware AI scheduling decisions without tenant-id labels.",
	}, []string{"action"})
	AITenantDistributedConcurrency = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_tenant_distributed_concurrency_total", Help: "Distributed tenant AI concurrency quota outcomes without tenant-id labels.",
	}, []string{"provider", "model", "slo_class", "outcome"})

	AIBudgetAdmissions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_budget_admissions_total", Help: "AI budget admission decisions.",
	}, []string{"result", "reason"})

	AIBudgetSettledUSD = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_budget_settled_usd_total", Help: "AI budget spend settled in USD.",
	})

	AIBudgetSettledTokens = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_budget_settled_tokens_total", Help: "AI budget token usage settled after execution.",
	})

	AIBudgetOverruns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "ai_budget_overrun_total", Help: "Executions whose actual usage exceeded the admission reservation.",
	}, []string{"kind"})
	ControlPlaneAudits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_audits_total", Help: "Control-plane audit persistence and request outcomes without operator-id labels.",
	}, []string{"method", "outcome"})
	ControlPlaneSIEMDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_deliveries_total", Help: "Control-plane SIEM delivery outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMDeliveryBacklog = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_delivery_backlog", Help: "Pending or leased control-plane audit deliveries awaiting SIEM acknowledgement.",
	})
	ControlPlaneSIEMDeliveryDLQ = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_delivery_dlq", Help: "Control-plane audit deliveries currently in the SIEM delivery DLQ.",
	})
	ControlPlaneSIEMLeadership = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_leadership_total", Help: "Distributed SIEM delivery leadership outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_leader", Help: "Whether this process currently holds SIEM delivery leadership (1=leader).",
	})
	ControlPlaneSIEMLeaderFence = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_leader_fence", Help: "Latest SIEM delivery fencing token activated by this process.",
	})
	ControlPlaneSIEMLifecycle = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_lifecycle_total", Help: "SIEM delivery history retention lifecycle outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMPurged = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_purged_total", Help: "SIEM delivery history rows purged by retention class.",
	}, []string{"class"})
	ControlPlaneSIEMAcks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_ack_total", Help: "Validated SIEM receiver acknowledgement outcomes.",
	}, []string{"status"})
	ControlPlaneSIEMDR = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_dr_total", Help: "Cross-region SIEM evidence replication outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMDRCopies = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_dr_copies", Help: "Verified evidence copies for the most recent SIEM batch.",
	})
	ControlPlaneSIEMReconcile = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_reconcile_total", Help: "Receiver-checkpoint reconciliation outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMRecoveredReceipts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_recovered_receipts_total", Help: "SIEM delivery receipts reconstructed from receiver checkpoints.",
	})
	ControlPlaneSIEMRecoveryEvidence = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_recovery_evidence_total", Help: "Verified SIEM recovery evidence by bounded DR source.",
	}, []string{"source"})
	ControlPlaneSIEMReceiverTerm = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_receiver_term", Help: "Current durable SIEM receiver routing term.",
	})
	ControlPlaneSIEMReceiverRPO = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_receiver_rpo_sequences", Help: "Sequence lag between active and standby SIEM receivers.",
	})
	ControlPlaneSIEMAntiEntropy = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_anti_entropy_total", Help: "Receiver anti-entropy synchronization outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMAntiEntropyReplayed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_anti_entropy_replayed_total", Help: "Receiver events replayed only to a lagging standby during anti-entropy.",
	})
	ControlPlaneSIEMSwitchover = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_switchover_total", Help: "Operator or automatic receiver switchover outcomes.",
	}, []string{"target", "outcome"})
	ControlPlaneSIEMSwitchoverRTO = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_switchover_rto_seconds", Help: "Observed duration of the most recent controlled receiver switchover.",
	})
	ControlPlaneSIEMRoutingRecovery = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_routing_recovery_total", Help: "Durable receiver routing states reconstructed from agreeing signed receiver checkpoints.",
	})
	ControlPlaneSIEMPromotionIntents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_promotion_intents_total", Help: "Two-person receiver promotion intent workflow outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMPromotionExecution = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_promotion_execution_total", Help: "Coordinated receiver promotion execution outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMRPOFence = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_rpo_fenced", Help: "Whether receiver promotion/failover is fenced by the durable RPO SLO policy (1=fenced).",
	})
	ControlPlaneSIEMRPOBreaches = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_rpo_breaches_total", Help: "Transitions into the durable receiver RPO SLO fenced state.",
	})
	ControlPlaneSIEMTermHistoryIntegrity = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_term_history_integrity", Help: "Whether the receiver term-history hash chain verifies (1=valid).",
	})
	ControlPlaneSIEMBreakGlass = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_break_glass_total", Help: "Emergency receiver promotion outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMTermAnchors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_term_anchors_total", Help: "Externally signed receiver term-ledger anchor outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMIncidentEvidence = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_incident_evidence_total", Help: "Durable SIEM governance incident evidence records by bounded kind.",
	}, []string{"kind"})
	ControlPlaneSIEMIncidentAlerts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_incident_alerts_total", Help: "Signed governance incident alert delivery outcomes.",
	}, []string{"outcome"})
	ControlPlaneSIEMGovernanceBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_backlog", Help: "Durable external-governance delivery rows by evidence class and state.",
	}, []string{"class", "state"})
	ControlPlaneSIEMGovernanceOldestOutstanding = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_oldest_outstanding_seconds", Help: "Age in seconds of the oldest undelivered governance row by evidence class.",
	}, []string{"class"})
	ControlPlaneSIEMGovernanceDebtFence = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_debt_fenced", Help: "Whether ordinary receiver promotion is fenced by governance delivery debt (1=fenced).",
	})
	ControlPlaneSIEMGovernanceOpenRemediations = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_open_remediations", Help: "Open operator remediation attempts for dead governance evidence.",
	})
	ControlPlaneSIEMGovernancePendingApprovals = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_pending_remediation_approvals", Help: "Governance remediation requests waiting for an independent reviewer.",
	})
	ControlPlaneSIEMGovernanceReplicaReceiptDebt = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_replica_receipt_debt", Help: "Governance rows that have not yet reached the configured replica receipt quorum.",
	})
	ControlPlaneSIEMGovernancePreviousKeyGraceReplicas = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_previous_key_grace_replicas", Help: "Governance replicas whose previous receipt key is still inside the bounded grace window.",
	})
	ControlPlaneSIEMGovernanceReplicaReceipts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_replica_receipts_total", Help: "Governance replica receipt outcomes by evidence class and replica.",
	}, []string{"class", "replica", "outcome"})
	ControlPlaneSIEMGovernanceQuorum = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_quorum_total", Help: "Governance replica quorum outcomes by evidence class.",
	}, []string{"class", "outcome"})
	ControlPlaneSIEMGovernanceRemediations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_siem_governance_remediations_total", Help: "Operator governance remediation actions by outcome.",
	}, []string{"outcome"})
	ControlPlaneAuditIntegrity = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "control_plane_audit_integrity", Help: "Whether the latest control-plane audit hash-chain verification succeeded (1=valid, 0=invalid/unknown).",
	})
	ControlPlaneAuditLifecycle = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_audit_lifecycle_total", Help: "Control-plane audit sealing, verification and retention lifecycle outcomes.",
	}, []string{"outcome"})
	ControlPlaneAuditPurged = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_audit_purged_total", Help: "Control-plane audit rows removed after retention while preserving a chain anchor.",
	})
	ControlPlaneAuditExports = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "control_plane_audit_exports_total", Help: "SIEM-oriented control-plane audit export requests by outcome.",
	}, []string{"outcome"})

	QueueStreamLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_stream_length", Help: "Current Redis Stream length.",
	})
	QueuePending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_pending", Help: "Delivered Redis Stream messages awaiting ACK.",
	})
	QueueLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_lag", Help: "Redis Stream messages not yet delivered to the consumer group.",
	})
	QueueBacklog = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_backlog", Help: "Approximate outstanding queue work: pending plus non-negative lag.",
	})
	QueueStatsFresh = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_stats_fresh", Help: "Whether the latest queue statistics collection succeeded (1=fresh, 0=stale/unavailable).",
	})
	QueueStatsLastSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_stats_last_success_timestamp_seconds", Help: "Unix timestamp of the latest successful queue statistics collection.",
	})
	QueueStatsErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_stats_errors_total", Help: "Queue statistics collection errors.",
	})
	QueueCompactionRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_compaction_runs_total", Help: "Redis Stream compaction cycles by outcome.",
	}, []string{"outcome"})
	QueueCompactionTrimmedEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_compaction_trimmed_entries_total", Help: "Redis Stream entries removed by safe acknowledged-history compaction.",
	})
	QueueCompactionProtected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_compaction_protected_runs_total", Help: "Compaction cycles whose trim boundary was constrained by consumer-group delivery or pending state.",
	})
	QueuePoisonDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_poison_deliveries_total", Help: "Poison queue delivery handling outcomes by backend and failure class.",
	}, []string{"backend", "class", "outcome"})
	QueueDeadLetterReplays = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_replays_total", Help: "Dead-letter replay attempts by outcome.",
	}, []string{"outcome"})
	QueueDeadLetters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_dead_letters", Help: "Current durable dead-letter rows by failure class and replay state.",
	}, []string{"class", "state"})
	QueueDeadLetterOldestAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_oldest_age_seconds", Help: "Age of the oldest durable dead-letter row by failure class and replay state.",
	}, []string{"class", "state"})
	QueueDeadLetterLifecycleRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_lifecycle_runs_total", Help: "Dead-letter lifecycle cycles by outcome.",
	}, []string{"outcome"})
	QueueDeadLetterPurged = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_purged_total", Help: "Dead-letter rows removed by lifecycle or explicit operator purge.",
	}, []string{"reason"})
	QueueDeadLetterReplayAuditsPurged = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_replay_audits_purged_total", Help: "Expired replay-attempt audit rows removed by lifecycle retention.",
	})
	QueueDeadLetterOverLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_over_limit", Help: "Rows above the configured DLQ capacity after safe lifecycle cleanup; replaying rows may keep this positive.",
	})
	QueueDeadLetterArchives = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archives_total", Help: "DLQ archive attempts by bounded outcome.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_bytes_total", Help: "Bytes durably written to the configured DLQ archive sink.",
	})
	QueueDeadLetterArchiveIntegrityFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_integrity_failures_total", Help: "DLQ archive reads rejected by size, SHA-256, or package metadata verification.",
	})
	QueueDeadLetterRestores = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_restores_total", Help: "DLQ archive restore attempts by bounded outcome.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveReplications = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_replications_total", Help: "DLQ archive replica writes and verification outcomes.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveReplicaReads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_replica_reads_total", Help: "DLQ archive reads served from a verified replica.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveObjectGC = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_object_gc_total", Help: "Archive object lifecycle GC attempts by outcome.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveOrphans = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_orphans_total", Help: "Archive orphan-object reconciliation outcomes.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveDrills = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_restore_drills_total", Help: "Read-only archive restore-drill outcomes.",
	}, []string{"outcome"})
	QueueDeadLetterArchiveDrillLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "taskflow", Name: "queue_dead_letter_archive_restore_drill_last_success_timestamp_seconds", Help: "Unix timestamp of the last successful archive restore drill.",
	})
	EnqueueFencing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "enqueue_fencing_total", Help: "Per-generation enqueue fencing outcomes by publisher source.",
	}, []string{"source", "outcome"})
	EnqueueConfirmed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "taskflow", Name: "enqueue_confirmed_total", Help: "Task generations confirmed broker-visible by publisher source.",
	}, []string{"source"})
)

func init() {
	prometheus.MustRegister(
		HTTPRequests,
		HTTPDuration,
		TasksProcessed,
		ProcessDraining,
		WorkerInflight,
		ExecutorInflight,
		DrainOutcomes,
		ReconcilerCoordination,
		ReconcilerRecovered,
		ReconcilerEnqueued,
		CloudSagaRunnable,
		CloudSagaClaims,
		CloudSagaStepOperations,
		CloudSagaStepDuration,
		GRPCExecutorCalls,
		GRPCExecutorDuration,
		AIProviderCalls,
		AIProviderDuration,
		AITokens,
		AICostUSD,
		AIFallbacks,
		AIRouteSelections,
		AIProviderCircuitSkips,
		AIModelConcurrencyActive,
		AIModelConcurrencyQueued,
		AIModelQueueWait,
		AIBackpressure,
		AISLORequests,
		AIGlobalConcurrencyActive,
		AIGlobalConcurrencyLimit,
		AIGlobalConcurrency,
		AILoadShedding,
		AISystemPressureState,
		AIExecutorQueueDepth,
		AIHedgeRequests,
		AIHedgeDelay,
		AIHedgeExtraCostUSD,
		AITenantFairScheduling,
		AITenantDistributedConcurrency,
		AIBudgetAdmissions,
		AIBudgetSettledUSD,
		AIBudgetSettledTokens,
		AIBudgetOverruns,
		ControlPlaneAudits,
		ControlPlaneSIEMDeliveries,
		ControlPlaneSIEMDeliveryBacklog,
		ControlPlaneSIEMDeliveryDLQ,
		ControlPlaneSIEMLeadership,
		ControlPlaneSIEMLeader,
		ControlPlaneSIEMLeaderFence,
		ControlPlaneSIEMLifecycle,
		ControlPlaneSIEMPurged,
		ControlPlaneSIEMAcks,
		ControlPlaneSIEMDR,
		ControlPlaneSIEMDRCopies,
		ControlPlaneSIEMReconcile,
		ControlPlaneSIEMRecoveredReceipts,
		ControlPlaneSIEMRecoveryEvidence,
		ControlPlaneSIEMReceiverTerm,
		ControlPlaneSIEMReceiverRPO,
		ControlPlaneSIEMAntiEntropy,
		ControlPlaneSIEMAntiEntropyReplayed,
		ControlPlaneSIEMSwitchover,
		ControlPlaneSIEMSwitchoverRTO,
		ControlPlaneSIEMRoutingRecovery,
		ControlPlaneSIEMPromotionIntents,
		ControlPlaneSIEMPromotionExecution,
		ControlPlaneSIEMRPOFence,
		ControlPlaneSIEMRPOBreaches,
		ControlPlaneSIEMTermHistoryIntegrity,
		ControlPlaneSIEMBreakGlass,
		ControlPlaneSIEMTermAnchors,
		ControlPlaneSIEMIncidentEvidence,
		ControlPlaneSIEMIncidentAlerts,
		ControlPlaneSIEMGovernanceBacklog,
		ControlPlaneSIEMGovernanceOldestOutstanding,
		ControlPlaneSIEMGovernanceDebtFence,
		ControlPlaneSIEMGovernanceOpenRemediations,
		ControlPlaneSIEMGovernancePendingApprovals,
		ControlPlaneSIEMGovernanceReplicaReceiptDebt,
		ControlPlaneSIEMGovernancePreviousKeyGraceReplicas,
		ControlPlaneSIEMGovernanceReplicaReceipts,
		ControlPlaneSIEMGovernanceQuorum,
		ControlPlaneSIEMGovernanceRemediations,
		ControlPlaneAuditIntegrity,
		ControlPlaneAuditLifecycle,
		ControlPlaneAuditPurged,
		ControlPlaneAuditExports,
		QueueStreamLength,
		QueuePending,
		QueueLag,
		QueueBacklog,
		QueueStatsFresh,
		QueueStatsLastSuccessTimestamp,
		QueueStatsErrors,
		QueueCompactionRuns,
		QueueCompactionTrimmedEntries,
		QueueCompactionProtected,
		QueuePoisonDeliveries,
		QueueDeadLetterReplays,
		QueueDeadLetters,
		QueueDeadLetterOldestAge,
		QueueDeadLetterLifecycleRuns,
		QueueDeadLetterPurged,
		QueueDeadLetterReplayAuditsPurged,
		QueueDeadLetterOverLimit,
		QueueDeadLetterArchives,
		QueueDeadLetterArchiveBytes,
		QueueDeadLetterArchiveIntegrityFailures,
		QueueDeadLetterRestores,
		QueueDeadLetterArchiveReplications,
		QueueDeadLetterArchiveReplicaReads,
		QueueDeadLetterArchiveObjectGC,
		QueueDeadLetterArchiveOrphans,
		QueueDeadLetterArchiveDrills,
		QueueDeadLetterArchiveDrillLastSuccess,
		EnqueueFencing,
		EnqueueConfirmed,
	)
}
