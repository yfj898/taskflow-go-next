#!/usr/bin/env python3
"""Validate Phase 19 DLQ lifecycle, purge and replay-audit guardrails."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 19 DLQ lifecycle guardrail missing from {path}: {needle}")


require("internal/domain/dead_letter.go", "type DeadLetterReplayAudit struct")
require("internal/domain/dead_letter.go", "idx_dlq_state_created")
require("internal/domain/dead_letter.go", "idx_dlq_state_replayed")
require("internal/domain/dead_letter.go", 'TableName() string { return "queue_dead_letter_replay_audits" }')
require("internal/repository/task_repository.go", "&domain.DeadLetterReplayAudit{}")
require("internal/repository/dead_letter_lifecycle.go", "RunDeadLetterLifecycle")
require("internal/repository/dead_letter_lifecycle.go", "DeadLetterLifecycleStats")
require("internal/repository/dead_letter_lifecycle.go", 'clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}')
require("internal/repository/dead_letter_lifecycle.go", 'id IN ? AND replay_state = ?')
require("internal/repository/dead_letter_lifecycle.go", 'Where("replay_state = ?", domain.DeadLetterReplayed)')
require("internal/repository/dead_letter.go", "ErrDeadLetterReplayActive")
require("internal/repository/dead_letter_lifecycle.go", "DeadLetterReplaying")
require("internal/service/dead_letter.go", "replayAuditSnapshot")
require("internal/service/dead_letter.go", "ListReplayAudits")
require("internal/service/dead_letter.go", "QueueDeadLetterPurged")
require("internal/handler/router.go", 'controlV1.DELETE("/queue/dead-letters/:id"')
require("internal/handler/router.go", 'controlV1.GET("/queue/dead-letters/:id/replay-audits"')
require("internal/monitor/dead_letter_lifecycle.go", "StartDeadLetterLifecycle")
require("internal/monitor/dead_letter_lifecycle.go", "QueueDeadLetterOldestAge.Reset()")
require("internal/metrics/metrics.go", "queue_dead_letter_oldest_age_seconds")
require("internal/metrics/metrics.go", "queue_dead_letter_over_limit")
require("internal/config/config.go", 'getenvBool("QUEUE_DLQ_LIFECYCLE_ENABLED", true)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_REPLAYED_RETENTION_MS", 604800000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_QUARANTINED_RETENTION_MS", 2592000000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_REPLAY_AUDIT_RETENTION_MS", 7776000000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_MAX_ENTRIES", 100000)')
require("docker-compose.yml", "QUEUE_DLQ_LIFECYCLE_INTERVAL_MS")
require("docker-compose.yml", 'MYSQL_DSN: "${MYSQL_DSN:-')
require("deploy/k8s/configmap.yaml", 'QUEUE_DLQ_MAX_ENTRIES: "100000"')
require("Makefile", "phase19-dlq-lifecycle-smoke")
require("scripts/gate.sh", "phase19-dlq-lifecycle-check.py")
require(".github/workflows/ci.yml", "phase19-dlq-lifecycle-smoke.sh")

print("phase19 dead-letter lifecycle governance guardrails: ok")
