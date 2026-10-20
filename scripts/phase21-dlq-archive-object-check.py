#!/usr/bin/env python3
"""Validate Phase 21 archive object lifecycle, replication and drills."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 21 archive object guardrail missing from {path}: {needle}")


require("internal/archive/dead_letter.go", "Delete(context.Context, string) error")
require("internal/archive/dead_letter.go", "type Lister interface")
require("internal/archive/dead_letter.go", "func WithName")
require("internal/archive/dead_letter.go", "func (s *FileSink) List")
require("internal/archive/dead_letter.go", "func (s *HTTPObjectSink) Delete")
require("internal/domain/dead_letter.go", "DeadLetterArchiveExpiring")
require("internal/domain/dead_letter.go", "DeadLetterArchiveExpired")
require("internal/domain/dead_letter.go", "VerifiedCopies")
require("internal/domain/dead_letter.go", "type DeadLetterArchiveReplica struct")
require("internal/repository/task_repository.go", "&domain.DeadLetterArchiveReplica{}")
require("internal/repository/dead_letter_archive_object.go", "ClaimDeadLetterArchiveGC")
require("internal/repository/dead_letter_archive_object.go", "MarkDeadLetterArchiveExpired")
require("internal/repository/dead_letter_archive_object.go", "ListReferencedArchiveObjectKeys")
require("internal/repository/dead_letter_archive_object.go", "NOT EXISTS (SELECT 1 FROM queue_dead_letters")
require("internal/repository/dead_letter_lifecycle.go", "verified_copies >= ?")
require("internal/service/dead_letter.go", "ConfigureArchiveReplication")
require("internal/service/dead_letter.go", "ensureArchiveCopies")
require("internal/service/dead_letter.go", "readArchiveWithFallback")
require("internal/service/dead_letter.go", "ReconcileArchiveOrphans")
require("internal/service/dead_letter.go", "GCArchiveObject")
require("internal/service/dead_letter.go", "DrillArchive")
require("internal/monitor/dead_letter_archive_object.go", "StartDeadLetterArchiveObjectLifecycle")
require("internal/handler/router.go", 'controlV1.POST("/queue/dead-letter-archives/:id/drill"')
require("internal/metrics/metrics.go", "queue_dead_letter_archive_replications_total")
require("internal/metrics/metrics.go", "queue_dead_letter_archive_object_gc_total")
require("internal/metrics/metrics.go", "queue_dead_letter_archive_orphans_total")
require("internal/metrics/metrics.go", "queue_dead_letter_archive_restore_drills_total")
require("internal/config/config.go", 'getenvBool("QUEUE_DLQ_ARCHIVE_REPLICA_ENABLED", false)')
require("internal/config/config.go", 'getenvInt("QUEUE_DLQ_ARCHIVE_MIN_COPIES", 1)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_ARCHIVE_OBJECT_RETENTION_MS", 31536000000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS", 300000)')
require("cmd/server/main.go", "QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS")
require("docker-compose.yml", "dlq-archive-replica:/var/lib/taskflow/dlq-archive-replica")
require("deploy/k8s/deployment.yaml", "claimName: taskflow-dlq-archive-replica")
require("deploy/k8s/dlq-archive-replica-pvc.yaml", "ReadWriteMany")
require("Makefile", "phase21-dlq-archive-object-smoke")
require("scripts/gate.sh", "phase21-dlq-archive-object-check.py")
require(".github/workflows/ci.yml", "phase21-dlq-archive-object-smoke.sh")
require("README.md", "### Phase 21: archive object lifecycle governance")

print("phase21 archive object lifecycle governance guardrails: ok")
