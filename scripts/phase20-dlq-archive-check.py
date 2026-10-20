#!/usr/bin/env python3
"""Validate Phase 20 DLQ archive/export/restore guardrails."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 20 DLQ archive guardrail missing from {path}: {needle}")


require("internal/archive/dead_letter.go", "type Sink interface")
require("internal/archive/dead_letter.go", "type FileSink struct")
require("internal/archive/dead_letter.go", "type HTTPObjectSink struct")
require("internal/archive/dead_letter.go", "SHA256Hex")
require("internal/archive/dead_letter.go", "DeadLetterArchiveFormatVersion = 1")
require("internal/domain/dead_letter.go", "type DeadLetterArchive struct")
require("internal/domain/dead_letter.go", 'TableName() string { return "queue_dead_letter_archives" }')
require("internal/domain/dead_letter.go", "VerifiedAt")
require("internal/repository/task_repository.go", "&domain.DeadLetterArchive{}")
require("internal/repository/dead_letter_archive.go", "ClaimDeadLetterArchive")
require("internal/repository/dead_letter_archive.go", "ConfirmDeadLetterArchive")
require("internal/repository/dead_letter_archive.go", "MarkDeadLetterArchiveVerified")
require("internal/repository/dead_letter_archive.go", "RestoreDeadLetterArchive")
require("internal/repository/dead_letter_lifecycle.go", "ErrDeadLetterArchiveRequired")
require("internal/repository/dead_letter_lifecycle.go", "a.verified_at >= ?")
require("internal/monitor/dead_letter_lifecycle.go", "verificationStarted := time.Now()")
require("internal/monitor/dead_letter_lifecycle.go", "archiver.Archive(runCtx, id)")
require("internal/service/dead_letter.go", "ConfigureArchive")
require("internal/service/dead_letter.go", "func (s *DeadLetterService) Archive")
require("internal/service/dead_letter.go", "func (s *DeadLetterService) ExportArchive")
require("internal/service/dead_letter.go", "func (s *DeadLetterService) RestoreArchive")
require("internal/service/dead_letter.go", "DeadLetterQuarantined")
require("internal/service/dead_letter.go", "QueueDeadLetterArchiveIntegrityFailures")
require("internal/handler/router.go", 'controlV1.POST("/queue/dead-letters/:id/archive"')
require("internal/handler/router.go", 'controlV1.GET("/queue/dead-letter-archives/:id/export"')
require("internal/handler/router.go", 'controlV1.POST("/queue/dead-letter-archives/:id/restore"')
require("internal/metrics/metrics.go", "queue_dead_letter_archives_total")
require("internal/metrics/metrics.go", "queue_dead_letter_archive_integrity_failures_total")
require("internal/metrics/metrics.go", "queue_dead_letter_archive_restores_total")
require("internal/config/config.go", 'getenvBool("QUEUE_DLQ_ARCHIVE_ENABLED", true)')
require("internal/config/config.go", 'getenv("QUEUE_DLQ_ARCHIVE_SINK", "file")')
require("internal/config/config.go", 'getenvInt("QUEUE_DLQ_ARCHIVE_LEASE_MS", 30000)')
require("cmd/server/main.go", 'case "http", "object-http", "object_http":')
require("docker-compose.yml", "QUEUE_DLQ_ARCHIVE_DIR")
require("docker-compose.yml", "dlq-archive:/var/lib/taskflow/dlq-archive")
require("deploy/k8s/configmap.yaml", 'QUEUE_DLQ_ARCHIVE_ENABLED: "true"')
require("deploy/k8s/deployment.yaml", "claimName: taskflow-dlq-archive")
require("deploy/k8s/dlq-archive-pvc.yaml", "ReadWriteMany")
require("Makefile", "phase20-dlq-archive-smoke")
require("scripts/gate.sh", "phase20-dlq-archive-check.py")
require(".github/workflows/ci.yml", "phase20-dlq-archive-smoke.sh")
require("README.md", "### Phase 20: DLQ archive/export and restore")

print("phase20 DLQ archive/export/restore guardrails: ok")
