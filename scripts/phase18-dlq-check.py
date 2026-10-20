#!/usr/bin/env python3
"""Validate the safety-critical Phase 18 poison-message quarantine guardrails."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 18 DLQ guardrail missing from {path}: {needle}")


require("internal/domain/dead_letter.go", "type DeadLetterMessage struct")
require("internal/domain/dead_letter.go", "SourceDeliveryID")
require("internal/domain/dead_letter.go", "RawEnvelope")
require("internal/domain/dead_letter.go", "ReplayLeaseUntil")
require("internal/domain/dead_letter.go", "idx_dead_letter_source")
require("internal/repository/task_repository.go", "&domain.DeadLetterMessage{}")
require("internal/repository/dead_letter.go", "QuarantineDeadLetter")
require("internal/repository/dead_letter.go", "ClaimDeadLetterReplay")
require("internal/repository/dead_letter.go", "ConfirmTaskEnqueuedAndDeadLetterReplayed")
require("internal/queue/task_queue.go", "type PoisonDeliveryError struct")
require("internal/queue/task_queue.go", 'FailureClass: "decode"')
require("internal/queue/kafka.go", "&PoisonDeliveryError{")
require("internal/worker/pool.go", "quarantinePoison")
require("internal/worker/pool.go", 'FailureClass: "orphan"')
require("internal/worker/pool.go", "p.queue.Ack(context.Background(), poison.DeliveryID)")
require("internal/service/dead_letter.go", "task.EnqueueGeneration != generation")
require("internal/service/dead_letter.go", "ClaimDeadLetterReplay")
require("internal/service/dead_letter.go", "ClaimTaskEnqueue")
require("internal/service/dead_letter.go", 'WithLabelValues("dlq_replay"')
require("internal/service/dead_letter.go", "enqueueState != repository.EnqueueClaimAcquired")
require("internal/service/dead_letter.go", "ConfirmTaskEnqueuedAndDeadLetterReplayed")
require("internal/handler/router.go", 'controlV1.GET("/queue/dead-letters"')
require("internal/handler/router.go", 'controlV1.POST("/queue/dead-letters/:id/replay"')
require("internal/config/config.go", 'getenvInt("QUEUE_DLQ_REPLAY_LEASE_MS", 30000)')
require("internal/metrics/metrics.go", "queue_poison_deliveries_total")
require("internal/metrics/metrics.go", "queue_dead_letter_replays_total")
require("docker-compose.yml", "QUEUE_DLQ_REPLAY_LEASE_MS")
require("deploy/k8s/configmap.yaml", 'QUEUE_DLQ_REPLAY_LEASE_MS: "30000"')

print("phase18 poison-message quarantine and controlled replay guardrails: ok")
