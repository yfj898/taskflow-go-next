#!/usr/bin/env python3
"""Validate the safety-critical Phase 16 enqueue generation fences."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 16 enqueue fence missing from {path}: {needle}")


require("internal/domain/task.go", "EnqueueGeneration")
require("internal/domain/task.go", "EnqueuedGeneration")
require("internal/domain/task.go", "EnqueueLeaseUntil")
require("internal/repository/task_repository.go", 'enqueue_generation, enqueued_generation')
require("internal/repository/task_repository.go", 'enqueue_generation = ?')
require("internal/repository/task_repository.go", 'gorm.Expr("enqueue_generation + 1")')
require("internal/repository/enqueue_fencing.go", "ClaimTaskEnqueue")
require("internal/repository/enqueue_fencing.go", "EnqueueClaimAlreadyVisible")
require("internal/repository/enqueue_fencing.go", "task.Status != domain.TaskPending")
require("internal/repository/enqueue_fencing.go", "ConfirmTaskEnqueuedAndOutboxBatch")
require("internal/queue/task_queue.go", '"enqueue_generation"')
require("internal/queue/task_queue.go", "Generation uint64")
require("internal/queue/kafka.go", "encodeKafkaMessage")
require("internal/worker/pool.go", "delivery.Generation")
require("internal/outbox/dispatcher.go", "ClaimTaskEnqueue(ctx, events[i].TaskID, 1")
require("internal/outbox/dispatcher.go", "visibleClaims")
require("internal/reconciler/reconciler.go", "ClaimTaskEnqueue(ctx, task.ID, 0")
require("internal/reconciler/reconciler.go", "ConfirmTaskEnqueued")
require("internal/config/config.go", 'getenvInt("ENQUEUE_VISIBILITY_LEASE_MS", 10000)')
require("docker-compose.yml", "ENQUEUE_VISIBILITY_LEASE_MS")
require("deploy/k8s/configmap.yaml", 'ENQUEUE_VISIBILITY_LEASE_MS: "10000"')

print("phase16 per-task enqueue generation fencing guardrails: ok")
