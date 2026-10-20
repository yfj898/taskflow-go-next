#!/usr/bin/env python3
"""Validate the safety-critical Phase 17 Redis Stream compaction guardrails."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Phase 17 compaction guardrail missing from {path}: {needle}")


require("internal/queue/compaction.go", "safeStreamTrimScript")
require("internal/queue/compaction.go", "redis.call('XINFO', 'GROUPS', KEYS[1])")
require("internal/queue/compaction.go", "redis.call('XPENDING', KEYS[1], name)")
require("internal/queue/compaction.go", "redis.call('XTRIM', KEYS[1], 'MINID', cutoff)")
require("internal/queue/compaction.go", "last-delivered-id")
require("internal/queue/compaction.go", "XRevRangeN")
require("internal/queue/compaction.go", "compare_ids(cutoff, protected) > 0")
require("internal/queue/compaction.go", "local lower = pending[2]")
require("cmd/server/main.go", "StartQueueCompaction")
require("cmd/server/main.go", "MaxRetainedEntries")
require("internal/config/config.go", 'getenvBool("QUEUE_COMPACTION_ENABLED", true)')
require("internal/config/config.go", 'getenvInt("QUEUE_COMPACTION_INTERVAL_MS", 30000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_ACK_RETENTION_MS", 86400000)')
require("internal/config/config.go", 'getenvNonNegativeInt("QUEUE_ACK_RETENTION_MAX_ENTRIES", 100000)')
require("internal/metrics/metrics.go", "queue_compaction_trimmed_entries_total")
require("docker-compose.yml", "QUEUE_ACK_RETENTION_MAX_ENTRIES")
require("deploy/k8s/configmap.yaml", 'QUEUE_ACK_RETENTION_MAX_ENTRIES: "100000"')

print("phase17 Redis Stream acknowledged-history compaction guardrails: ok")
