#!/usr/bin/env python3
"""Static guardrails for Phase 15 distributed reconciler coordination."""

from pathlib import Path


def require(text: str, needle: str, source: str) -> None:
    if needle not in text:
        raise SystemExit(f"Phase 15 coordination guardrail missing from {source}: {needle}")


config = Path("internal/config/config.go").read_text(encoding="utf-8")
server = Path("cmd/server/main.go").read_text(encoding="utf-8")
reconciler = Path("internal/reconciler/reconciler.go").read_text(encoding="utf-8")
coordination = Path("internal/reconciler/coordination.go").read_text(encoding="utf-8")
compose = Path("docker-compose.yml").read_text(encoding="utf-8")
k8s_config = Path("deploy/k8s/configmap.yaml").read_text(encoding="utf-8")

for needle in (
    'getenvBool("RECONCILE_DISTRIBUTED_COORDINATION", true)',
    'getenv("RECONCILE_LOCK_KEY", "taskflow:reconciler:leader")',
    'getenvInt("RECONCILE_LEASE_MS", 15000)',
):
    require(config, needle, "internal/config/config.go")

require(server, "reconcileRunner.ConfigureCoordinator(", "cmd/server/main.go")
require(server, "reconciler.NewRedisCoordinator(rdb, cfg.ReconcileLockKey)", "cmd/server/main.go")
require(server, "reconcileRunner.Start(reconcileCtx)", "cmd/server/main.go")
require(server, "reconcileCancel()", "cmd/server/main.go")
require(reconciler, "lease = r.runCycle(ctx, lease)", "internal/reconciler/reconciler.go")
require(reconciler, 'WithLabelValues("contended")', "internal/reconciler/reconciler.go")

for needle in (
    "SetNX(parent, c.key, owner, ttl)",
    'redis.call("PEXPIRE", KEYS[1], ARGV[2])',
    'redis.call("GET", KEYS[1]) == ARGV[1]',
    'redis.call("DEL", KEYS[1])',
):
    require(coordination, needle, "internal/reconciler/coordination.go")

for needle in (
    "RECONCILE_DISTRIBUTED_COORDINATION: ${RECONCILE_DISTRIBUTED_COORDINATION:-true}",
    "RECONCILE_LOCK_KEY: ${RECONCILE_LOCK_KEY:-taskflow:reconciler:leader}",
    "RECONCILE_LEASE_MS: ${RECONCILE_LEASE_MS:-15000}",
    "REDIS_QUEUE_KEY: ${REDIS_QUEUE_KEY:-taskflow:queue}",
):
    require(compose, needle, "docker-compose.yml")

for needle in (
    'RECONCILE_DISTRIBUTED_COORDINATION: "true"',
    'RECONCILE_LOCK_KEY: "taskflow:reconciler:leader"',
    'RECONCILE_LEASE_MS: "15000"',
):
    require(k8s_config, needle, "deploy/k8s/configmap.yaml")

print("phase15 distributed reconciler coordination guardrails: ok")
