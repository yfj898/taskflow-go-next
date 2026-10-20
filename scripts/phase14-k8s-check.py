#!/usr/bin/env python3
"""Validate Phase 14 Kubernetes disruption and rollout availability guardrails."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def require(text: str, needle: str, source: str) -> None:
    if needle not in text:
        raise SystemExit(f"Phase 14 guardrail missing from {source}: {needle}")


def scalar(text: str, key: str, source: str) -> int:
    match = re.search(rf"^\s*{re.escape(key)}:\s*[\"']?([0-9]+)[\"']?\s*$", text, re.MULTILINE)
    if not match:
        raise SystemExit(f"Phase 14 guardrail missing integer {key} from {source}")
    return int(match.group(1))


def check_workload(
    deployment_path: str,
    pdb_path: str,
    autoscaler_path: str,
    app: str,
    *,
    keda: bool = False,
) -> None:
    deployment = read(deployment_path)
    pdb = read(pdb_path)
    autoscaler = read(autoscaler_path)

    require(deployment, "type: RollingUpdate", deployment_path)
    require(deployment, "maxUnavailable: 0", deployment_path)
    require(deployment, "maxSurge: 1", deployment_path)
    require(deployment, "preferredDuringSchedulingIgnoredDuringExecution:", deployment_path)
    require(deployment, "topologyKey: kubernetes.io/hostname", deployment_path)
    require(deployment, "topologyKey: topology.kubernetes.io/zone", deployment_path)
    require(deployment, f"app: {app}", deployment_path)

    if deployment.count(f"app: {app}") < 3:
        raise SystemExit(f"Phase 14 placement selectors do not consistently target app={app}")

    require(pdb, "apiVersion: policy/v1", pdb_path)
    require(pdb, "kind: PodDisruptionBudget", pdb_path)
    require(pdb, "minAvailable: 1", pdb_path)
    require(pdb, f"app: {app}", pdb_path)

    replicas = scalar(deployment, "replicas", deployment_path)
    if replicas < 2:
        raise SystemExit(f"{deployment_path} must start with at least two replicas")

    min_key = "minReplicaCount" if keda else "minReplicas"
    min_replicas = scalar(autoscaler, min_key, autoscaler_path)
    if min_replicas < 2:
        raise SystemExit(f"{autoscaler_path} must preserve at least two replicas")

    if 1 >= min_replicas:
        raise SystemExit(f"{pdb_path} leaves no disruption headroom")


check_workload(
    "deploy/k8s/deployment.yaml",
    "deploy/k8s/taskflow-pdb.yaml",
    "deploy/k8s/hpa.yaml",
    "taskflow",
)
check_workload(
    "deploy/k8s/deployment.yaml",
    "deploy/k8s/taskflow-pdb.yaml",
    "deploy/k8s/keda-scaledobject.example.yaml",
    "taskflow",
    keda=True,
)
check_workload(
    "deploy/k8s/executor-deployment.yaml",
    "deploy/k8s/executor-pdb.yaml",
    "deploy/k8s/executor-hpa.yaml",
    "executor-worker",
)

print("phase14 Kubernetes disruption and rollout availability guardrails: ok")
