#!/usr/bin/env python3
"""Validate real Kubernetes eviction/KEDA release-evidence wiring."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Kubernetes release-evidence guardrail missing from {path}: {needle}")


require("deploy/k8s/deployment.yaml", "maxUnavailable: 0")
require("deploy/k8s/deployment.yaml", "terminationGracePeriodSeconds: 45")
require("Dockerfile", "USER 10001")
require("deploy/k8s/deployment.yaml", "runAsUser: 10001")
require("deploy/k8s/deployment.yaml", "fsGroup: 10001")
require("deploy/k8s/executor-deployment.yaml", "runAsUser: 10001")
require("deploy/k8s/taskflow-pdb.yaml", "minAvailable: 1")
require("deploy/k8s/keda-scaledobject.example.yaml", "prometheus.default.svc.cluster.local")
require("deploy/k8s/keda-scaledobject.example.yaml", "ignoreNullValues: \"false\"")
require("deploy/k8s/release-validation-infra.yaml", "name: prometheus")
require("deploy/k8s/release-validation-infra.yaml", "ReadWriteMany")
require("deploy/k8s/release-validation-infra.yaml", 'targets: ["taskflow:80"]')
require("scripts/release-k8s-keda-smoke.sh", "/eviction")
require("scripts/release-k8s-keda-smoke.sh", 'KEDA_VERSION="2.20.2"')
require("scripts/release-k8s-keda-smoke.sh", "wait_deploy keda-metrics-apiserver keda")
require("scripts/release-k8s-keda-smoke.sh", "taskflow_queue_backlog")
require("scripts/release-k8s-keda-smoke.sh", "keda-hpa-taskflow-queue-scaler")
require("scripts/release-k8s-keda-smoke.sh", 'phase="verify scaler healthy before backlog"')
require("scripts/release-k8s-keda-smoke.sh", 'test "$fallback" != "True"')
require("Makefile", "release-k8s-keda-smoke")
require("scripts/gate.sh", "release-k8s-keda-check.py")

print("real Kubernetes eviction/KEDA release-evidence guardrails: ok")
