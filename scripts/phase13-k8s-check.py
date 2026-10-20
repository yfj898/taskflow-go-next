#!/usr/bin/env python3
"""Validate the safety-critical Phase 13 Kubernetes drain settings."""

import re
from pathlib import Path


def require(text: str, needle: str, source: str) -> None:
    if needle not in text:
        raise SystemExit(f"Phase 13 drain guardrail missing from {source}: {needle}")


def yaml_int(text: str, key: str, source: str) -> int:
    match = re.search(rf"^\s*{re.escape(key)}:\s*\"?(\d+)\"?\s*$", text, flags=re.MULTILINE)
    if match is None:
        raise SystemExit(f"Phase 13 drain guardrail missing integer {key} from {source}")
    return int(match.group(1))


taskflow_path = Path("deploy/k8s/deployment.yaml")
executor_path = Path("deploy/k8s/executor-deployment.yaml")
config_path = Path("deploy/k8s/configmap.yaml")

taskflow = taskflow_path.read_text(encoding="utf-8")
executor = executor_path.read_text(encoding="utf-8")
config = config_path.read_text(encoding="utf-8")

taskflow_grace_s = yaml_int(taskflow, "terminationGracePeriodSeconds", str(taskflow_path))
executor_grace_s = yaml_int(executor, "terminationGracePeriodSeconds", str(executor_path))
drain_fence_ms = yaml_int(config, "DRAIN_FENCE_MS", str(config_path))
drain_timeout_ms = yaml_int(config, "DRAIN_TIMEOUT_MS", str(config_path))

# cmd/server reserves up to five seconds for HTTP/pprof shutdown after the
# worker drain. cmd/executor-worker reserves up to three seconds for the metrics
# server shutdown. Kubernetes must leave headroom for both sequences before it
# sends SIGKILL at the Pod termination deadline.
taskflow_required_ms = drain_fence_ms + drain_timeout_ms + 5000
executor_required_ms = drain_timeout_ms + 3000
if taskflow_grace_s * 1000 <= taskflow_required_ms:
    raise SystemExit(
        f"TaskFlow termination grace {taskflow_grace_s}s must exceed "
        f"fence+drain+shutdown budget {taskflow_required_ms}ms"
    )
if executor_grace_s * 1000 <= executor_required_ms:
    raise SystemExit(
        f"executor termination grace {executor_grace_s}s must exceed "
        f"drain+metrics-shutdown budget {executor_required_ms}ms"
    )

require(taskflow, "periodSeconds: 2", str(taskflow_path))
require(taskflow, "failureThreshold: 1", str(taskflow_path))
require(executor, "readinessProbe:\n            grpc:\n              port: 9091", str(executor_path))
require(executor, "livenessProbe:\n            grpc:\n              port: 9091", str(executor_path))
require(executor, "name: DRAIN_TIMEOUT_MS", str(executor_path))
require(config, 'DRAIN_FENCE_MS: "1000"', str(config_path))
require(config, 'DRAIN_TIMEOUT_MS: "30000"', str(config_path))

print("phase13 Kubernetes drain guardrails: ok")
