#!/usr/bin/env python3
"""Validate the safety-critical Phase 12 KEDA example without a cluster."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def fail(message: str) -> None:
    raise SystemExit(message)


path = Path(sys.argv[1] if len(sys.argv) > 1 else "deploy/k8s/keda-scaledobject.example.yaml")
text = path.read_text(encoding="utf-8")

for required in (
    "fallback:",
    "failureThreshold: 3",
    "replicas: 4",
    "scaleDown:",
    "stabilizationWindowSeconds: 300",
    "type: prometheus",
    'ignoreNullValues: "false"',
):
    if required not in text:
        fail(f"KEDA guardrail is missing: {required}")

if len(re.findall(r"^\s*- type: prometheus\s*$", text, flags=re.MULTILINE)) != 1:
    fail("Phase 12 example must have exactly one Prometheus scaler")

required_query_terms = (
    "taskflow_queue_backlog",
    "taskflow_queue_stats_fresh",
    "taskflow_queue_stats_last_success_timestamp_seconds",
    "time()",
)
for term in required_query_terms:
    if term not in text:
        fail(f"KEDA query is missing freshness guard term: {term}")

print("phase12 KEDA scaling guardrails: ok")
