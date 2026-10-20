#!/usr/bin/env python3
"""Static guardrails for Phase 29 receiver failover and signed-checkpoint DR."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/siem/receiver_checkpoint.go", "ReceiverCheckpointSignatureVerifier")
require("internal/siem/receiver_checkpoint.go", "receiver checkpoint HMAC verification failed")
require("internal/siem/receiver_cluster.go", "ReceiverClusterGuard")
require("internal/siem/receiver_cluster.go", "receiver split-brain divergence")
require("internal/siem/receiver_cluster.go", "PreferSecondary")
require("internal/siem/delivery.go", "activeSecondary")
require("internal/siem/delivery.go", "ensureSecondaryFailoverReady")
require("internal/siem/delivery.go", "failover_fenced")
require("internal/siem/delivery.go", "shadow_delivered")
require("internal/siem/delivery.go", "ReceiverReceipts")
require("internal/siem/delivery.go", "appendReceiverProofToEvidence")
require("internal/siem/evidence_replica.go", "matchedReceiver")
require("internal/domain/control_plane_delivery.go", "ReceiverID")
require("internal/domain/control_plane_delivery.go", "ReceiverGen")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_FAILOVER_ENABLED")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_CLUSTER_GUARD_ENABLED")
require("cmd/server/main.go", "NewReceiverClusterGuard")
require("cmd/server/main.go", "StartOnSecondary")
require("scripts/siem-sink-mock.py", "X-TaskFlow-Checkpoint-Signature")
require("scripts/siem-sink-mock.py", "checkpoint_hash_override_sequence")
require("docker-compose.yml", "siem-sink-secondary:")
require("docker-compose.yml", '"phase29-smoke"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_FAILOVER_ENABLED: "false"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_CHECKPOINT_HMAC_KEY")
require("Makefile", "phase29-receiver-failover-dr-smoke")
require("scripts/gate.sh", "phase29-receiver-failover-dr-check.py")
require(".github/workflows/ci.yml", "phase29-receiver-failover-dr-smoke.sh")
require("README.md", "Phase 29: signed receiver checkpoints and sticky active/passive failover")
require("scripts/phase29-receiver-failover-dr-smoke.sh", "prove signed split-brain checkpoint fences startup")
require("scripts/phase29-receiver-failover-dr-smoke.sh", "prove sticky secondary remains active after primary recovers")
require("scripts/phase29-receiver-failover-dr-smoke.sh", "restore SIEM hot state from secondary signed checkpoint and replica evidence")

print("Phase 29 signed receiver checkpoint, sticky failover and multi-receiver DR guardrails: ok")
