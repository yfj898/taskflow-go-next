#!/usr/bin/env python3
"""Static guardrails for Phase 30 receiver term fencing, failback and DR."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMReceiverRoutingState")
require("internal/domain/control_plane_delivery.go", "ReceiverTerm")
require("internal/repository/control_plane_delivery.go", "PromoteControlPlaneSIEMReceiver")
require("internal/repository/control_plane_delivery.go", "RestoreControlPlaneSIEMReceiverRoutingState")
require("internal/repository/control_plane_delivery.go", "expectedTerm")
require("internal/siem/receiver_control.go", "PromoteReceiver")
require("internal/siem/receiver_control.go", "SyncReceiverAntiEntropy")
require("internal/siem/receiver_control.go", "EnsureReceiverRoutingState")
require("internal/siem/receiver_control.go", "signed receiver checkpoints disagree on routing term")
require("internal/siem/receiver_control.go", "standby secondary is ahead of authoritative primary")
require("internal/siem/receiver_control.go", "receiver promotion target is not caught up")
require("internal/siem/delivery.go", "X-TaskFlow-Receiver-Term")
require("internal/siem/delivery.go", "automatic-failover")
require("internal/siem/evidence_replica.go", "ReplicateReceiverReceipt")
require("internal/siem/evidence_replica.go", "deliveryReceiverReceiptEvidenceObjectKey")
require("internal/metrics/metrics.go", "control_plane_siem_receiver_rpo_sequences")
require("internal/metrics/metrics.go", "control_plane_siem_switchover_rto_seconds")
require("internal/metrics/metrics.go", "control_plane_siem_routing_recovery_total")
require("internal/handler/router.go", "/control-plane/siem/receivers/promote")
require("internal/handler/router.go", "/control-plane/siem/receivers/sync")
require("cmd/server/main.go", "durable routing term")
require("scripts/siem-sink-mock.py", "/control/term")
require("scripts/siem-sink-mock.py", "stale_term_rejections")
require("scripts/siem-sink-mock.py", '"active_receiver": active_receiver')
require("internal/config/config.go", "CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_ANTI_ENTROPY_ENABLED")
require("docker-compose.yml", '"phase30-smoke"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_TERM_FENCING_ENABLED: "false"')
require("Makefile", "phase30-receiver-term-failback-dr-smoke")
require("scripts/gate.sh", "phase30-receiver-term-failback-dr-check.py")
require(".github/workflows/ci.yml", "phase30-receiver-term-failback-dr-smoke.sh")
require("README.md", "Phase 30: receiver term fencing, anti-entropy and controlled failback")
require("scripts/phase30-receiver-term-failback-dr-smoke.sh", "prove standby anti-entropy catch-up")
require("scripts/phase30-receiver-term-failback-dr-smoke.sh", "prove stale term fencing and CAS promotion")
require("scripts/phase30-receiver-term-failback-dr-smoke.sh", "restore routing term and SIEM hot state from signed checkpoints and surviving evidence")

print("Phase 30 receiver term fencing, anti-entropy, controlled failback and DR guardrails: ok")
