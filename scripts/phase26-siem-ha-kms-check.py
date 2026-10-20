#!/usr/bin/env python3
"""Static guardrails for Phase 26 SIEM HA, signing rotation and history GC."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "LeaderFence")
require("internal/domain/control_plane_delivery.go", "ControlPlaneAuditDeliveryReceipt")
require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMDeliveryCoordinatorState")
require("internal/repository/task_repository.go", "ControlPlaneSIEMDeliveryCoordinatorState")
require("internal/repository/control_plane_delivery.go", "ActivateControlPlaneSIEMDeliveryFence")
require("internal/repository/control_plane_delivery.go", "state.ActiveFence + 1")
require("internal/repository/control_plane_delivery.go", "ErrControlPlaneSIEMStaleFence")
require("internal/repository/control_plane_delivery.go", "ensureControlPlaneSIEMFence")
require("internal/repository/control_plane_delivery.go", "PurgeControlPlaneSIEMDeliveryHistory")
require("internal/repository/control_plane_delivery.go", "BackfillControlPlaneAuditDeliveryReceipts")
require("internal/repository/control_plane_audit.go", "control_plane_audit_delivery_receipts.chain_sequence")
require("internal/siem/coordination.go", "INCR")
require("internal/siem/coordination.go", "RedisFencingCoordinator")
require("internal/siem/delivery.go", "ControlPlaneSIEMLeaderFence")
require("internal/siem/delivery.go", "PreviousSignatureProvider")
require("internal/siem/signer.go", "RotatingHMACSHA256Signer")
require("internal/siem/remote_signer.go", "DigestSHA256")
require("internal/siem/remote_signer.go", "RemoteCheckpointSigner")
require("internal/monitor/control_plane_siem.go", "StartControlPlaneSIEMHistoryLifecycle")
require("internal/metrics/metrics.go", "control_plane_siem_leadership_total")
require("internal/metrics/metrics.go", "control_plane_siem_purged_total")
require("internal/config/config.go", 'getenv("CONTROL_PLANE_SIEM_SIGNING_PROVIDER", "hmac")')
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION", false)')
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_SIEM_GC_ENABLED", true)')
require("cmd/server/main.go", "NewRedisFencingCoordinator")
require("cmd/server/main.go", "NewRemoteCheckpointSigner")
require("cmd/server/main.go", "StartControlPlaneSIEMHistoryLifecycle")
require("docker-compose.yml", 'profiles: ["phase26-smoke"]')
require("docker-compose.yml", "CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_DISTRIBUTED_COORDINATION: "true"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_PREVIOUS_SIGNING_KEY")
require("scripts/kms-signer-mock.py", "digest_sha256")
require("scripts/siem-sink-mock.py", "previous_signature_valid")
require("scripts/phase26-siem-ha-kms-smoke.sh", "fence_three_after_redis_reset")
require("scripts/phase26-siem-ha-kms-smoke.sh", "accepted_after_restart")
require("scripts/phase26-siem-ha-kms-smoke.sh", "control_plane_audit_delivery_receipts")
require("Makefile", "phase26-siem-ha-kms-smoke")
require("scripts/gate.sh", "phase26-siem-ha-kms-check.py")
require("README.md", "Phase 26: SIEM HA fencing, signing-key rotation and delivery-history GC")

print("Phase 26 SIEM HA, KMS signing rotation and delivery-GC guardrails: ok")
