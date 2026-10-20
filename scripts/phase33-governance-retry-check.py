#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "AnchorState")
require("internal/domain/control_plane_delivery.go", "AnchorAttempts")
require("internal/domain/control_plane_delivery.go", "AlertState")
require("internal/domain/control_plane_delivery.go", "AlertAttempts")
require("internal/repository/control_plane_siem_governance.go", "ClaimControlPlaneSIEMTermAnchorDeliveries")
require("internal/repository/control_plane_siem_governance.go", "ClaimControlPlaneSIEMIncidentAlertDeliveries")
require("internal/repository/control_plane_siem_governance.go", 'Options: "SKIP LOCKED"')
require("internal/repository/control_plane_siem_governance.go", "boundedGovernanceDeliveryError")
require("internal/siem/governance_reconcile.go", "reconcileGovernance")
require("internal/siem/governance_reconcile.go", "retry_scheduled")
require("internal/siem/governance_reconcile.go", "FailControlPlaneSIEMIncidentAlertDelivery")
require("internal/siem/delivery.go", "w.reconcileGovernance(ctx)")
require("cmd/server/main.go", "defer control-plane SIEM receiver term anchoring to governance reconciler")
require("scripts/phase33-governance-retry-smoke.sh", "docker pause")
require("scripts/phase33-governance-retry-smoke.sh", "anchor_attempts>=1")
require("scripts/phase33-governance-retry-smoke.sh", "alert_attempts>=1")
require("docker-compose.yml", '"phase33-smoke"')
require("Makefile", "phase33-governance-retry-smoke")
require("scripts/gate.sh", "phase33-governance-retry-check.py")
require(".github/workflows/ci.yml", "phase33-governance-retry-smoke.sh")
require("README.md", "### Phase 33: durable governance retry and outage reconciliation")

print("Phase 33 durable governance retry, lease recovery and outage reconciliation guardrails: ok")
