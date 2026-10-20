#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "TargetIdentity")
require("internal/domain/control_plane_delivery.go", "ExpiresAt")
require("internal/domain/control_plane_delivery.go", "RejectedBy")
require("internal/domain/control_plane_delivery.go", "CancelledBy")
require("internal/repository/control_plane_siem_governance.go", "ExpireControlPlaneSIEMGovernanceRemediations")
require("internal/repository/control_plane_siem_governance.go", "RejectControlPlaneSIEMGovernanceRemediation")
require("internal/repository/control_plane_siem_governance.go", "CancelControlPlaneSIEMGovernanceRemediation")
require("internal/repository/control_plane_siem_governance.go", '"state": "stale"')
require("internal/repository/control_plane_siem_governance.go", "target changed after request")
require("internal/siem/governance_reconcile.go", "RejectGovernanceRemediation")
require("internal/siem/governance_reconcile.go", "CancelGovernanceRemediation")
require("internal/siem/governance_reconcile.go", 'WithLabelValues("expired")')
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_TTL_MS")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_STALE_TARGET_FENCING_ENABLED")
require("internal/handler/router.go", '"/control-plane/siem/governance/remediations/:id/reject"')
require("internal/handler/router.go", '"/control-plane/siem/governance/remediations/:id/cancel"')
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_TTL_MS")
require("docker-compose.yml", '"phase37-smoke"')
require("scripts/phase37-governance-approval-lifecycle-smoke.sh", "pending_approval")
require("scripts/phase37-governance-approval-lifecycle-smoke.sh", "stale")
require("scripts/phase37-governance-approval-lifecycle-smoke.sh", "expired")
require("Makefile", "phase37-governance-approval-lifecycle-smoke")
require("scripts/gate.sh", "phase37-governance-approval-lifecycle-check.py")
require(".github/workflows/ci.yml", "phase37-governance-approval-lifecycle-smoke.sh")
require("README.md", "### Phase 37: remediation approval lifecycle and stale-target fencing")

print("Phase 37 remediation TTL, rejection/cancellation and stale-target guardrails: ok")
