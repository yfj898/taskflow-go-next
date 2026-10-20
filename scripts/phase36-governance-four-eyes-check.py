#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "OperationKey")
require("internal/domain/control_plane_delivery.go", 'gorm:"size:128;uniqueIndex"')
require("internal/domain/control_plane_delivery.go", "ApprovedBy")
require("internal/domain/control_plane_delivery.go", "PendingRemediationApprovals")
require("internal/repository/control_plane_siem_governance.go", "RequestControlPlaneSIEMGovernanceRemediation")
require("internal/repository/control_plane_siem_governance.go", "ApproveControlPlaneSIEMGovernanceRemediation")
require("internal/repository/control_plane_siem_governance.go", "clause.OnConflict{DoNothing: true}")
require("internal/repository/control_plane_siem_governance.go", "requester and reviewer must be different operators")
require("internal/repository/control_plane_siem_governance.go", 'State:        "pending_approval"')
require("internal/siem/governance_reconcile.go", "direct governance remediation requeue is disabled while approval workflow is required")
require("internal/siem/governance_reconcile.go", "RequestGovernanceRemediation")
require("internal/siem/governance_reconcile.go", "ApproveGovernanceRemediation")
require("internal/siem/governance_reconcile.go", '"governance-reviewer"')
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED")
require("internal/config/config.go", 'case "governance-reviewer":')
require("internal/handler/router.go", '"/control-plane/siem/governance/dead/:kind/:id/remediations"')
require("internal/handler/router.go", '"/control-plane/siem/governance/remediations/:id/approve"')
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernancePendingApprovals")
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_REMEDIATION_APPROVAL_REQUIRED")
require("docker-compose.yml", '"phase36-smoke"')
require("scripts/phase36-governance-four-eyes-smoke.sh", "operation_key")
require("scripts/phase36-governance-four-eyes-smoke.sh", "must be different operators")
require("scripts/phase36-governance-four-eyes-smoke.sh", "request_idempotent")
require("Makefile", "phase36-governance-four-eyes-smoke")
require("scripts/gate.sh", "phase36-governance-four-eyes-check.py")
require(".github/workflows/ci.yml", "phase36-governance-four-eyes-smoke.sh")
require("README.md", "### Phase 36: four-eyes governance remediation and idempotent approval")

print("Phase 36 four-eyes governance remediation and idempotent approval guardrails: ok")
