#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMGovernanceStatus")
require("internal/domain/control_plane_delivery.go", "OldestOutstandingSeconds")
require("internal/repository/control_plane_siem_governance.go", "ControlPlaneSIEMGovernanceDeliveryStatus")
require("internal/siem/governance_reconcile.go", "GovernanceStatus")
require("internal/siem/governance_reconcile.go", "dead governance evidence")
require("internal/siem/governance_reconcile.go", "governance outstanding=")
require("internal/siem/governance_reconcile.go", "oldest governance debt")
require("internal/siem/promotion_workflow.go", "receiver promotion fenced by governance debt policy")
require("internal/siem/promotion_workflow.go", "governance_debt_bypass")
require("internal/handler/router.go", '"/control-plane/siem/governance/status"')
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceBacklog")
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceOldestOutstanding")
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceDebtFence")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_MAX_OUTSTANDING")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_MAX_AGE_MS")
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_DEBT_POLICY_ENABLED")
require("docker-compose.yml", '"phase34-smoke"')
require("scripts/phase34-governance-debt-smoke.sh", "governance/status")
require("scripts/phase34-governance-debt-smoke.sh", "governance debt policy")
require("Makefile", "phase34-governance-debt-smoke")
require("scripts/gate.sh", "phase34-governance-debt-check.py")
require(".github/workflows/ci.yml", "phase34-governance-debt-smoke.sh")
require("README.md", "### Phase 34: governance debt SLO and promotion safety fence")

print("Phase 34 governance debt SLO, observability and promotion fencing guardrails: ok")
