#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMGovernanceRemediation")
require("internal/domain/control_plane_delivery.go", "OpenRemediations")
require("internal/repository/task_repository.go", "ControlPlaneSIEMGovernanceRemediation{}")
require("internal/repository/control_plane_siem_governance.go", "ListControlPlaneSIEMGovernanceDead")
require("internal/repository/control_plane_siem_governance.go", "RequeueControlPlaneSIEMGovernanceDead")
require("internal/repository/control_plane_siem_governance.go", "SettleControlPlaneSIEMGovernanceRemediations")
require("internal/repository/control_plane_siem_governance.go", 'Clauses(clause.Locking{Strength: "UPDATE"})')
require("internal/siem/governance_reconcile.go", "governance remediation in progress=")
require("internal/siem/governance_reconcile.go", "incident-commander role for governance remediation")
require("internal/siem/governance_reconcile.go", "settleGovernanceRemediation")
require("internal/handler/router.go", '"/control-plane/siem/governance/dead"')
require("internal/handler/router.go", '"/control-plane/siem/governance/remediations"')
require("internal/handler/router.go", '"/control-plane/siem/governance/dead/:kind/:id/requeue"')
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceOpenRemediations")
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceRemediations")
require("scripts/phase35-governance-remediation-smoke.sh", "term history hash mismatch")
require("scripts/phase35-governance-remediation-smoke.sh", "incident-commander")
require("scripts/phase35-governance-remediation-smoke.sh", "state='resolved'")
require("docker-compose.yml", '"phase35-smoke"')
require("Makefile", "phase35-governance-remediation-smoke")
require("scripts/gate.sh", "phase35-governance-remediation-check.py")
require(".github/workflows/ci.yml", "phase35-governance-remediation-smoke.sh")
require("README.md", "### Phase 35: controlled governance dead-evidence remediation")

print("Phase 35 controlled governance dead-evidence remediation guardrails: ok")
