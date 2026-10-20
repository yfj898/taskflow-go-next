#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMGovernanceReplicaReceipt")
require("internal/domain/control_plane_delivery.go", "VerifiedReplicaReceipts")
require("internal/domain/control_plane_delivery.go", "ReplicaReceiptDebt")
require("internal/repository/task_repository.go", "ControlPlaneSIEMGovernanceReplicaReceipt{}")
require("internal/repository/control_plane_siem_governance.go", "RecordControlPlaneSIEMGovernanceReplicaReceipts")
require("internal/repository/control_plane_siem_governance.go", "governance replica receipt conflict")
require("internal/siem/governance.go", "NewGovernanceHAClient")
require("internal/siem/governance.go", "GovernanceConsistencyError")
require("internal/siem/governance.go", "verifyGovernanceAnchorReplicaReceipt")
require("internal/siem/governance.go", "verifyGovernanceAlertReplicaReceipt")
require("internal/siem/governance.go", "governance anchor receipt quorum incomplete")
require("internal/siem/governance.go", "receipt signature mismatch")
require("internal/siem/governance_reconcile.go", "governance replica quorum incomplete")
require("internal/siem/governance_reconcile.go", "RecordControlPlaneSIEMGovernanceReplicaReceipts")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_RECEIPT_QUORUM")
require("internal/config/config.go", "dedicated receipt HMAC keys")
require("internal/handler/router.go", '"/control-plane/siem/governance/replica-receipts"')
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceReplicaReceipts")
require("internal/metrics/metrics.go", "ControlPlaneSIEMGovernanceQuorum")
require("scripts/phase32-governance-mock.py", "GOVERNANCE_RECEIPT_HMAC_KEY")
require("scripts/phase32-governance-mock.py", "receipt_signature")
require("docker-compose.yml", "siem-governance-secondary:")
require("docker-compose.yml", '"phase38-smoke"')
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_HA_ENABLED")
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_SECONDARY_RECEIPT_HMAC_KEY")
require("scripts/phase38-governance-ha-smoke.sh", "replica quorum incomplete")
require("scripts/phase38-governance-ha-smoke.sh", "signed replica divergence")
require("Makefile", "phase38-governance-ha-smoke")
require("scripts/gate.sh", "phase38-governance-ha-check.py")
require(".github/workflows/ci.yml", "phase38-governance-ha-smoke.sh")
require("README.md", "### Phase 38: signed governance replica quorum and consistency fencing")

print("Phase 38 signed governance replica quorum, durable receipts and consistency fencing guardrails: ok")
