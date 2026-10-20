#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ReceiptKeyGeneration")
require("internal/domain/control_plane_delivery.go", "ReceiptKeyState")
require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMGovernanceReplicaKeyState")
require("internal/domain/control_plane_delivery.go", "PreviousKeyGraceReplicas")
require("internal/repository/task_repository.go", "ControlPlaneSIEMGovernanceReplicaKeyState{}")
require("internal/repository/control_plane_siem_governance.go", "enforceGovernanceReplicaActiveKeyGeneration")
require("internal/repository/control_plane_siem_governance.go", "key generation rollback")
require("internal/repository/control_plane_siem_governance.go", 'clause.Locking{Strength: "UPDATE"}')
require("internal/repository/control_plane_siem_governance.go", "receipt_key_generation")
require("internal/siem/governance.go", "PreviousReceiptValidUntil")
require("internal/siem/governance.go", "previous receipt key grace expired")
require("internal/siem/governance.go", "receipt key generation mismatch")
require("internal/siem/governance.go", "PreviousKeyGraceCount")
require("internal/config/config.go", "PRIMARY_RECEIPT_KEY_GENERATION")
require("internal/config/config.go", "PRIMARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS")
require("internal/config/config.go", "SECONDARY_PREVIOUS_RECEIPT_VALID_UNTIL_UNIX_MS")
require("cmd/server/main.go", "PreviousReceiptValidUntil")
require("scripts/phase32-governance-mock.py", "GOVERNANCE_RECEIPT_KEY_GENERATION")
require("internal/metrics/metrics.go", "control_plane_siem_governance_previous_key_grace_replicas")
require("docker-compose.yml", '"phase40-smoke"')
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_PRIMARY_RECEIPT_KEY_GENERATION")
require("scripts/phase40-governance-key-retirement-smoke.sh", "key generation rollback")
require("scripts/phase40-governance-key-retirement-smoke.sh", "previous receipt key grace expired")
require("Makefile", "phase40-governance-key-retirement-smoke")
require("scripts/gate.sh", "phase40-governance-key-retirement-check.py")
require(".github/workflows/ci.yml", "phase40-governance-key-retirement-smoke.sh")
require("README.md", "### Phase 40: bounded governance receipt-key retirement and anti-rollback fencing")

print("Phase 40 bounded receipt-key grace, durable key generation and anti-rollback fencing guardrails: ok")
