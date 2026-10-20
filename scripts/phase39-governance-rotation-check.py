#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "MembershipEpoch")
require("internal/siem/governance.go", "PreviousReceiptKeyID")
require("internal/siem/governance.go", "governanceReceiptVerificationKey")
require("internal/siem/governance.go", "receipt membership epoch mismatch")
require("internal/siem/governance.go", "MembershipEpoch()")
require("internal/siem/governance_phase39_test.go", "RollingRotation")
require("internal/siem/governance_phase39_test.go", "StaleMembershipEpoch")
require("internal/repository/control_plane_siem_governance.go", "existing.MembershipEpoch")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH")
require("internal/config/config.go", "PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY")
require("internal/config/config.go", "SECONDARY_PREVIOUS_RECEIPT_HMAC_KEY")
require("cmd/server/main.go", "PreviousReceiptKeyID")
require("scripts/phase32-governance-mock.py", "GOVERNANCE_MEMBERSHIP_EPOCH")
require("docker-compose.yml", '"phase39-smoke"')
require("deploy/k8s/configmap.yaml", "CONTROL_PLANE_SIEM_GOVERNANCE_MEMBERSHIP_EPOCH")
require("deploy/k8s/secret.example.yaml", "PRIMARY_PREVIOUS_RECEIPT_HMAC_KEY")
require("scripts/phase39-governance-rotation-smoke.sh", "previous receipt keys preserved quorum during rolling rotation")
require("scripts/phase39-governance-rotation-smoke.sh", "stale membership epoch")
require("Makefile", "phase39-governance-rotation-smoke")
require("scripts/gate.sh", "phase39-governance-rotation-check.py")
require(".github/workflows/ci.yml", "phase39-governance-rotation-smoke.sh")
require("README.md", "### Phase 39: governance receipt key rotation and membership epoch fencing")

print("Phase 39 governance receipt key rotation and membership epoch fencing guardrails: ok")
