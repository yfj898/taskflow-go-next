#!/usr/bin/env python3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMPromotionApproval")
require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMTermLedgerAnchor")
require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMIncidentEvidence")
require("internal/domain/control_plane_delivery.go", "Emergency")
require("internal/domain/control_plane_delivery.go", "IncidentID")
require("internal/domain/control_plane_siem_term_history.go", "HashVersion >= 2")
require("internal/repository/control_plane_siem_governance.go", "ApproveControlPlaneSIEMPromotionIntentWithQuorum")
require("internal/domain/control_plane_delivery.go", "idx_siem_promotion_approval")
require("internal/repository/control_plane_siem_governance.go", "CommitControlPlaneSIEMPromotionExecutionV2")
require("internal/repository/control_plane_siem_governance.go", "CreateControlPlaneSIEMIncidentEvidence")
require("internal/handler/control_plane_auth.go", "AdditionalBearerCredentials")
require("internal/handler/control_plane_auth.go", 'controlPlaneCredentialSlotKey, "additional"')
require("internal/siem/promotion_workflow.go", "BreakGlassPromotion")
require("internal/siem/promotion_workflow.go", '"incident-commander"')
require("internal/siem/promotion_workflow.go", "PromotionApprovalQuorum")
require("internal/siem/promotion_workflow.go", "cannot anchor tampered SIEM receiver term history")
require("internal/siem/governance.go", "governance anchor receipt hash mismatch")
require("internal/siem/governance.go", "governance incident receipt identity mismatch")
require("internal/metrics/metrics.go", "ControlPlaneSIEMBreakGlass")
require("internal/metrics/metrics.go", "ControlPlaneSIEMTermAnchors")
require("internal/metrics/metrics.go", "ControlPlaneSIEMIncidentEvidence")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_PROMOTION_APPROVAL_QUORUM")
require("internal/config/config.go", "CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS")
require("cmd/server/main.go", "NewGovernanceClient")
require("internal/handler/router.go", '"/control-plane/siem/receivers/break-glass"')
require("internal/handler/router.go", '"/control-plane/siem/receivers/term-history/anchor"')
require("internal/handler/router.go", '"/control-plane/siem/incidents"')
require("scripts/phase32-governance-mock.py", "immutable term anchor conflict")
require("docker-compose.yml", '"phase32-smoke"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_BREAK_GLASS_ENABLED: "false"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_ADDITIONAL_BEARER_CREDENTIALS")
require("Makefile", "phase32-governance-smoke")
require("scripts/gate.sh", "phase32-governance-check.py")
require(".github/workflows/ci.yml", "phase32-governance-smoke.sh")
require("README.md", "### Phase 32: break-glass governance, approval quorum and external incident evidence")

print("Phase 32 break-glass, quorum governance, external term anchoring and incident evidence guardrails: ok")
