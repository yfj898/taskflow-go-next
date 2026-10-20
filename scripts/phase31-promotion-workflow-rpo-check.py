#!/usr/bin/env python3
"""Static guardrails for Phase 31 coordinated promotion workflow and RPO fencing."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMPromotionIntent")
require("internal/domain/control_plane_delivery.go", "TransitionLeaseUntil")
require("internal/domain/control_plane_delivery.go", "RPOFenced")
require("internal/domain/control_plane_delivery.go", "ControlPlaneSIEMReceiverTermHistory")
require("internal/domain/control_plane_siem_term_history.go", "ComputeControlPlaneSIEMReceiverTermHistoryHash")
require("internal/repository/control_plane_siem_promotion.go", "CreateControlPlaneSIEMPromotionIntent")
require("internal/repository/control_plane_siem_promotion.go", "requester and approver must be different operators")
require("internal/repository/control_plane_siem_promotion.go", "ClaimControlPlaneSIEMPromotionExecution")
require("internal/repository/control_plane_siem_promotion.go", "CommitControlPlaneSIEMPromotionExecution")
require("internal/repository/control_plane_siem_promotion.go", "ErrControlPlaneSIEMPromotionBusy")
require("internal/repository/control_plane_siem_promotion.go", "ObserveControlPlaneSIEMReceiverRPO")
require("internal/repository/control_plane_siem_promotion.go", "appendControlPlaneSIEMTermHistoryTx")
require("internal/siem/promotion_workflow.go", "ExecutePromotionIntent")
require("internal/siem/promotion_workflow.go", "promoteReceiverTermCoordinated")
require("internal/siem/promotion_workflow.go", "verifyReceiverTermHistory")
require("internal/siem/receiver_control.go", "promotion workflow requires an approved promotion intent")
require("scripts/siem-sink-mock.py", "term_conflict_rejections")
require("scripts/siem-sink-mock.py", "conflicting receiver active assignment for term")
require("internal/handler/router.go", "/control-plane/siem/receivers/intents/:id/approve")
require("internal/handler/router.go", "/control-plane/siem/receivers/intents/:id/execute")
require("internal/handler/router.go", "/control-plane/siem/receivers/term-history")
require("internal/metrics/metrics.go", "control_plane_siem_promotion_execution_total")
require("internal/metrics/metrics.go", "control_plane_siem_rpo_fenced")
require("internal/metrics/metrics.go", "control_plane_siem_term_history_integrity")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED")
require("internal/config/config.go", "CONTROL_PLANE_SIEM_RPO_POLICY_ENABLED")
require("docker-compose.yml", '"phase31-smoke"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_PROMOTION_WORKFLOW_ENABLED: "false"')
require("Makefile", "phase31-promotion-workflow-rpo-smoke")
require("scripts/gate.sh", "phase31-promotion-workflow-rpo-check.py")
require(".github/workflows/ci.yml", "phase31-promotion-workflow-rpo-smoke.sh")
require("README.md", "Phase 31: coordinated promotion approval, term ledger and RPO fencing")
require("scripts/phase31-promotion-workflow-rpo-smoke.sh", "prove durable RPO SLO fencing")
require("scripts/phase31-promotion-workflow-rpo-smoke.sh", "prove two-person approval and multi-instance execution lease")
require("scripts/phase31-promotion-workflow-rpo-smoke.sh", "prove term-history tamper detection")

print("Phase 31 coordinated promotion approval, term ledger and RPO fencing guardrails: ok")
