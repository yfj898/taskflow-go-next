#!/usr/bin/env python3
"""Static guardrails for Phase 23 control-plane audit and credential rotation."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_audit.go", "type ControlPlaneAudit struct")
require("internal/repository/task_repository.go", "&domain.ControlPlaneAudit{}")
require("internal/repository/control_plane_audit.go", "CreateControlPlaneAudit")
require("internal/repository/control_plane_audit.go", "CompleteControlPlaneAudit")
require("internal/service/control_plane_audit.go", "type ControlPlaneAuditService struct")
require("internal/handler/control_plane_auth.go", "PreviousBearerToken")
require("internal/handler/control_plane_auth.go", 'c.Set(controlPlaneCredentialSlotKey, "previous")')
require("internal/handler/control_plane_audit.go", 'Outcome:        "started"')
require("internal/handler/control_plane_audit.go", "controlPlaneAuditOutcome")
require("internal/handler/router.go", 'controlV1.GET("/control-plane/audits"')
require("internal/metrics/metrics.go", "control_plane_audits_total")
require("internal/config/config.go", 'getenv("CONTROL_PLANE_PREVIOUS_BEARER_TOKEN", "")')
require("internal/config/config.go", 'getenv("CONTROL_PLANE_OPERATOR_ID", "operator")')
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_AUDIT_ENABLED", true)')
require("docker-compose.yml", "CONTROL_PLANE_PREVIOUS_BEARER_TOKEN")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_AUDIT_ENABLED: "true"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_PREVIOUS_BEARER_TOKEN:")
require("Makefile", "phase23-control-plane-audit-smoke")

print("Phase 23 control-plane audit and credential-rotation guardrails: ok")
