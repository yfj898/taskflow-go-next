#!/usr/bin/env python3
"""Static guardrails for Phase 24 control-plane audit integrity and SIEM export."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_audit.go", "ChainSequence")
require("internal/domain/control_plane_audit.go", "PreviousHash")
require("internal/domain/control_plane_audit.go", "RecordHash")
require("internal/domain/control_plane_audit.go", "ComputeControlPlaneAuditHash")
require("internal/domain/control_plane_audit.go", "ControlPlaneAuditChainState")
require("internal/repository/task_repository.go", "&domain.ControlPlaneAuditChainState{}")
require("internal/repository/control_plane_audit.go", "lockControlPlaneAuditChainState")
require("internal/repository/control_plane_audit.go", "VerifyControlPlaneAuditChain")
require("internal/repository/control_plane_audit.go", "PurgeControlPlaneAuditsBefore")
require("internal/repository/control_plane_audit.go", "RetentionAnchorSequence")
require("internal/service/control_plane_audit.go", "SealLegacy")
require("internal/monitor/control_plane_audit.go", "StartControlPlaneAuditLifecycle")
require("internal/handler/router.go", 'controlV1.GET("/control-plane/audits/verify"')
require("internal/handler/router.go", 'controlV1.GET("/control-plane/audits/export"')
require("internal/handler/router.go", 'Content-Type", "application/x-ndjson"')
require("internal/handler/router.go", 'http.StatusGone')
require("internal/metrics/metrics.go", "control_plane_audit_integrity")
require("internal/metrics/metrics.go", "control_plane_audit_exports_total")
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_AUDIT_RETENTION_ENABLED", true)')
require("internal/config/config.go", 'getenvInt("CONTROL_PLANE_AUDIT_RETENTION_MS", 7776000000)')
require("docker-compose.yml", "CONTROL_PLANE_AUDIT_RETENTION_ENABLED")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_AUDIT_RETENTION_ENABLED: "true"')
require("Makefile", "phase24-control-plane-integrity-smoke")
require("scripts/gate.sh", "phase24-control-plane-integrity-check.py")
require("README.md", "Phase 24: tamper-evident audit chain, retention anchors and SIEM export")

print("Phase 24 control-plane hash-chain, retention and SIEM-export guardrails: ok")
