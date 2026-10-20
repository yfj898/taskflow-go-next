#!/usr/bin/env python3
"""Static guardrails for Phase 25 SIEM delivery reliability."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/domain/control_plane_delivery.go", "type ControlPlaneAuditDelivery struct")
require("internal/domain/control_plane_delivery.go", "type ControlPlaneAuditDeliveryDLQ struct")
require("internal/repository/task_repository.go", "&domain.ControlPlaneAuditDelivery{}")
require("internal/repository/task_repository.go", "ConfigureControlPlaneSIEMDelivery")
require("internal/repository/control_plane_audit.go", "createControlPlaneAuditDelivery")
require("internal/repository/control_plane_audit.go", "BackfillControlPlaneAuditDeliveries")
require("internal/repository/control_plane_delivery.go", "ClaimControlPlaneAuditDeliveries")
require("internal/repository/control_plane_delivery.go", "FailControlPlaneAuditDeliveries")
require("internal/repository/control_plane_delivery.go", "ReplayControlPlaneAuditDeliveryDLQ")
require("internal/siem/signer.go", "type CheckpointSigner interface")
require("internal/siem/signer.go", "NewHMACSHA256Signer")
require("internal/siem/delivery.go", 'X-TaskFlow-Audit-Signature')
require("internal/siem/delivery.go", 'Idempotency-Key')
require("internal/siem/delivery.go", "retryDelay")
require("cmd/server/main.go", "BackfillControlPlaneAuditDeliveries")
require("cmd/server/main.go", "deliveryWorker.Start(ctx)")
require("internal/handler/router.go", 'controlV1.GET("/control-plane/siem/status"')
require("internal/handler/router.go", 'controlV1.POST("/control-plane/siem/dlq/:id/replay"')
require("internal/metrics/metrics.go", "control_plane_siem_deliveries_total")
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_SIEM_DELIVERY_ENABLED", false)')
require("internal/config/config.go", 'getenv("CONTROL_PLANE_SIEM_SIGNING_KEY", "")')
require("docker-compose.yml", '"phase25-smoke"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_DELIVERY_ENABLED: "false"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_SIGNING_KEY:")
require("scripts/siem-sink-mock.py", "hmac.compare_digest")
require("scripts/phase25-siem-delivery-smoke.sh", "taskflow_phase25")
require("Makefile", "phase25-siem-delivery-smoke")
require("README.md", "Phase 25: signed SIEM delivery outbox")

print("Phase 25 signed SIEM delivery, retry and DLQ guardrails: ok")

