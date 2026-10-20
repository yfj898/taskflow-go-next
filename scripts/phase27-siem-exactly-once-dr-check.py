#!/usr/bin/env python3
"""Static guardrails for Phase 27 SIEM ACK, direct KMS and DR evidence."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


def forbid(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle in text:
        raise SystemExit(f"{path}: forbidden {needle!r}")


require("internal/domain/control_plane_delivery.go", "ReceiptID")
require("internal/domain/control_plane_delivery.go", "IdempotencyKey")
require("internal/domain/control_plane_delivery.go", "PayloadSHA256")
require("internal/domain/control_plane_delivery.go", "ReplicaCopies")
require("internal/repository/control_plane_delivery.go", "ListControlPlaneAuditDeliveryReceipts")
require("internal/handler/router.go", '"/control-plane/siem/receipts"')
require("internal/siem/delivery.go", "taskflow-control-plane-siem-v2")
forbid("internal/siem/delivery.go", '`json:"delivery_id"`')
require("internal/siem/delivery.go", "SIEM acknowledgement does not match delivery batch")
require("internal/siem/delivery.go", "deliveryEvidenceObjectKey")
require("internal/siem/delivery.go", "firstAudit.CompletedAt")
require("internal/siem/delivery.go", 'fmt.Fprintf(h, "%d:%s\\n", delivery.ChainSequence, delivery.RecordHash)')
require("internal/siem/evidence_replica.go", "MultiSinkEvidenceReplica")
require("internal/siem/evidence_replica.go", "contains conflicting object")
require("internal/siem/aws_kms_signer.go", "TrentService.GenerateMac")
require("internal/siem/aws_kms_signer.go", "AWS4-HMAC-SHA256")
require("internal/siem/aws_kms_signer.go", "HMAC_SHA_256")
require("internal/config/config.go", 'getenv("CONTROL_PLANE_SIEM_AWS_KMS_REGION", "")')
require("internal/config/config.go", 'getenvBool("CONTROL_PLANE_SIEM_DR_ENABLED", false)')
require("cmd/server/main.go", "NewAWSKMSMACSigner")
require("cmd/server/main.go", "NewMultiSinkEvidenceReplica")
require("docker-compose.yml", '"phase27-smoke"')
require("docker-compose.yml", "CONTROL_PLANE_SIEM_DR_PRIMARY_S3_ENDPOINT")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_DR_ENABLED: "false"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_AWS_SECRET_ACCESS_KEY")
require("scripts/aws-kms-mock.py", "TrentService.GenerateMac")
require("scripts/siem-sink-mock.py", "drop_after_accept_remaining")
require("scripts/siem-sink-mock.py", "idempotency key reused with different payload")
require("scripts/phase27-siem-exactly-once-dr-smoke.sh", "prove local SIEM state loss rebuilds from retained audit chain")
require("Makefile", "phase27-siem-exactly-once-dr-smoke")
require("scripts/gate.sh", "phase27-siem-exactly-once-dr-check.py")
require("README.md", "Phase 27: receiver-acknowledged exactly-once acceptance and cross-region audit DR")

print("Phase 27 SIEM ACK, direct AWS-KMS protocol and cross-region DR guardrails: ok")
