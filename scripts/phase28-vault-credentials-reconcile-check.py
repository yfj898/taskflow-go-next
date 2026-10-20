#!/usr/bin/env python3
"""Static guardrails for Phase 28 Vault, AWS workload identity and SIEM recovery."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"{path}: missing {needle!r}")


require("internal/siem/vault_transit_signer.go", "VaultTransitSigner")
require("internal/siem/vault_transit_signer.go", 'SignatureAlgorithm: "pkcs1v15"')
require("internal/siem/vault_transit_signer.go", "Prehashed:          true")
require("internal/siem/aws_credentials.go", "AWSWebIdentityProvider")
require("internal/siem/aws_credentials.go", "AssumeRoleWithWebIdentity")
require("internal/siem/aws_credentials.go", "AWSIMDSv2Provider")
require("internal/siem/aws_credentials.go", "X-aws-ec2-metadata-token")
require("internal/siem/aws_kms_signer.go", "AWSCredentialProvider")
require("internal/siem/checkpoint_reconcile.go", "taskflow-control-plane-siem-checkpoint-v1")
require("internal/siem/checkpoint_reconcile.go", "canonicalDeliveryIdentityFromAudit")
require("internal/siem/checkpoint_reconcile.go", "receiver-checkpoint")
require("internal/siem/evidence_replica.go", "VerifyReceiptEvidence")
require("internal/repository/control_plane_delivery.go", "ImportControlPlaneAuditDeliveryReceipt")
require("internal/domain/control_plane_delivery.go", "RecoverySource")
require("internal/metrics/metrics.go", "control_plane_siem_reconcile_total")
require("cmd/server/main.go", 'case "vault-transit":')
require("cmd/server/main.go", "NewAWSWebIdentityProvider")
require("cmd/server/main.go", "NewAWSIMDSv2Provider")
require("cmd/server/main.go", "NewCheckpointReconciler")
require("scripts/siem-sink-mock.py", 'parsed.path == "/checkpoint"')
require("scripts/siem-sink-mock.py", "SIEM_VAULT_VERIFY_ENDPOINT")
require("scripts/aws-credentials-mock.py", "AssumeRoleWithWebIdentity")
require("scripts/aws-credentials-mock.py", "/latest/api/token")
require("scripts/aws-kms-mock.py", "X-Amz-Security-Token")
require("docker-compose.yml", 'profiles: ["phase28-smoke"]')
require("docker-compose.yml", "hashicorp/vault:2.0.4")
require("docker-compose.yml", "CONTROL_PLANE_SIEM_RECONCILE_ENABLED")
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_AWS_CREDENTIALS_SOURCE: "static"')
require("deploy/k8s/configmap.yaml", 'CONTROL_PLANE_SIEM_RECONCILE_ENABLED: "false"')
require("deploy/k8s/secret.example.yaml", "CONTROL_PLANE_SIEM_VAULT_TOKEN")
require("Makefile", "phase28-vault-credentials-reconcile-smoke")
require("scripts/gate.sh", "phase28-vault-credentials-reconcile-check.py")
require(".github/workflows/ci.yml", "phase28-vault-credentials-reconcile-smoke.sh")
require("README.md", "Phase 28: Vault Transit, AWS workload identity and receiver-checkpoint disaster recovery")

print("Phase 28 Vault Transit, AWS workload identity and checkpoint-recovery guardrails: ok")
