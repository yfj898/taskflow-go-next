#!/usr/bin/env python3
"""Validate real MinIO/S3 primary+replica release-evidence wiring."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"MinIO/S3 release-evidence guardrail missing from {path}: {needle}")


require("go.mod", "github.com/minio/minio-go/v7 v7.1.0")
require("internal/archive/s3.go", "type S3Sink struct")
require("internal/archive/s3.go", "PutObject")
require("internal/archive/s3.go", "GetObject")
require("internal/archive/s3.go", "RemoveObject")
require("internal/archive/s3.go", "ListObjects")
require("cmd/server/main.go", 'case "s3", "minio", "s3-compatible", "s3_compatible":')
require("internal/config/config.go", 'getenv("QUEUE_DLQ_ARCHIVE_S3_ENDPOINT", "")')
require("internal/config/config.go", 'getenv("QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT", "")')
require("docker-compose.yml", 'profiles: ["archive-s3"]')
require("docker-compose.yml", "minio-primary-data:/data")
require("docker-compose.yml", "minio-replica-data:/data")
require("docker-compose.yml", "RELEASE.2025-04-22T22-12-26Z")
require("deploy/k8s/configmap.yaml", 'QUEUE_DLQ_ARCHIVE_S3_ENDPOINT: ""')
require("deploy/k8s/configmap.yaml", 'QUEUE_DLQ_ARCHIVE_REPLICA_S3_ENDPOINT: ""')
require("deploy/k8s/secret.example.yaml", 'QUEUE_DLQ_ARCHIVE_S3_ACCESS_KEY: ""')
require("deploy/k8s/secret.example.yaml", 'QUEUE_DLQ_ARCHIVE_REPLICA_S3_ACCESS_KEY: ""')
require("scripts/release-s3-minio-smoke.sh", "QUEUE_DLQ_ARCHIVE_MIN_COPIES=2")
require("scripts/release-s3-minio-smoke.sh", 'phase="replica outage fail closed and repair"')
require("scripts/release-s3-minio-smoke.sh", 'phase="primary outage replica fallback"')
require("scripts/release-s3-minio-smoke.sh", 'phase="S3 orphan reconciliation"')
require("scripts/release-s3-minio-smoke.sh", 'phase="S3 retention GC tombstone"')
require("Makefile", "release-s3-minio-smoke")
require("scripts/gate.sh", "release-s3-minio-check.py")
require("README.md", "Release evidence: real MinIO/S3 primary+replica archive validation")

print("real MinIO/S3 primary+replica release-evidence guardrails: ok")
