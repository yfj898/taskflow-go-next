#!/usr/bin/env python3
"""Validate real-Kafka poison/DLQ release-evidence wiring."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needle: str) -> None:
    text = (ROOT / path).read_text()
    if needle not in text:
        raise SystemExit(f"Kafka release-evidence guardrail missing from {path}: {needle}")


require("docker-compose.yml", "KAFKA_TOPIC: ${KAFKA_TOPIC:-taskflow.tasks}")
require("docker-compose.yml", "KAFKA_GROUP: ${KAFKA_GROUP:-taskflow-workers}")
require("docker-compose.yml", 'profiles: ["kafka"]')
require("docker-compose.yml", "KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:19092")
require("docker-compose.yml", "${TASKFLOW_HTTP_PORT:-8080}:8080")
require("internal/queue/kafka.go", 'Backend: "kafka"')
require("internal/queue/kafka.go", 'DeliveryID: deliveryID')
require("internal/queue/kafka.go", "CommitMessages")
require("internal/worker/pool.go", "p.quarantinePoison(poison)")
require("scripts/release-kafka-dlq-smoke.sh", "kafka-console-producer.sh")
require("scripts/release-kafka-dlq-smoke.sh", "kafka-consumer-groups.sh")
require("scripts/release-kafka-dlq-smoke.sh", "QUEUE_BACKEND=kafka")
require("scripts/release-kafka-dlq-smoke.sh", "http://127.0.0.1:18080")
require("scripts/release-kafka-dlq-smoke.sh", "/api/v1/queue/dead-letter-archives/$archive_id/restore")
require("scripts/release-kafka-dlq-smoke.sh", "stale generation cannot bypass Phase 16 on Kafka")
require("Makefile", "release-kafka-dlq-smoke")
require("scripts/gate.sh", "release-kafka-dlq-check.py")
require(".github/workflows/ci.yml", "release-kafka-dlq-smoke.sh")
require("README.md", "### Release evidence: real Kafka poison/DLQ integration")

print("real Kafka poison/DLQ release-evidence guardrails: ok")
