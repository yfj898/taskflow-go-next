#!/usr/bin/env sh
set -eu

echo "== format =="
formatted="$(gofmt -l ./cmd ./internal scripts/ai-phase7-client.go scripts/ai-phase10-client/main.go scripts/phase41-oidc-mock/main.go)"
if [ -n "$formatted" ]; then
  echo "$formatted"
  echo "gofmt check failed" >&2
  exit 1
fi

echo "== modules =="
go mod tidy -diff -go=1.25.0

echo "== tests =="
go test -mod=readonly ./...

echo "== vet =="
go vet -mod=readonly ./...

echo "== build =="
build_dir="${GATE_BUILD_DIR:-.gate-bin}"
rm -rf "$build_dir"
mkdir -p "$build_dir"
trap 'rm -rf "$build_dir"' EXIT
CGO_ENABLED=0 go build -mod=readonly -o "$build_dir/taskflow" ./cmd/server
CGO_ENABLED=0 go build -mod=readonly -o "$build_dir/executor-worker" ./cmd/executor-worker
CGO_ENABLED=0 go build -mod=readonly -o "$build_dir/ai-phase7-client" ./scripts/ai-phase7-client.go
CGO_ENABLED=0 go build -mod=readonly -o "$build_dir/ai-phase10-client" ./scripts/ai-phase10-client

echo "== compose =="
docker compose config >/dev/null

echo "== scripts =="
sh -n \
  scripts/smoke.sh \
  scripts/grpc-smoke.sh \
  scripts/outbox-recovery.sh \
  scripts/cloud-control-quota-smoke.sh \
  scripts/cloud-control-saga-smoke.sh \
  scripts/ai-smoke.sh \
  scripts/ai-phase2-smoke.sh \
  scripts/ai-phase3-smoke.sh \
  scripts/ai-phase4-smoke.sh \
  scripts/ai-phase5-smoke.sh \
  scripts/ai-phase6-smoke.sh \
  scripts/ai-phase7-smoke.sh \
  scripts/ai-phase8-smoke.sh \
  scripts/ai-phase9-smoke.sh \
  scripts/ai-phase10-smoke.sh \
  scripts/ai-phase11-smoke.sh \
  scripts/phase12-autoscaling-smoke.sh \
  scripts/phase13-drain-smoke.sh \
  scripts/phase14-availability-smoke.sh \
  scripts/phase15-reconciler-smoke.sh \
  scripts/phase16-enqueue-fencing-smoke.sh \
  scripts/phase17-compaction-smoke.sh \
  scripts/phase18-dlq-smoke.sh \
  scripts/phase19-dlq-lifecycle-smoke.sh \
  scripts/phase20-dlq-archive-smoke.sh \
  scripts/phase21-dlq-archive-object-smoke.sh \
  scripts/phase22-control-plane-auth-smoke.sh \
  scripts/phase23-control-plane-audit-smoke.sh \
  scripts/phase24-control-plane-integrity-smoke.sh \
  scripts/phase25-siem-delivery-smoke.sh \
  scripts/phase26-siem-ha-kms-smoke.sh \
  scripts/phase27-siem-exactly-once-dr-smoke.sh \
  scripts/phase28-vault-credentials-reconcile-smoke.sh \
  scripts/phase29-receiver-failover-dr-smoke.sh \
  scripts/phase30-receiver-term-failback-dr-smoke.sh \
  scripts/phase31-promotion-workflow-rpo-smoke.sh \
  scripts/phase32-governance-smoke.sh \
  scripts/phase33-governance-retry-smoke.sh \
  scripts/phase34-governance-debt-smoke.sh \
  scripts/phase35-governance-remediation-smoke.sh \
  scripts/phase36-governance-four-eyes-smoke.sh \
  scripts/phase37-governance-approval-lifecycle-smoke.sh \
  scripts/phase38-governance-ha-smoke.sh \
  scripts/phase39-governance-rotation-smoke.sh \
  scripts/phase40-governance-key-retirement-smoke.sh \
  scripts/phase41-oidc-rbac-smoke.sh \
  scripts/phase42-mysql-ha-pitr-smoke.sh \
  scripts/phase43-slo-observability-smoke.sh \
  scripts/phase44-supply-chain-smoke.sh \
  scripts/phase45-multiregion-failover-smoke.sh \
  scripts/release-kafka-dlq-smoke.sh \
  scripts/release-k8s-keda-smoke.sh \
  scripts/release-s3-minio-smoke.sh \
  scripts/perf-benchmark.sh \
  scripts/perf-multi.sh \
  scripts/perf-ab.sh
python3 -m py_compile scripts/perf-load.py scripts/perf_multi.py scripts/perf_multi_test.py scripts/perf_benchmark_test.py scripts/ai-gateway-mock.py scripts/siem-sink-mock.py scripts/kms-signer-mock.py scripts/aws-kms-mock.py scripts/aws-credentials-mock.py scripts/phase32-governance-mock.py scripts/phase33-governance-retry-check.py scripts/phase34-governance-debt-check.py scripts/phase35-governance-remediation-check.py scripts/phase36-governance-four-eyes-check.py scripts/phase37-governance-approval-lifecycle-check.py scripts/phase38-governance-ha-check.py scripts/phase39-governance-rotation-check.py scripts/phase40-governance-key-retirement-check.py scripts/phase41-oidc-rbac-check.py scripts/phase42-mysql-ha-pitr-check.py scripts/phase43-slo-observability-check.py scripts/phase44-supply-chain-check.py scripts/phase45-multiregion-failover-check.py scripts/portfolio-seal-check.py scripts/phase12-keda-check.py scripts/phase13-k8s-check.py scripts/phase14-k8s-check.py scripts/phase15-coordination-check.py scripts/phase16-fencing-check.py scripts/phase17-compaction-check.py scripts/phase18-dlq-check.py scripts/phase19-dlq-lifecycle-check.py scripts/phase20-dlq-archive-check.py scripts/phase21-dlq-archive-object-check.py scripts/phase22-control-plane-auth-check.py scripts/phase23-control-plane-audit-check.py scripts/phase24-control-plane-integrity-check.py scripts/phase25-siem-delivery-check.py scripts/phase26-siem-ha-kms-check.py scripts/phase27-siem-exactly-once-dr-check.py scripts/phase28-vault-credentials-reconcile-check.py scripts/phase29-receiver-failover-dr-check.py scripts/phase30-receiver-term-failback-dr-check.py scripts/phase31-promotion-workflow-rpo-check.py scripts/phase32-governance-check.py scripts/release-kafka-dlq-check.py scripts/release-k8s-keda-check.py scripts/release-s3-minio-check.py
PYTHONPATH=scripts python3 -m unittest scripts/perf_multi_test.py scripts/perf_benchmark_test.py
python3 scripts/phase12-keda-check.py
python3 scripts/phase13-k8s-check.py
python3 scripts/phase14-k8s-check.py
python3 scripts/phase15-coordination-check.py
python3 scripts/phase16-fencing-check.py
python3 scripts/phase17-compaction-check.py
python3 scripts/phase18-dlq-check.py
python3 scripts/phase19-dlq-lifecycle-check.py
python3 scripts/phase20-dlq-archive-check.py
python3 scripts/phase21-dlq-archive-object-check.py
python3 scripts/phase22-control-plane-auth-check.py
python3 scripts/phase23-control-plane-audit-check.py
python3 scripts/phase24-control-plane-integrity-check.py
python3 scripts/phase25-siem-delivery-check.py
python3 scripts/phase26-siem-ha-kms-check.py
python3 scripts/phase27-siem-exactly-once-dr-check.py
python3 scripts/phase28-vault-credentials-reconcile-check.py
python3 scripts/phase29-receiver-failover-dr-check.py
python3 scripts/phase30-receiver-term-failback-dr-check.py
python3 scripts/phase31-promotion-workflow-rpo-check.py
python3 scripts/phase32-governance-check.py
python3 scripts/phase33-governance-retry-check.py
python3 scripts/phase34-governance-debt-check.py
python3 scripts/phase35-governance-remediation-check.py
python3 scripts/phase36-governance-four-eyes-check.py
python3 scripts/phase37-governance-approval-lifecycle-check.py
python3 scripts/phase38-governance-ha-check.py
python3 scripts/phase39-governance-rotation-check.py
python3 scripts/phase40-governance-key-retirement-check.py
python3 scripts/phase41-oidc-rbac-check.py
python3 scripts/phase42-mysql-ha-pitr-check.py
python3 scripts/phase43-slo-observability-check.py
python3 scripts/phase44-supply-chain-check.py
python3 scripts/phase45-multiregion-failover-check.py
python3 scripts/portfolio-seal-check.py
python3 scripts/release-kafka-dlq-check.py
python3 scripts/release-k8s-keda-check.py
python3 scripts/release-s3-minio-check.py

if [ "${GATE_DOCKER_BUILD:-1}" = "1" ]; then
  echo "== docker build =="
  docker build \
    --build-arg GOPROXY="${GOPROXY:-https://proxy.golang.org|https://goproxy.cn|direct}" \
    -t taskflow-go:gate .
fi

echo "GATE_OK"
