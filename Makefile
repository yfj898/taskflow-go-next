.PHONY: tidy fmt-check vet test schema-status schema-expand gate portfolio-check cloud-control-test cloud-control-quota-smoke cloud-control-saga-smoke cloud-control-kind-e2e cloud-control-kind-chaos cloud-control-scale-benchmark cloud-control-scale-ab gpuflow-test gpuflow-bench gpuflow-fairness-bench gpuflow-server run run-worker up down smoke grpc-smoke outbox-smoke ai-smoke ai-phase2-smoke ai-phase3-smoke ai-phase4-smoke ai-phase5-smoke ai-phase6-smoke ai-phase7-smoke ai-phase8-smoke ai-phase9-smoke ai-phase10-smoke ai-phase11-smoke phase12-autoscaling-smoke phase13-drain-smoke phase14-availability-smoke phase15-reconciler-smoke phase16-enqueue-fencing-smoke phase17-compaction-smoke phase18-dlq-smoke phase19-dlq-lifecycle-smoke phase20-dlq-archive-smoke phase21-dlq-archive-object-smoke phase22-control-plane-auth-smoke phase23-control-plane-audit-smoke phase24-control-plane-integrity-smoke phase25-siem-delivery-smoke phase26-siem-ha-kms-smoke phase27-siem-exactly-once-dr-smoke phase28-vault-credentials-reconcile-smoke phase29-receiver-failover-dr-smoke phase30-receiver-term-failback-dr-smoke phase31-promotion-workflow-rpo-smoke phase32-governance-smoke phase33-governance-retry-smoke phase34-governance-debt-smoke phase35-governance-remediation-smoke phase36-governance-four-eyes-smoke phase37-governance-approval-lifecycle-smoke phase38-governance-ha-smoke phase39-governance-rotation-smoke phase40-governance-key-retirement-smoke phase41-oidc-rbac-smoke phase42-mysql-ha-pitr-smoke phase43-slo-observability-smoke phase44-supply-chain-smoke phase45-multiregion-failover-smoke release-kafka-dlq-smoke release-k8s-keda-smoke release-s3-minio-smoke loadtest perf perf-multi perf-ab

tidy:
	go mod tidy

test:
	go test ./...

# The contract phase intentionally has no one-word Make target: it requires
# an explicit operator acknowledgement after all old writers are drained.
schema-status:
	go run ./cmd/schema-migrate --phase=status

schema-expand:
	go run ./cmd/schema-migrate --phase=expand

schema-workflow-status:
	go run ./cmd/schema-migrate --phase=workflow-status

schema-workflow-expand:
	go run ./cmd/schema-migrate --phase=workflow-expand

fmt-check:
	test -z "$$(gofmt -l ./cmd ./internal ./pkg)"

taskflowctl-build:
	mkdir -p ./bin
	go build -o ./bin/taskflowctl ./cmd/taskflowctl

taskflowctl-test:
	go test -count=1 ./cmd/taskflowctl ./pkg/cloudclient

vet:
	go vet ./...

gate:
	sh scripts/gate.sh

portfolio-check:
	python3 scripts/portfolio-seal-check.py

cloud-control-test:
	go vet ./internal/cloudcontrol
	go test ./internal/cloudcontrol

cloud-control-quota-smoke:
	sh scripts/cloud-control-quota-smoke.sh

cloud-control-saga-smoke:
	sh scripts/cloud-control-saga-smoke.sh

cloud-control-kind-e2e:
	sh scripts/cloud-control-kind-e2e.sh

cloud-control-kind-chaos:
	sh scripts/cloud-control-kind-chaos.sh

cloud-control-scale-benchmark:
	sh scripts/cloud-control-scale-benchmark.sh

cloud-control-scale-ab:
	sh scripts/cloud-control-scale-ab.sh

gpuflow-test:
	go vet ./internal/gpuflow ./cmd/gpuflow-server ./cmd/gpuflow-bench ./cmd/gpuflow-fairness-bench
	go test ./internal/gpuflow
	go build ./cmd/gpuflow-server ./cmd/gpuflow-bench ./cmd/gpuflow-fairness-bench

gpuflow-bench:
	go run ./cmd/gpuflow-bench -jobs 100000 -nodes 64

gpuflow-fairness-bench:
	go run ./cmd/gpuflow-fairness-bench -dispatches 1200

gpuflow-server:
	go run ./cmd/gpuflow-server

run:
	go run ./cmd/server

run-worker:
	go run ./cmd/executor-worker

up:
	docker compose up --build -d

down:
	docker compose down

smoke:
	bash scripts/smoke.sh

grpc-smoke:
	bash scripts/grpc-smoke.sh

outbox-smoke:
	bash scripts/outbox-recovery.sh

ai-smoke:
	bash scripts/ai-smoke.sh

ai-phase2-smoke:
	bash scripts/ai-phase2-smoke.sh

ai-phase3-smoke:
	bash scripts/ai-phase3-smoke.sh

ai-phase4-smoke:
	bash scripts/ai-phase4-smoke.sh

ai-phase5-smoke:
	bash scripts/ai-phase5-smoke.sh

ai-phase6-smoke:
	bash scripts/ai-phase6-smoke.sh

ai-phase7-smoke:
	bash scripts/ai-phase7-smoke.sh

ai-phase8-smoke:
	bash scripts/ai-phase8-smoke.sh

ai-phase9-smoke:
	bash scripts/ai-phase9-smoke.sh

ai-phase10-smoke:
	bash scripts/ai-phase10-smoke.sh

ai-phase11-smoke:
	bash scripts/ai-phase11-smoke.sh

phase12-autoscaling-smoke:
	bash scripts/phase12-autoscaling-smoke.sh

phase13-drain-smoke:
	bash scripts/phase13-drain-smoke.sh

phase14-availability-smoke:
	bash scripts/phase14-availability-smoke.sh

phase15-reconciler-smoke:
	bash scripts/phase15-reconciler-smoke.sh

phase16-enqueue-fencing-smoke:
	bash scripts/phase16-enqueue-fencing-smoke.sh

phase17-compaction-smoke:
	bash scripts/phase17-compaction-smoke.sh

phase18-dlq-smoke:
	bash scripts/phase18-dlq-smoke.sh

phase19-dlq-lifecycle-smoke:
	bash scripts/phase19-dlq-lifecycle-smoke.sh

phase20-dlq-archive-smoke:
	bash scripts/phase20-dlq-archive-smoke.sh

phase21-dlq-archive-object-smoke:
	bash scripts/phase21-dlq-archive-object-smoke.sh

phase22-control-plane-auth-smoke:
	bash scripts/phase22-control-plane-auth-smoke.sh

phase23-control-plane-audit-smoke:
	bash scripts/phase23-control-plane-audit-smoke.sh

phase24-control-plane-integrity-smoke:
	bash scripts/phase24-control-plane-integrity-smoke.sh

phase25-siem-delivery-smoke:
	bash scripts/phase25-siem-delivery-smoke.sh

phase26-siem-ha-kms-smoke:
	bash scripts/phase26-siem-ha-kms-smoke.sh

phase27-siem-exactly-once-dr-smoke:
	bash scripts/phase27-siem-exactly-once-dr-smoke.sh

phase28-vault-credentials-reconcile-smoke:
	bash scripts/phase28-vault-credentials-reconcile-smoke.sh

phase29-receiver-failover-dr-smoke:
	bash scripts/phase29-receiver-failover-dr-smoke.sh

phase30-receiver-term-failback-dr-smoke:
	bash scripts/phase30-receiver-term-failback-dr-smoke.sh

phase31-promotion-workflow-rpo-smoke:
	bash scripts/phase31-promotion-workflow-rpo-smoke.sh

phase32-governance-smoke:
	bash scripts/phase32-governance-smoke.sh

phase33-governance-retry-smoke:
	bash scripts/phase33-governance-retry-smoke.sh

phase34-governance-debt-smoke:
	bash scripts/phase34-governance-debt-smoke.sh

phase35-governance-remediation-smoke:
	bash scripts/phase35-governance-remediation-smoke.sh

phase36-governance-four-eyes-smoke:
	bash scripts/phase36-governance-four-eyes-smoke.sh

phase37-governance-approval-lifecycle-smoke:
	bash scripts/phase37-governance-approval-lifecycle-smoke.sh

phase38-governance-ha-smoke:
	bash scripts/phase38-governance-ha-smoke.sh

phase39-governance-rotation-smoke:
	bash scripts/phase39-governance-rotation-smoke.sh

phase40-governance-key-retirement-smoke:
	bash scripts/phase40-governance-key-retirement-smoke.sh

phase41-oidc-rbac-smoke:
	bash scripts/phase41-oidc-rbac-smoke.sh

phase42-mysql-ha-pitr-smoke:
	bash scripts/phase42-mysql-ha-pitr-smoke.sh

phase43-slo-observability-smoke:
	bash scripts/phase43-slo-observability-smoke.sh

phase44-supply-chain-smoke:
	bash scripts/phase44-supply-chain-smoke.sh

phase45-multiregion-failover-smoke:
	bash scripts/phase45-multiregion-failover-smoke.sh

release-kafka-dlq-smoke:
	bash scripts/release-kafka-dlq-smoke.sh

release-k8s-keda-smoke:
	bash scripts/release-k8s-keda-smoke.sh

release-s3-minio-smoke:
	bash scripts/release-s3-minio-smoke.sh

loadtest:
	docker run --rm --network host -i grafana/k6 run - < loadtest/tasks.js

perf:
	docker build --build-arg GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} -t taskflow-go:perf .
	bash scripts/perf-benchmark.sh

perf-multi:
	docker build --build-arg GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} -t taskflow-go:perf .
	sh scripts/perf-multi.sh

perf-ab:
	docker build --build-arg GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} -t taskflow-go:perf .
	bash scripts/perf-ab.sh
