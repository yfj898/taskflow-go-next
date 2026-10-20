#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUTPUT_ROOT="${CLOUD_SAGA_SCALE_AB_OUTPUT_ROOT:-/tmp/taskflow-cloud-saga-scale-ab-$(date -u +%Y%m%dT%H%M%SZ)}"
BASELINE="$OUTPUT_ROOT/locked-baseline"
OPTIMIZED="$OUTPUT_ROOT/atomic-optimized"
mkdir -p "$BASELINE" "$OPTIMIZED"

common_run() {
  cluster="$1"
  namespace="$2"
  output="$3"
  quota_mode="$4"
  db_mode="$5"
  CLOUD_SAGA_SCALE_CLUSTER="$cluster" \
  CLOUD_SAGA_SCALE_NAMESPACE="$namespace" \
  CLOUD_SAGA_SCALE_KUBECONFIG="/tmp/${cluster}-kubeconfig" \
  CLOUD_SAGA_SCALE_OUTPUT_DIR="$output" \
  CLOUD_SAGA_SCALE_QUOTA_ADMISSION_MODE="$quota_mode" \
  CLOUD_SAGA_SCALE_DB_MODE="$db_mode" \
  CLOUD_SAGA_SCALE_ORDER_REQUESTS="${CLOUD_SAGA_SCALE_ORDER_REQUESTS:-600}" \
  CLOUD_SAGA_SCALE_ORDER_CONCURRENCIES="${CLOUD_SAGA_SCALE_ORDER_CONCURRENCIES:-16 64 128}" \
  CLOUD_SAGA_SCALE_CONTROLLER_WORKFLOWS="${CLOUD_SAGA_SCALE_CONTROLLER_WORKFLOWS:-90}" \
  CLOUD_SAGA_SCALE_CONTROLLER_REPLICAS="${CLOUD_SAGA_SCALE_CONTROLLER_REPLICAS:-1 2 4}" \
  CLOUD_SAGA_SCALE_CONTROLLER_INTERVAL_MS="${CLOUD_SAGA_SCALE_CONTROLLER_INTERVAL_MS:-20}" \
    sh scripts/cloud-control-scale-benchmark.sh
}

echo "== A/B baseline: locked quota + baseline Saga DB =="
common_run taskflow-scale-ab-a taskflow-scale-ab-a "$BASELINE" locked baseline

echo "== A/B optimized: atomic-versioned quota + optimized Saga DB =="
common_run taskflow-scale-ab-b taskflow-scale-ab-b "$OPTIMIZED" atomic_versioned optimized

python3 scripts/cloud-saga-scale-ab-report.py "$BASELINE" "$OPTIMIZED" --output-root "$OUTPUT_ROOT" | tee "$OUTPUT_ROOT/comparison.stdout"
echo "CLOUD_CONTROL_SCALE_AB_OK"
echo "results=$OUTPUT_ROOT"
