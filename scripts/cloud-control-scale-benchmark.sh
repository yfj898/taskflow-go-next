#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

SCALE_LOCK_FILE="${CLOUD_SAGA_SCALE_LOCK_FILE:-/tmp/taskflow-cloud-saga-scale-benchmark.lock}"
exec 9>"$SCALE_LOCK_FILE"
if ! flock -n 9; then
  echo "another cloud-control scale benchmark is already running: $SCALE_LOCK_FILE" >&2
  exit 1
fi

CLUSTER_NAME="${CLOUD_SAGA_SCALE_CLUSTER:-taskflow-cloud-saga-scale}"
NAMESPACE="${CLOUD_SAGA_SCALE_NAMESPACE:-taskflow-cloud-saga-scale}"
KUBECONFIG_FILE="${CLOUD_SAGA_SCALE_KUBECONFIG:-/tmp/cloud-saga-scale-kubeconfig}"
TOOLS_DIR="${CLOUD_SAGA_SCALE_TOOLS_DIR:-/tmp/taskflow-release-k8s-tools}"
KUBECTL="$TOOLS_DIR/kubectl"
KIND="$TOOLS_DIR/kind"
TOKEN="release-control-plane-token"
OUTPUT_DIR="${CLOUD_SAGA_SCALE_OUTPUT_DIR:-/tmp/taskflow-cloud-saga-scale-$(date -u +%Y%m%dT%H%M%SZ)}"
ORDER_REQUESTS="${CLOUD_SAGA_SCALE_ORDER_REQUESTS:-600}"
ORDER_CONCURRENCIES="${CLOUD_SAGA_SCALE_ORDER_CONCURRENCIES:-16 64 128}"
SHARD_TENANTS="${CLOUD_SAGA_SCALE_SHARD_TENANTS:-32}"
CONTROLLER_WORKFLOWS="${CLOUD_SAGA_SCALE_CONTROLLER_WORKFLOWS:-90}"
CONTROLLER_REPLICAS="${CLOUD_SAGA_SCALE_CONTROLLER_REPLICAS:-1 2 4}"
CONTROLLER_INTERVAL_MS="${CLOUD_SAGA_SCALE_CONTROLLER_INTERVAL_MS:-20}"
QUOTA_ADMISSION_MODES="${CLOUD_SAGA_SCALE_QUOTA_ADMISSION_MODES:-locked atomic_versioned bucketed}"
QUOTA_BUCKETS="${CLOUD_SAGA_SCALE_QUOTA_BUCKETS:-16}"
SAGA_DB_MODES="${CLOUD_SAGA_SCALE_DB_MODES:-legacy baseline optimized}"
SAGA_OPTIMIZED_WORKERS="${CLOUD_SAGA_SCALE_OPTIMIZED_WORKERS:-1 8}"
SAGA_OPTIMIZED_BACKOFFS="${CLOUD_SAGA_SCALE_OPTIMIZED_BACKOFFS:-0 100}"
QUOTA_ADMISSION_MODE=locked
SAGA_DB_MODE=baseline
SAGA_WORKER_CONCURRENCY=1
SAGA_OBSERVE_BACKOFF_MS=0
SAGA_OBSERVE_BACKOFF_MAX_MS="${CLOUD_SAGA_SCALE_OBSERVE_BACKOFF_MAX_MS:-2000}"
KEEP_CLUSTER="${CLOUD_SAGA_SCALE_KEEP_CLUSTER:-0}"
SERVICE_PORT="18280"
POD1_PORT="18281"
POD2_PORT="18282"
service_pf_pid=""
pod1_pf_pid=""
pod2_pf_pid=""
phase="initialization"

mkdir -p "$OUTPUT_DIR"

k() {
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" "$@"
}

cleanup() {
  rc=$?
  for pid in "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
    if [ -n "$pid" ]; then kill "$pid" >/dev/null 2>&1 || true; fi
  done
  if [ "$rc" -ne 0 ]; then
    echo "cloud Saga scale benchmark failed during phase: $phase" >&2
    k -n "$NAMESPACE" get pods,deploy,svc,pvc 2>/dev/null >&2 || true
    k -n "$NAMESPACE" logs deployment/taskflow --all-containers --tail=160 2>/dev/null >&2 || true
  fi
  if [ "$KEEP_CLUSTER" != "1" ] && [ -x "$KIND" ]; then
    "$KIND" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  return "$rc"
}
trap cleanup EXIT

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 160 ]; do
    if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.25
  done
  echo "timeout waiting for $url" >&2
  return 1
}

stop_forwards() {
  for pid in "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
    if [ -n "$pid" ]; then kill "$pid" >/dev/null 2>&1 || true; fi
  done
  service_pf_pid=""; pod1_pf_pid=""; pod2_pf_pid=""
  for port in "$SERVICE_PORT" "$POD1_PORT" "$POD2_PORT"; do
    fuser -k "${port}/tcp" >/dev/null 2>&1 || true
  done
}

start_forwards() {
  stop_forwards
  pods="$(k -n "$NAMESPACE" get pods -l app=taskflow -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in sorted(d.get("items",[]), key=lambda x:x["metadata"]["name"]):
    if p.get("metadata",{}).get("deletionTimestamp"): continue
    ready=any(c.get("type")=="Ready" and c.get("status")=="True" for c in p.get("status",{}).get("conditions",[]))
    if ready: print(p["metadata"]["name"])
')"
  pod1="$(printf '%s\n' "$pods" | sed -n '1p')"
  pod2="$(printf '%s\n' "$pods" | sed -n '2p')"
  test -n "$pod1"; test -n "$pod2"
  k -n "$NAMESPACE" port-forward service/taskflow "$SERVICE_PORT:80" >/tmp/cloud-saga-scale-service-pf.log 2>&1 & service_pf_pid=$!
  k -n "$NAMESPACE" port-forward "pod/$pod1" "$POD1_PORT:8080" >/tmp/cloud-saga-scale-pod1-pf.log 2>&1 & pod1_pf_pid=$!
  k -n "$NAMESPACE" port-forward "pod/$pod2" "$POD2_PORT:8080" >/tmp/cloud-saga-scale-pod2-pf.log 2>&1 & pod2_pf_pid=$!
  wait_http "http://127.0.0.1:$SERVICE_PORT/health/ready"
  wait_http "http://127.0.0.1:$POD1_PORT/health/ready"
  wait_http "http://127.0.0.1:$POD2_PORT/health/ready"
}

mysql_exec() {
  k -n "$NAMESPACE" exec deployment/mysql -- mysql -utaskflow -ptaskflow taskflow -N -e "$1" 2>/dev/null
}

reset_benchmark_state() {
  mysql_exec "SET FOREIGN_KEY_CHECKS=0; DELETE FROM cloud_workflow_steps; DELETE FROM cloud_workflows; DELETE FROM cloud_orders; DELETE FROM cloud_resources; DELETE FROM cloud_tenant_quota_buckets; DELETE FROM cloud_tenant_quotas; SET FOREIGN_KEY_CHECKS=1;" >/dev/null
  k -n "$NAMESPACE" delete deployment,service,pvc -l app.kubernetes.io/managed-by=taskflow-cloud-saga --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
}

row_lock_status() {
  mysql_exec "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_row_lock_time_max');"
}

put_quota() {
  tenant="$1"
  curl -fsS -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"cpu_milli_limit":2000000,"memory_mib_limit":2000000,"replica_limit":2000000}' \
    "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/tenants/$tenant/quota" >/dev/null
}

disable_controllers_two_api_pods() {
  stop_forwards
  k -n "$NAMESPACE" set env deployment/taskflow \
    CLOUD_SAGA_CONTROLLER_ENABLED=false \
    CLOUD_SAGA_RECONCILE_INTERVAL_MS="$CONTROLLER_INTERVAL_MS" \
    CLOUD_SAGA_FAULT_INJECTION_ENABLED=false \
    CLOUD_QUOTA_ADMISSION_MODE="$QUOTA_ADMISSION_MODE" \
    CLOUD_QUOTA_BUCKETS="$QUOTA_BUCKETS" \
    CLOUD_SAGA_DB_MODE="$SAGA_DB_MODE" \
    CLOUD_SAGA_WORKER_CONCURRENCY="$SAGA_WORKER_CONCURRENCY" \
    CLOUD_SAGA_OBSERVE_BACKOFF_MS="$SAGA_OBSERVE_BACKOFF_MS" \
    CLOUD_SAGA_OBSERVE_BACKOFF_MAX_MS="$SAGA_OBSERVE_BACKOFF_MAX_MS" >/dev/null
  k -n "$NAMESPACE" scale deployment/taskflow --replicas=2 >/dev/null
  k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=240s >/dev/null
  start_forwards
}

phase="validated real Kind baseline"
CLOUD_SAGA_E2E_CLUSTER="$CLUSTER_NAME" \
CLOUD_SAGA_E2E_NAMESPACE="$NAMESPACE" \
CLOUD_SAGA_E2E_KUBECONFIG="$KUBECONFIG_FILE" \
CLOUD_SAGA_E2E_LOCK_FILE="/tmp/${CLUSTER_NAME}.e2e.lock" \
CLOUD_SAGA_E2E_KEEP_CLUSTER=1 \
CLOUD_SAGA_E2E_IMAGE=taskflow-go:cloud-saga-scale \
  sh scripts/cloud-control-kind-e2e.sh >"$OUTPUT_DIR/e2e-baseline.log" 2>&1

{
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cluster=$CLUSTER_NAME"
  echo "namespace=$NAMESPACE"
  echo "order_requests=$ORDER_REQUESTS"
  echo "order_concurrencies=$ORDER_CONCURRENCIES"
  echo "shard_tenants=$SHARD_TENANTS"
  echo "controller_workflows=$CONTROLLER_WORKFLOWS"
  echo "controller_replicas=$CONTROLLER_REPLICAS"
  echo "controller_interval_ms=$CONTROLLER_INTERVAL_MS"
  echo "quota_admission_modes=$QUOTA_ADMISSION_MODES"
  echo "quota_buckets=$QUOTA_BUCKETS"
  echo "saga_db_modes=$SAGA_DB_MODES"
  echo "saga_optimized_workers=$SAGA_OPTIMIZED_WORKERS"
  echo "saga_optimized_backoffs_ms=$SAGA_OPTIMIZED_BACKOFFS"
  echo "saga_observe_backoff_max_ms=$SAGA_OBSERVE_BACKOFF_MAX_MS"
  echo "logical_cpus=$(nproc 2>/dev/null || echo unknown)"
  echo "taskflow_image_id=$(docker image inspect taskflow-go:cloud-saga-scale --format '{{.Id}}' 2>/dev/null || true)"
  echo "kind_server=$($KUBECTL --kubeconfig "$KUBECONFIG_FILE" version -o json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("serverVersion",{}).get("gitVersion","unknown"))' 2>/dev/null || echo unknown)"
  echo "mysql=$(mysql_exec 'SELECT VERSION();' 2>/dev/null || true)"
  echo "--- lscpu ---"
  lscpu 2>/dev/null || true
} >"$OUTPUT_DIR/environment.txt"

phase="prepare two API replicas with controllers disabled"
disable_controllers_two_api_pods

phase="high-concurrency order admission and hot-tenant quota contention"
for QUOTA_ADMISSION_MODE in $QUOTA_ADMISSION_MODES; do
  disable_controllers_two_api_pods
  quota_mode_name="$(printf '%s' "$QUOTA_ADMISSION_MODE" | tr '_' '-')"
  for mode in hot sharded; do
    for concurrency in $ORDER_CONCURRENCIES; do
      reset_benchmark_state
      if [ "$mode" = "hot" ]; then
        put_quota "scale-hot"
        tenant_prefix="scale-hot"
        tenant_count=1
      else
        tenant_prefix="scale-shard"
        tenant_count="$SHARD_TENANTS"
        i=0
        while [ "$i" -lt "$SHARD_TENANTS" ]; do
          put_quota "scale-shard-$(printf '%02d' "$i")"
          i=$((i + 1))
        done
      fi
      result="$OUTPUT_DIR/admission-${QUOTA_ADMISSION_MODE}-${mode}-c${concurrency}.json"
      if [ -f "$result" ] && [ -f "${result%.json}.locks.before" ] && [ -f "${result%.json}.locks.after" ]; then
        echo "skip existing admission result: $(basename "$result")"
        continue
      fi
      row_lock_status >"${result%.json}.locks.before"
      python3 scripts/cloud-saga-order-load.py \
        --endpoints "127.0.0.1:$POD1_PORT,127.0.0.1:$POD2_PORT" \
        --token "$TOKEN" \
        --requests "$ORDER_REQUESTS" \
        --concurrency "$concurrency" \
        --tenant-mode "$mode" \
        --tenant-prefix "$tenant_prefix" \
        --tenant-count "$tenant_count" \
        --name-prefix "ord-${quota_mode_name}-${mode}-c${concurrency}" \
        --namespace "$NAMESPACE" \
        --replicas 1 --cpu 1m --memory 1Mi \
        --output "$result" >/dev/null
      row_lock_status >"${result%.json}.locks.after"
      python3 - "$result" "$QUOTA_ADMISSION_MODE" <<'PY'
import json,sys
p,mode=sys.argv[1:]
d=json.load(open(p)); d['quota_mode']=mode
open(p,'w').write(json.dumps(d,indent=2)+'\n')
PY
    done
  done
done

controller_result() {
  db_mode="$1"
  workers="$2"
  replicas="$3"
  backoff_ms="$4"
  start_sql="$5"
  output="$6"
  stem="${output%.json}"
  lat_file="${stem}.latencies"
  attempts_file="${stem}.attempts"
  mysql_exec "SELECT ROUND(TIMESTAMPDIFF(MICROSECOND, '$start_sql', completed_at)/1000000,6) FROM cloud_workflows WHERE status='succeeded' ORDER BY id;" >"$lat_file"
  mysql_exec "SELECT name,SUM(attempt_count) FROM cloud_workflow_steps GROUP BY name ORDER BY name;" >"$attempts_file"
  deploy_count="$(k -n "$NAMESPACE" get deployment -l app.kubernetes.io/managed-by=taskflow-cloud-saga -o json | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("items",[])))')"
  test "$deploy_count" = "$CONTROLLER_WORKFLOWS"
  python3 - "$replicas" "$CONTROLLER_WORKFLOWS" "$lat_file" "$attempts_file" "$deploy_count" "$CONTROLLER_INTERVAL_MS" "$output" "$db_mode" "$workers" "$backoff_ms" <<'PY'
import json, math, statistics, sys
replicas=int(sys.argv[1]); expected=int(sys.argv[2]); path=sys.argv[3]; attempts_path=sys.argv[4]; deploy_count=int(sys.argv[5]); interval_ms=int(sys.argv[6]); output=sys.argv[7]
vals=sorted(float(x.strip()) for x in open(path) if x.strip())
if len(vals) != expected:
    raise SystemExit(f"completion count {len(vals)} != expected {expected}")
attempts={}
for line in open(attempts_path):
    name,value=line.rstrip('\n').split('\t')
    attempts[name]=int(value)
def pct(p):
    return vals[min(len(vals)-1, max(0, math.ceil(p*len(vals))-1))]
wall=max(vals)
data={
  "schema_version":1,
  "db_mode":sys.argv[8],
  "worker_concurrency":int(sys.argv[9]),
  "observe_backoff_ms":int(sys.argv[10]),
  "controller_replicas":replicas,
  "controller_interval_ms":interval_ms,
  "workflows":expected,
  "wall_s":round(wall,6),
  "throughput_workflows_s":round(expected/wall,6),
  "completion_latency_s":{
    "avg":round(statistics.fmean(vals),6),
    "p50":round(pct(.50),6),
    "p95":round(pct(.95),6),
    "p99":round(pct(.99),6),
    "max":round(max(vals),6),
  },
  "step_attempts":attempts,
  "compute_attempts_per_workflow":round(attempts.get("compute",0)/expected,6),
  "kubernetes_deployments":deploy_count,
}
open(output,"w").write(json.dumps(data,indent=2)+"\n")
print(json.dumps(data,sort_keys=True))
PY
}

phase="controller replica scaling"
stop_forwards
for SAGA_DB_MODE in $SAGA_DB_MODES; do
  QUOTA_ADMISSION_MODE=atomic_versioned
  workers_list="1"
  if [ "$SAGA_DB_MODE" = "optimized" ]; then
    workers_list="$SAGA_OPTIMIZED_WORKERS"
  fi
  for SAGA_WORKER_CONCURRENCY in $workers_list; do
   backoff_list="0"
   if [ "$SAGA_DB_MODE" = "optimized" ] && [ "$SAGA_WORKER_CONCURRENCY" = "8" ]; then
     backoff_list="$SAGA_OPTIMIZED_BACKOFFS"
   fi
   for SAGA_OBSERVE_BACKOFF_MS in $backoff_list; do
    backoff_suffix=""
    if [ "$SAGA_OBSERVE_BACKOFF_MS" != "0" ]; then
      backoff_suffix="-b${SAGA_OBSERVE_BACKOFF_MS}"
    fi
   for replicas in $CONTROLLER_REPLICAS; do
    controller_output="$OUTPUT_DIR/controller-${SAGA_DB_MODE}-w${SAGA_WORKER_CONCURRENCY}${backoff_suffix}-${replicas}.json"
    admission_output="${controller_output%.json}-admission.json"
    case_prefix="ctrl-${SAGA_DB_MODE}-w${SAGA_WORKER_CONCURRENCY}${backoff_suffix}-r${replicas}"
    if [ -f "$controller_output" ]; then
      echo "skip existing controller result: $(basename "$controller_output")"
      continue
    fi
    disable_controllers_two_api_pods
    reset_benchmark_state
    i=0
    while [ "$i" -lt 16 ]; do
      put_quota "${case_prefix}-$(printf '%02d' "$i")"
      i=$((i + 1))
    done
    python3 scripts/cloud-saga-order-load.py \
      --endpoints "127.0.0.1:$POD1_PORT,127.0.0.1:$POD2_PORT" \
      --token "$TOKEN" \
      --requests "$CONTROLLER_WORKFLOWS" \
      --concurrency 32 \
      --tenant-mode sharded \
      --tenant-prefix "$case_prefix" \
      --tenant-count 16 \
      --name-prefix "$case_prefix" \
      --namespace "$NAMESPACE" \
      --replicas 0 --cpu '' --memory '' \
      --output "$admission_output" >/dev/null

  accepted="$(python3 - "$admission_output" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
print(d.get('accepted',0))
PY
)"
    test "$accepted" = "$CONTROLLER_WORKFLOWS"

    mysql_exec "UPDATE cloud_workflows SET lease_until=DATE_ADD(NOW(6), INTERVAL 1 HOUR) WHERE status='pending';" >/dev/null
    stop_forwards
    k -n "$NAMESPACE" set env deployment/taskflow \
      CLOUD_SAGA_CONTROLLER_ENABLED=true \
      CLOUD_SAGA_RECONCILE_INTERVAL_MS="$CONTROLLER_INTERVAL_MS" \
      CLOUD_SAGA_RECONCILE_BATCH=50 \
      CLOUD_QUOTA_ADMISSION_MODE=atomic_versioned \
      CLOUD_QUOTA_BUCKETS="$QUOTA_BUCKETS" \
      CLOUD_SAGA_DB_MODE="$SAGA_DB_MODE" \
      CLOUD_SAGA_WORKER_CONCURRENCY="$SAGA_WORKER_CONCURRENCY" \
      CLOUD_SAGA_OBSERVE_BACKOFF_MS="$SAGA_OBSERVE_BACKOFF_MS" \
      CLOUD_SAGA_OBSERVE_BACKOFF_MAX_MS="$SAGA_OBSERVE_BACKOFF_MAX_MS" >/dev/null
    k -n "$NAMESPACE" scale deployment/taskflow --replicas="$replicas" >/dev/null
    k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=240s >/dev/null
    pending="$(mysql_exec "SELECT COUNT(*) FROM cloud_workflows WHERE status='pending';")"
    succeeded="$(mysql_exec "SELECT COUNT(*) FROM cloud_workflows WHERE status='succeeded';")"
    test "$pending" = "$CONTROLLER_WORKFLOWS"
    test "$succeeded" = "0"

    start_sql="$(mysql_exec "SET @benchmark_start=NOW(6); UPDATE cloud_workflows SET lease_until=NULL, next_reconcile_at=NULL WHERE status='pending'; SELECT DATE_FORMAT(@benchmark_start,'%Y-%m-%d %H:%i:%s.%f');" | tail -1)"
    i=0
    while [ "$i" -lt 1200 ]; do
      succeeded="$(mysql_exec "SELECT COUNT(*) FROM cloud_workflows WHERE status='succeeded';")"
      if [ "$succeeded" = "$CONTROLLER_WORKFLOWS" ]; then break; fi
      i=$((i + 1)); sleep 0.25
    done
    test "$succeeded" = "$CONTROLLER_WORKFLOWS"
    failed="$(mysql_exec "SELECT COUNT(*) FROM cloud_workflows WHERE status IN ('compensated');")"
    test "$failed" = "0"
    controller_result "$SAGA_DB_MODE" "$SAGA_WORKER_CONCURRENCY" "$replicas" "$SAGA_OBSERVE_BACKOFF_MS" "$start_sql" "$controller_output" >/dev/null
   done
   done
  done
done

phase="build benchmark report"
python3 scripts/cloud-saga-scale-report.py "$OUTPUT_DIR" | tee "$OUTPUT_DIR/summary.stdout"

phase="complete"
echo "CLOUD_CONTROL_SCALE_BENCHMARK_OK"
echo "results=$OUTPUT_DIR"
