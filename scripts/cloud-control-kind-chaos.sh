#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${CLOUD_SAGA_CHAOS_CLUSTER:-taskflow-cloud-saga-chaos}"
NAMESPACE="${CLOUD_SAGA_CHAOS_NAMESPACE:-taskflow-cloud-saga-chaos}"
TOOLS_DIR="${CLOUD_SAGA_CHAOS_TOOLS_DIR:-/tmp/taskflow-release-k8s-tools}"
KIND="$TOOLS_DIR/kind"
KUBECTL="$TOOLS_DIR/kubectl"
KUBECONFIG_FILE="${CLOUD_SAGA_CHAOS_KUBECONFIG:-/tmp/cloud-saga-chaos-kubeconfig}"
TOKEN="release-control-plane-token"
TENANT="kind-saga-tenant"
SERVICE_PORT="18180"
POD1_PORT="18181"
POD2_PORT="18182"
KEEP_CLUSTER="${CLOUD_SAGA_CHAOS_KEEP_CLUSTER:-0}"
service_pf_pid=""
pod1_pf_pid=""
pod2_pf_pid=""
request_pid=""
phase="baseline"

cleanup() {
  rc=$?
  for pid in "$request_pid" "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
    if [ -n "$pid" ]; then kill "$pid" >/dev/null 2>&1 || true; fi
  done
  if [ "$rc" -ne 0 ]; then
    echo "cloud-control Kind chaos failed during phase: $phase" >&2
    k -n "$NAMESPACE" get pods,deploy,svc,pvc 2>/dev/null >&2 || true
    k -n "$NAMESPACE" get events --sort-by=.lastTimestamp 2>/dev/null | tail -80 >&2 || true
    k -n "$NAMESPACE" logs deployment/taskflow --all-containers --tail=200 2>/dev/null >&2 || true
  fi
  if [ "$KEEP_CLUSTER" != "1" ] && [ -x "$KIND" ]; then
    "$KIND" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  return "$rc"
}
trap cleanup EXIT

k() {
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" "$@"
}

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 160 ]; do
    if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1))
    sleep 0.25
  done
  echo "timeout waiting for $url" >&2
  return 1
}

auth_curl() {
  curl -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

json_field() {
  field="$1"
  python3 -c "import json,sys; d=json.load(sys.stdin); x=d; [x:=x[p] for p in '$field'.split('.')]; print(x)"
}

workflow_status() {
  auth_curl "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/$1" | json_field workflow.status
}

drive_workflow() {
  workflow_id="$1"
  want="$2"
  i=0
  while [ "$i" -lt 240 ]; do
    status="$(workflow_status "$workflow_id")"
    if [ "$status" = "$want" ]; then return 0; fi
    code="$(curl -sS --max-time 20 -o /tmp/cloud-saga-chaos-drive.json -w '%{http_code}' \
      -X POST -H "Authorization: Bearer $TOKEN" \
      "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/$workflow_id/reconcile" || true)"
    case "$code" in
      200|409|500) ;;
      *) echo "unexpected reconcile HTTP $code" >&2; return 1 ;;
    esac
    i=$((i + 1))
    sleep 0.25
  done
  echo "workflow $workflow_id did not reach $want; last=$(workflow_status "$workflow_id")" >&2
  return 1
}

stop_forwards() {
  for pid in "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
    if [ -n "$pid" ]; then kill "$pid" >/dev/null 2>&1 || true; fi
  done
  service_pf_pid=""; pod1_pf_pid=""; pod2_pf_pid=""
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
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward service/taskflow "$SERVICE_PORT:80" >/tmp/cloud-saga-chaos-service-pf.log 2>&1 & service_pf_pid=$!
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward "pod/$pod1" "$POD1_PORT:8080" >/tmp/cloud-saga-chaos-pod1-pf.log 2>&1 & pod1_pf_pid=$!
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward "pod/$pod2" "$POD2_PORT:8080" >/tmp/cloud-saga-chaos-pod2-pf.log 2>&1 & pod2_pf_pid=$!
  wait_http "http://127.0.0.1:$SERVICE_PORT/health/ready"
  wait_http "http://127.0.0.1:$POD1_PORT/health/ready"
  wait_http "http://127.0.0.1:$POD2_PORT/health/ready"
}

wait_stable_taskflow_pods() {
  i=0
  while [ "$i" -lt 180 ]; do
    summary="$(k -n "$NAMESPACE" get pods -l app=taskflow -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
live=[]
for p in d.get("items",[]):
    if p.get("metadata",{}).get("deletionTimestamp"): continue
    ready=any(c.get("type")=="Ready" and c.get("status")=="True" for c in p.get("status",{}).get("conditions",[]))
    if ready: live.append(p["metadata"]["name"])
print(len(live), *sorted(live))
')"
    count="$(printf '%s' "$summary" | awk '{print $1}')"
    total="$(k -n "$NAMESPACE" get pods -l app=taskflow -o json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("items",[])))')"
    if [ "$count" = "2" ] && [ "$total" = "2" ]; then
      disabled=1
      for p in $(printf '%s' "$summary" | cut -d' ' -f2-); do
        if ! k -n "$NAMESPACE" logs "$p" 2>/dev/null | grep -q 'cloud-saga reconciler disabled'; then
          disabled=0
          break
        fi
      done
      if [ "$disabled" = "1" ]; then
        sleep 2
        return 0
      fi
    fi
    i=$((i + 1))
    sleep 0.5
  done
  echo "TaskFlow Pods did not stabilize with Saga controller disabled" >&2
  return 1
}

phase="validated Kind baseline"
CLOUD_SAGA_E2E_CLUSTER="$CLUSTER_NAME" \
CLOUD_SAGA_E2E_NAMESPACE="$NAMESPACE" \
CLOUD_SAGA_E2E_KUBECONFIG="$KUBECONFIG_FILE" \
CLOUD_SAGA_E2E_KEEP_CLUSTER=1 \
  sh scripts/cloud-control-kind-e2e.sh >/tmp/cloud-saga-chaos-baseline.log 2>&1

phase="configure crash-after-side-effect fault window"
k -n "$NAMESPACE" set env deployment/taskflow \
  CLOUD_SAGA_CONTROLLER_ENABLED=false \
  CLOUD_SAGA_LEASE_MS=10000 \
  CLOUD_SAGA_FAULT_INJECTION_ENABLED=true \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP=network \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_DELAY_MS=20000 \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR=false >/dev/null
k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=180s >/dev/null
wait_stable_taskflow_pods
start_forwards

phase="create crash-after-side-effect workflow"
cat >/tmp/cloud-saga-chaos-crash.json <<EOF
{
  "tenant_id":"$TENANT",
  "name":"kind-chaos-crash",
  "provider":"kubernetes",
  "spec":{
    "compute":{"namespace":"$NAMESPACE","image":"nginx:1.27-alpine","replicas":1,"cpu":"50m","memory":"64Mi","container_port":80},
    "network":{"enabled":true,"port":8091,"target_port":80},
    "storage":{"enabled":false}
  }
}
EOF
auth_curl -X POST -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-chaos-crash-001' \
  --data-binary @/tmp/cloud-saga-chaos-crash.json \
  "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/provision" >/tmp/cloud-saga-chaos-crash-response.json
crash_workflow_id="$(json_field workflow.id </tmp/cloud-saga-chaos-crash-response.json)"
crash_resource_id="$(json_field resource.id </tmp/cloud-saga-chaos-crash-response.json)"

phase="create external side effect then kill lease owner"
curl -sS --max-time 20 -o /tmp/cloud-saga-chaos-owner.body -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$POD1_PORT/api/v1/cloud/workflows/$crash_workflow_id/reconcile" \
  >/tmp/cloud-saga-chaos-owner.status & request_pid=$!
i=0
while [ "$i" -lt 80 ]; do
  if k -n "$NAMESPACE" get service kind-chaos-crash-svc >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 0.1
done
k -n "$NAMESPACE" get service kind-chaos-crash-svc >/dev/null
service_uid_before="$(k -n "$NAMESPACE" get service kind-chaos-crash-svc -o jsonpath='{.metadata.uid}')"

busy_code="$(curl -sS --max-time 3 -o /tmp/cloud-saga-chaos-busy.body -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$POD2_PORT/api/v1/cloud/workflows/$crash_workflow_id/reconcile" || true)"
echo "pre-kill contender HTTP=$busy_code" >/tmp/cloud-saga-chaos-contention.txt
test "$busy_code" = "409"

k -n "$NAMESPACE" delete pod "$pod1" --grace-period=0 --force >/dev/null 2>&1 || true
wait "$request_pid" >/dev/null 2>&1 || true
request_pid=""

phase="prove durable running state survived owner death"
db_state="$(k -n "$NAMESPACE" exec deployment/mysql -- mysql -utaskflow -ptaskflow taskflow -N -e "select concat(w.status,'|',coalesce(w.lease_until,''),'|',s.status) from cloud_workflows w join cloud_workflow_steps s on s.workflow_id=w.id and s.name='network' where w.id=$crash_workflow_id;" 2>/dev/null)"
case "$db_state" in running\|*\|running) ;; *) echo "unexpected crash state $db_state" >&2; exit 1 ;; esac

phase="wait lease expiry and let surviving replica resume idempotently"
sleep 11
resume_code="$(curl -sS --max-time 30 -o /tmp/cloud-saga-chaos-resume.body -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$POD2_PORT/api/v1/cloud/workflows/$crash_workflow_id/reconcile")"
test "$resume_code" = "200"
service_uid_after="$(k -n "$NAMESPACE" get service kind-chaos-crash-svc -o jsonpath='{.metadata.uid}')"
test "$service_uid_before" = "$service_uid_after"

stop_forwards
k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=180s >/dev/null
start_forwards
drive_workflow "$crash_workflow_id" succeeded
k -n "$NAMESPACE" rollout status deployment/kind-chaos-crash --timeout=180s >/dev/null

auth_curl -X POST -H 'Idempotency-Key: kind-chaos-crash-delete-002' \
  "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/resources/$crash_resource_id/workflows/delete" >/tmp/cloud-saga-chaos-crash-delete.json
crash_delete_id="$(json_field workflow.id </tmp/cloud-saga-chaos-crash-delete.json)"
drive_workflow "$crash_delete_id" succeeded

phase="configure synthetic outcome-unknown"
k -n "$NAMESPACE" set env deployment/taskflow \
  CLOUD_SAGA_CONTROLLER_ENABLED=false \
  CLOUD_SAGA_LEASE_MS=10000 \
  CLOUD_SAGA_FAULT_INJECTION_ENABLED=true \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_STEP=network \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_DELAY_MS=0 \
  CLOUD_SAGA_FAULT_AFTER_SUCCESS_ERROR=true >/dev/null
k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=180s >/dev/null
wait_stable_taskflow_pods
start_forwards

phase="inject error after real Kubernetes Service creation"
cat >/tmp/cloud-saga-chaos-outcome.json <<EOF
{
  "tenant_id":"$TENANT",
  "name":"kind-chaos-outcome",
  "provider":"kubernetes",
  "spec":{
    "compute":{"namespace":"$NAMESPACE","image":"nginx:1.27-alpine","replicas":1,"cpu":"50m","memory":"64Mi","container_port":80},
    "network":{"enabled":true,"port":8092,"target_port":80},
    "storage":{"enabled":false}
  }
}
EOF
auth_curl -X POST -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-chaos-outcome-003' \
  --data-binary @/tmp/cloud-saga-chaos-outcome.json \
  "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/provision" >/tmp/cloud-saga-chaos-outcome-response.json
outcome_workflow_id="$(json_field workflow.id </tmp/cloud-saga-chaos-outcome-response.json)"

outcome_code="$(curl -sS --max-time 10 -o /tmp/cloud-saga-chaos-outcome-error.body -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$POD1_PORT/api/v1/cloud/workflows/$outcome_workflow_id/reconcile" || true)"
test "$outcome_code" = "500"
k -n "$NAMESPACE" get service kind-chaos-outcome-svc >/dev/null
outcome_state="$(auth_curl "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/$outcome_workflow_id")"
test "$(printf '%s' "$outcome_state" | json_field workflow.status)" = "compensating"
printf '%s' "$outcome_state" >/tmp/cloud-saga-chaos-outcome-state.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/cloud-saga-chaos-outcome-state.json'))
steps={s['name']:s for s in d['steps']}
assert steps['network']['status'] == 'failed', steps['network']
assert 'outcome-unknown' in steps['network']['last_error'], steps['network']
PY

phase="compensate the failed step itself"
drive_workflow "$outcome_workflow_id" compensated
if k -n "$NAMESPACE" get service kind-chaos-outcome-svc >/dev/null 2>&1; then
  echo "outcome-unknown Service survived compensation" >&2
  exit 1
fi
if k -n "$NAMESPACE" get deployment kind-chaos-outcome >/dev/null 2>&1; then
  echo "compute unexpectedly existed after network failure" >&2
  exit 1
fi
final_state="$(auth_curl "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/workflows/$outcome_workflow_id")"
printf '%s' "$final_state" >/tmp/cloud-saga-chaos-outcome-final.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/cloud-saga-chaos-outcome-final.json'))
steps={s['name']:s for s in d['steps']}
assert d['workflow']['status'] == 'compensated', d['workflow']
assert d['order']['status'] == 'failed', d['order']
assert d['resource']['actual_state'] == 'deleted', d['resource']
assert steps['network']['status'] == 'compensated', steps['network']
assert steps['network']['compensation_attempt_count'] >= 1, steps['network']
PY
quota="$(auth_curl "http://127.0.0.1:$SERVICE_PORT/api/v1/cloud/tenants/$TENANT/quota")"
test "$(printf '%s' "$quota" | json_field cpu_milli_used)" = "0"
test "$(printf '%s' "$quota" | json_field memory_mib_used)" = "0"
test "$(printf '%s' "$quota" | json_field replica_used)" = "0"

phase="verify fault evidence metric"
curl -fsS "http://127.0.0.1:$POD1_PORT/metrics" >/tmp/cloud-saga-chaos-metrics.txt
python3 - <<'PY'
import re
s=open('/tmp/cloud-saga-chaos-metrics.txt').read().splitlines()
vals=[]
for line in s:
    if line.startswith('taskflow_cloud_saga_step_operations_total{') and 'direction="forward"' in line and 'kind="provision_bundle"' in line and 'outcome="error"' in line and 'step="network"' in line:
        vals.append(float(line.rsplit(' ',1)[1]))
assert vals and sum(vals) >= 1, vals
PY

phase="complete"
echo "CLOUD_CONTROL_KIND_CHAOS_OK"
echo "crash-after-side-effect: busy-before-kill=409 service_uid_preserved=$service_uid_before"
echo "lease-expiry takeover: surviving replica resumed workflow after owner pod death"
echo "outcome-unknown: external Service existed before synthetic error and failed step was compensated"
echo "final quota: cpu=0 memory=0 replicas=0"
