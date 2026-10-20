#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
EXECUTOR_METRICS_URL="${EXECUTOR_METRICS_URL:-http://127.0.0.1:19093/metrics}"
TENANT="phase5-$(date +%s)-$$"
TENANT_TOKEN="${TENANT}-tokens"
TENANT_FAIL="${TENANT}-fail"
INITIAL_GLOBAL_USD="0"
INITIAL_GLOBAL_TOKENS="0"

put_budget() {
  scope="$1"
  key="$2"
  usd="$3"
  tokens="$4"
  curl -fsS -X PUT "$BASE_URL/api/v1/ai/budgets/$scope/$key" \
    -H 'Content-Type: application/json' \
    -d "{\"limit_usd\":$usd,\"limit_tokens\":$tokens}"
}

restore() {
  docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null 2>&1 || true
  put_budget global global "$INITIAL_GLOBAL_USD" "$INITIAL_GLOBAL_TOKENS" >/dev/null 2>&1 || true
  put_budget tenant "$TENANT" 0 0 >/dev/null 2>&1 || true
  put_budget tenant "$TENANT_TOKEN" 0 0 >/dev/null 2>&1 || true
  put_budget tenant "$TENANT_FAIL" 0 0 >/dev/null 2>&1 || true
}
trap restore EXIT

wait_ready() {
  i=0
  while [ "$i" -lt 40 ]; do
    if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1 && curl -fsS "$EXECUTOR_METRICS_URL" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

create_task() {
  payload_json="$1"
  response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"ai-phase5\",\"type\":\"ai_generate\",\"payload\":$payload_json}")"
  printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'
}

wait_task() {
  id="$1"
  wanted="$2"
  i=0
  while [ "$i" -lt 40 ]; do
    task="$(curl -fsS "$BASE_URL/api/v1/tasks/$id")"
    case "$task" in
      *"\"status\":\"$wanted\""*) printf '%s' "$task"; return 0 ;;
      *'"status":"failed"'*)
        if [ "$wanted" = "failed" ]; then printf '%s' "$task"; return 0; fi
        printf '%s\n' "$task" >&2
        return 1
        ;;
    esac
    i=$((i + 1))
    sleep 1
  done
  return 1
}

provider_calls() {
  curl -fsS "$EXECUTOR_METRICS_URL" | awk '/^taskflow_ai_provider_calls_total\{/ {sum += $NF} END {printf "%.0f", sum+0}'
}

wait_ready

initial_global="$(curl -fsS "$BASE_URL/api/v1/ai/budgets/global/global" 2>/dev/null || true)"
if [ -n "$initial_global" ]; then
  INITIAL_GLOBAL_USD="$(printf '%s' "$initial_global" | python3 -c 'import json,sys; print(json.load(sys.stdin)["limit_usd"])')"
  INITIAL_GLOBAL_TOKENS="$(printf '%s' "$initial_global" | python3 -c 'import json,sys; print(json.load(sys.stdin)["limit_tokens"])')"
fi

# Keep the global account disabled during the successful settlement so repeated
# smoke runs never add test spend to a long-lived global ledger.
put_budget global global 0 0 >/dev/null
put_budget tenant "$TENANT" 0.01 1000 >/dev/null

id="$(create_task "\"{\\\"prompt\\\":\\\"budget-ok\\\",\\\"tenant_id\\\":\\\"$TENANT\\\",\\\"max_cost_usd\\\":0.002}\"")"
test -n "$id"
task="$(wait_task "$id" success)"
printf '%s' "$task" | grep -q 'mock-ai:budget-ok'

tenant_after="$(curl -fsS "$BASE_URL/api/v1/ai/budgets/tenant/$TENANT")"
printf '%s' "$tenant_after" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["spent_usd"]>0 and x["spent_tokens"]>0 and x["reserved_usd"]==0 and x["reserved_tokens"]==0'

# Global admission is tested separately with a denial, which proves the global
# account participates without accumulating test spend.
put_budget tenant "$TENANT" 1 1000 >/dev/null
put_budget global global 0.0002 1000000 >/dev/null
before_calls="$(provider_calls)"
global_id="$(create_task "\"{\\\"prompt\\\":\\\"global-denied\\\",\\\"tenant_id\\\":\\\"$TENANT\\\",\\\"max_cost_usd\\\":0.002}\"")"
global_task="$(wait_task "$global_id" failed)"
printf '%s' "$global_task" | grep -q 'scope=global'
after_calls="$(provider_calls)"
test "$before_calls" = "$after_calls"
put_budget global global "$INITIAL_GLOBAL_USD" "$INITIAL_GLOBAL_TOKENS" >/dev/null

# Shrinking the tenant USD budget below the next reservation must reject the
# task before any provider call is made.
put_budget tenant "$TENANT" 0.0002 1000 >/dev/null
before_calls="$(provider_calls)"
denied_id="$(create_task "\"{\\\"prompt\\\":\\\"budget-denied\\\",\\\"tenant_id\\\":\\\"$TENANT\\\",\\\"max_cost_usd\\\":0.002}\"")"
denied_task="$(wait_task "$denied_id" failed)"
printf '%s' "$denied_task" | grep -q 'AI budget exceeded'
after_calls="$(provider_calls)"
test "$before_calls" = "$after_calls"

# Token quota is enforced independently from USD budget.
put_budget tenant "$TENANT_TOKEN" 1 10 >/dev/null
before_calls="$(provider_calls)"
token_id="$(create_task "\"{\\\"prompt\\\":\\\"token-denied\\\",\\\"tenant_id\\\":\\\"$TENANT_TOKEN\\\",\\\"max_cost_usd\\\":0.002}\"")"
token_task="$(wait_task "$token_id" failed)"
printf '%s' "$token_task" | grep -q 'kind=tokens'
after_calls="$(provider_calls)"
test "$before_calls" = "$after_calls"

# A permanent provider error must release both global and tenant reservations
# instead of leaving quota stranded.
put_budget global global 100 1000000 >/dev/null
put_budget tenant "$TENANT_FAIL" 0.01 1000 >/dev/null
global_before_fail="$(curl -fsS "$BASE_URL/api/v1/ai/budgets/global/global")"
global_spent_before="$(printf '%s' "$global_before_fail" | python3 -c 'import json,sys; print(json.load(sys.stdin)["spent_usd"])')"
AI_ENDPOINT="http://taskflow:8080/health/live" \
AI_FALLBACK_ENDPOINT="" \
docker compose --profile ai-smoke up -d --force-recreate executor-worker >/dev/null
wait_ready

fail_id="$(create_task "\"{\\\"prompt\\\":\\\"provider-fail\\\",\\\"tenant_id\\\":\\\"$TENANT_FAIL\\\",\\\"max_cost_usd\\\":0.002}\"")"
wait_task "$fail_id" failed >/dev/null
tenant_fail_after="$(curl -fsS "$BASE_URL/api/v1/ai/budgets/tenant/$TENANT_FAIL")"
printf '%s' "$tenant_fail_after" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["spent_usd"]==0 and x["spent_tokens"]==0 and x["reserved_usd"]==0 and x["reserved_tokens"]==0'
global_after_fail="$(curl -fsS "$BASE_URL/api/v1/ai/budgets/global/global")"
printf '%s' "$global_after_fail" | GLOBAL_SPENT_BEFORE="$global_spent_before" python3 -c 'import json,os,sys; x=json.load(sys.stdin); assert abs(x["spent_usd"]-float(os.environ["GLOBAL_SPENT_BEFORE"]))<1e-9 and x["reserved_usd"]==0 and x["reserved_tokens"]==0'

budget_metrics="$(curl -fsS "$BASE_URL/metrics")"
printf '%s\n' "$budget_metrics" | grep 'taskflow_ai_budget_admissions_total' | grep 'reason="usd"' | grep 'result="denied"' >/dev/null
printf '%s\n' "$budget_metrics" | grep 'taskflow_ai_budget_admissions_total' | grep 'reason="tokens"' | grep 'result="denied"' >/dev/null
printf '%s\n' "$budget_metrics" | grep '^taskflow_ai_budget_settled_usd_total ' >/dev/null
printf '%s\n' "$budget_metrics" | grep '^taskflow_ai_budget_settled_tokens_total ' >/dev/null

echo "ai phase5 durable budget + quota + admission control: ok"
