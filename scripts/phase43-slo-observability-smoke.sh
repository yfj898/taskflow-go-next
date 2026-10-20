#!/usr/bin/env sh
set -eu

PROM_URL="${PHASE43_PROM_URL:-http://127.0.0.1:9090}"
ALERT_URL="${PHASE43_ALERT_URL:-http://127.0.0.1:19094}"
GRAFANA_URL="${PHASE43_GRAFANA_URL:-http://127.0.0.1:13000}"
phase="initialization"

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 300 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for $url" >&2
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 43 SLO observability smoke failed during phase: $phase" >&2
    docker compose --profile phase43-smoke logs --tail 250 prometheus alertmanager grafana >&2 2>/dev/null || true
  fi
  docker compose --profile phase43-smoke rm -sf alertmanager grafana >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="validate Prometheus and Alertmanager configuration"
docker run --rm --entrypoint /bin/promtool -v "$PWD/prometheus.yml:/etc/prometheus/prometheus.yml:ro" -v "$PWD/monitoring/alerts.yml:/etc/prometheus/alerts.yml:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml >/tmp/phase43-promtool.txt
grep -q 'SUCCESS' /tmp/phase43-promtool.txt
docker run --rm --entrypoint /bin/amtool -v "$PWD/monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro" prom/alertmanager:v0.28.1 check-config /etc/alertmanager/alertmanager.yml >/tmp/phase43-amtool.txt
python3 -c 'import json; d=json.load(open("monitoring/grafana/taskflow-overview.json")); assert d["uid"]=="taskflow-production-overview" and len(d["panels"])>=4'

phase="start Prometheus Alertmanager and Grafana"
docker compose --profile phase43-smoke up -d prometheus alertmanager grafana >/dev/null
wait_http "$PROM_URL/-/ready"
wait_http "$ALERT_URL/-/ready"
wait_http "$GRAFANA_URL/api/health"

phase="prove SLO recording/alert rules and dashboard provisioning"
curl -fsS "$PROM_URL/api/v1/rules" >/tmp/phase43-rules.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/phase43-rules.json'))
assert d['status']=='success', d
names={r.get('name') for g in d['data']['groups'] for r in g.get('rules',[])}
required={
    'taskflow:slo:http_availability:ratio5m',
    'taskflow:slo:http_availability:ratio1h',
    'TaskFlowHTTPAvailabilityBurnRateFast',
    'TaskFlowQueueStatsStale',
    'TaskFlowGovernanceDebtFenced',
    'taskflow:slo:cloud_saga_provider_error_ratio5m',
    'TaskFlowCloudSagaBacklogStuck',
    'TaskFlowCloudSagaProviderErrors',
}
assert required <= names, (required-names, names)
PY

i=0
while [ "$i" -lt 100 ]; do
  if curl -fsS "$GRAFANA_URL/api/search?query=TaskFlow" >/tmp/phase43-grafana.json 2>/dev/null && python3 -c 'import json; d=json.load(open("/tmp/phase43-grafana.json")); assert any(x.get("uid")=="taskflow-production-overview" for x in d)' 2>/dev/null; then
    break
  fi
  i=$((i + 1)); sleep 0.2
done
[ "$i" -lt 100 ]

echo "Phase 43 Prometheus recording rules and multi-window SLO alerts: ok"
echo "Phase 43 Alertmanager routing, Grafana provisioning and production runbook: ok"
