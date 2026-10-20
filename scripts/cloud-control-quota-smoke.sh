#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "== cloud-control quota: start real MySQL =="
docker compose up -d mysql >/dev/null

i=0
until docker compose exec -T mysql mysqladmin ping -h 127.0.0.1 -proot --silent >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "mysql did not become ready" >&2
    exit 1
  fi
  sleep 1
done

DSN='taskflow:taskflow@tcp(127.0.0.1:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true'

echo "== cloud-control quota: transactional integration test =="
if command -v go >/dev/null 2>&1; then
  TASKFLOW_MYSQL_INTEGRATION_DSN="$DSN" go test ./internal/repository -run '^TestCloudQuotaIntegration$' -count=1 -v
else
  docker run --rm --network host \
    -e GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" \
    -e TASKFLOW_MYSQL_INTEGRATION_DSN="$DSN" \
    -v "$ROOT_DIR":/src:ro \
    -v taskflow-go-modcache-cn:/go/pkg/mod \
    -v taskflow-go-buildcache-cn:/root/.cache/go-build \
    -w /src \
    golang:1.26.8-alpine \
    sh -lc '/usr/local/go/bin/go test ./internal/repository -run "^TestCloudQuotaIntegration$" -count=1 -v'
fi

echo "CLOUD_CONTROL_QUOTA_SMOKE_OK"
