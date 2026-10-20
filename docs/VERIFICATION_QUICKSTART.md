# 从独立 clone 提交一轮可信验证

需要 Linux、Git、Docker Engine/Compose、Python 3、OpenSSL、Go 1.26+、Node.js 20.19+ 或 22.12+、npm，以及首次下载依赖的网络。Windows 可在自己的 WSL 环境执行；SDK/CLI 也支持 Windows。

## 1. 克隆并构建

```sh
git clone https://github.com/yfj898/taskflow-go-next.git
git clone https://github.com/yfj898/change-workspace-demo.git
cd taskflow-go-next
export TF_ROOT="$PWD"
export DEMO_ROOT="$(cd ../change-workspace-demo && pwd)"
mkdir -p bin tmp/verification/work tmp/verification/reports tmp/verification/go-build tmp/verification/npm
go mod download
go build -o bin/taskflow-server ./cmd/server
go build -o bin/taskflowctl ./cmd/taskflowctl
go build -o bin/schema-migrate ./cmd/schema-migrate
```

## 2. 独立密码与数据库

下面的初始化块只在全新目录执行一次。有 `.env` 或数据卷时使用原配套凭据，**不要重新生成覆盖**。

```sh
if test -e examples/verification/.env; then
  echo 'Existing environment retained; skip password generation.'
else
  (umask 077; printf 'TF_MYSQL_PASSWORD=%s\nTF_MYSQL_ROOT_PASSWORD=%s\nCONTROL_PLANE_BEARER_TOKEN=%s\n' \
    "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" \
    > examples/verification/.env)
fi
set -a
. ./examples/verification/.env
set +a
docker compose -f examples/verification/compose.yaml --env-file examples/verification/.env up -d --wait
export MYSQL_DSN="taskflow_lab:${TF_MYSQL_PASSWORD}@tcp(127.0.0.1:23316)/taskflow_verification?charset=utf8mb4&parseTime=True&loc=Local"
export TASKFLOW_SCHEMA_DSN="$MYSQL_DSN"
./bin/schema-migrate --phase=verification-expand
```

Compose 只创建独立 MySQL/Redis。23316/26389 仅绑定 loopback；有冲突时调整本份配置及对应环境变量，不停止别的服务。

## 3. 准备依赖，注册固定检查

只注册可信代码。检查进程不接收云账号、SSH agent、Docker socket 或开发者 HOME；当前 runner 不是不可信多租户代码沙箱。

```sh
export DEMO_MOD_CACHE="$TF_ROOT/tmp/verification/go-mod"
(cd "$DEMO_ROOT" && GOMODCACHE="$DEMO_MOD_CACHE" go mod download)
npm ci --prefix "$DEMO_ROOT/frontend" --cache "$TF_ROOT/tmp/verification/npm" --no-fund --no-audit
python3 - <<'PY'
import json, os
from pathlib import Path
root, demo = Path(os.environ['TF_ROOT']), Path(os.environ['DEMO_ROOT'])
spec = json.loads((demo / 'verification/verify-change.json').read_text())
checks = [{'id': c['id'], 'required': c['required'], 'argv': c['argv'], 'directory': c['working_dir']} for c in spec['checks']]
config = {
    'work_root': str(root / 'tmp/verification/work'),
    'artifact_root': str(root / 'tmp/verification/reports'),
    'projects': [{
        'id': 'change-workspace-demo', 'repository': str(demo),
        'subjects': ['weknora-demo-service'],
        'profile': {'id': 'verify-change', 'revision': '1', 'checks': checks},
        'environment': {
            'PATH': os.environ['PATH'], 'GOTOOLCHAIN': 'local', 'GOWORK': 'off',
            'GOFLAGS': '-mod=readonly', 'GOPROXY': 'off',
            'GOMODCACHE': os.environ['DEMO_MOD_CACHE'],
            'GOCACHE': str(root / 'tmp/verification/go-build'),
            'DEMO_NPM_CACHE': str(root / 'tmp/verification/npm')
        }
    }]
}
(root / 'tmp/verification/projects.json').write_text(json.dumps(config, indent=2) + '\n')
PY
export TASKFLOW_VERIFICATION_CONFIG="$TF_ROOT/tmp/verification/projects.json"
export HTTP_ADDR=127.0.0.1:18089 PPROF_ADDR=127.0.0.1:16069 REDIS_ADDR=127.0.0.1:26389
export CONTROL_PLANE_AUTH_ENABLED=true CONTROL_PLANE_AUTH_MODE=bearer
export CONTROL_PLANE_OPERATOR_ID=weknora-demo-service EXECUTION_BACKEND=local WORKER_COUNT=1
./bin/taskflow-server
```

PATH 必须能找到真实 Linux Go/npm，不能复制 Windows node_modules。保留服务前台运行，另开终端提交。

## 4. 提交并保留 UUID

第二个终端进入本仓库，加载已有凭据。目标是固定修复 commit，不依赖今后 main 指向哪里。

```sh
set -a; . ./examples/verification/.env; set +a
export TASKFLOW_ENDPOINT=http://127.0.0.1:18089
export TASKFLOW_ACCESS_TOKEN="$CONTROL_PLANE_BEARER_TOKEN"
curl --fail http://127.0.0.1:18089/health/ready
python3 - <<'PY'
import json, uuid
from pathlib import Path
p = Path('tmp/verification/request.json')
if p.exists():
    raise SystemExit('Request exists: reuse its UUID, or choose another file for an intentional new run.')
p.write_text(json.dumps({
  'run_id': str(uuid.uuid4()), 'project_id': 'change-workspace-demo',
  'target_commit': '1750bd795b9a002fb4b0c2aa6a64946070796aa1',
  'check_profile_id': 'verify-change', 'check_profile_revision': '1',
  'check_ids': ['backend-test', 'frontend-test', 'frontend-build'],
  'external_snapshot_id': 'standalone-verification'
}, indent=2) + '\n')
PY
./bin/taskflowctl verification-submit tmp/verification/request.json
RUN_ID=$(python3 -c "import json; print(json.load(open('tmp/verification/request.json'))['run_id'])")
./bin/taskflowctl verification-get "$RUN_ID"
```

首次 202 只代表受理。等待后再次查询；`status=success` 且当前 generation 三个必需检查通过才是成功。响应未知时保留原文件重交/查询。

要复演故障，另建请求文件和 UUID，目标改为 `7aca2e8ec80fd4796f7ff8780960bab615340bb5`；不要修改旧 UUID 的 commit。历史失败保留，修复提交新建 run。

## 接入 ChangePilot

按 [ChangePilot 说明](https://github.com/yfj898/change-pilot) 从本地源码启动工作台。其 `config/change-workspace.example.yaml` 必须映射实际本地业务仓库、**实际工作空间 tenant ID** 及同一项目/profile；服务进程用 `TASKFLOW_ACCESS_TOKEN` 取得本环境令牌。前端、业务包与请求不携带此服务令牌。

`weknora-demo-service` 是保留的内部主体 ID。ChangePilot 展示品牌不要求修改 module、API、数据库名或鉴权键。

## 停止与证据范围

确认无正在执行的 run 后 Ctrl+C 正常停止服务，再运行：

```sh
docker compose -f examples/verification/compose.yaml --env-file examples/verification/.env down
```

保留卷、凭据与报告。需要全新环境应选新目录和卷，不删除现有业务数据。

指南依据已完成的隔离 Linux 联调配置整理；新增独立 Compose/示例做结构检查，没有为发布重建本机原环境。已实际执行的 run 和恢复见 [evidence](evidence/README.md)，不把新 clone 操作说明当成再次完整联调的结果。
