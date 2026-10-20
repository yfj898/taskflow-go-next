#!/usr/bin/env bash
# Explicit, fail-closed first push to the account owner, never a random repo.
# This script requires the owner to authenticate GitHub CLI interactively.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
TARGET="${TASKFLOW_GITHUB_REPO:-yfj898/taskflow-go}"

if [[ "$TARGET" != "yfj898/taskflow-go" ]]; then
  echo 'Refusing unexpected GitHub owner/repo; reviewed default is yfj898/taskflow-go' >&2
  exit 2
fi
for bin in git gh; do
  command -v "$bin" >/dev/null || { echo "Missing $bin" >&2; exit 2; }
done
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo 'Not a local Git checkout' >&2
  exit 2
fi
if [[ "$(git branch --show-current)" != main ]]; then
  echo 'First publish requires reviewed main branch' >&2
  exit 2
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'Commit/review all source changes before first push' >&2
  exit 2
fi
if ! gh auth status >/dev/null 2>&1; then
  echo 'GitHub CLI is not authenticated. Owner must run: gh auth login -h github.com -p https -w' >&2
  exit 2
fi
if git remote get-url origin >/dev/null 2>&1; then
  echo 'An origin remote already exists; inspect it manually before pushing' >&2
  exit 2
fi
if gh repo view "$TARGET" --json name >/dev/null 2>&1; then
  echo "Repository $TARGET already exists; refusing to overwrite or attach it automatically" >&2
  exit 2
fi

echo "Creating new PRIVATE GitHub repository $TARGET and pushing reviewed main snapshot"
gh repo create "$TARGET" --private --source "$ROOT" --remote origin --push
visibility="$(gh repo view "$TARGET" --json visibility --jq .visibility)"
if [[ "$visibility" != PRIVATE ]]; then
  echo "Unexpected GitHub repository visibility: $visibility" >&2
  exit 1
fi
echo 'PRIVATE_GITHUB_INITIAL_PUSH_VERIFIED'
echo "CI: gh run list --repo $TARGET --limit 8"
