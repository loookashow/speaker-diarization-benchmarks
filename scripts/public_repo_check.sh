#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[check] scanning for machine-specific absolute paths and obvious secret leaks..."

EXCLUDES=(
  -g '!**/data/venvs/**'
  -g '!.ruff_cache/**'
  -g '!.idea/**'
  -g '!**/__pycache__/**'
  -g '!**/*.pyc'
  -g '!scripts/public_repo_check.sh'
)

PATH_PATTERN='/Users/|/home/|C:\\Users\\|records/benchmark'
SECRET_PATTERN='BEGIN [A-Z ]*PRIVATE KEY|MIGH[A-Za-z0-9+/=]{20,}|FOXNOSE_SECRET_KEY=[A-Za-z0-9+/=]{40,}|BENCH_API_KEYS=[A-Za-z0-9]{32,}'
INTERNAL_PATTERN='http://127\.0\.0\.1|localhost:8080|/app/data/work'

PATH_HITS="$(rg -n "$PATH_PATTERN" . "${EXCLUDES[@]}" || true)"
SECRET_HITS="$(rg -n "$SECRET_PATTERN" . "${EXCLUDES[@]}" || true)"
INTERNAL_HITS="$(rg -n "$INTERNAL_PATTERN" benchmarks "${EXCLUDES[@]}" || true)"

if [[ -n "$PATH_HITS" ]]; then
  echo
  echo "[fail] found machine-specific paths:"
  echo "$PATH_HITS"
  exit 1
fi

if [[ -n "$SECRET_HITS" ]]; then
  echo
  echo "[fail] found potential secrets in plain text:"
  echo "$SECRET_HITS"
  exit 1
fi

if [[ -n "$INTERNAL_HITS" ]]; then
  echo
  echo "[fail] found internal/private runtime markers:"
  echo "$INTERNAL_HITS"
  exit 1
fi

echo "[ok] no obvious local paths or secrets found."
