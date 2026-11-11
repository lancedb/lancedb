#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="${1:?usage: verify_python.sh OUTPUT_DIR}"
venv="${2:?usage: verify_python.sh OUTPUT_DIR VENV}"
mkdir -p "$out_dir" "$(dirname "$venv")"

cd "$repo_root"
git rev-parse HEAD >"$out_dir/source-head.txt"
git status --porcelain=v1 >"$out_dir/source-status-before.txt"
sha256sum "${BASH_SOURCE[0]}" >"$out_dir/runner-sha256.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/started-at.txt"

if [[ -s "$out_dir/source-status-before.txt" ]]; then
  printf 'source checkout is dirty\n' >"$out_dir/runner.error"
  exit 2
fi

uv venv --clear --python 3.11 "$venv"
UV_PROJECT_ENVIRONMENT="$venv" uv sync \
  --project "$repo_root/python" \
  --locked \
  --extra tests \
  --extra dev \
  --no-install-project

source "$venv/bin/activate"
export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=/home/ec2-user/Code/targets/ent3563-freshness-bb527bb

uvx --from 'maturin>=1.10' maturin develop \
  --manifest-path "$repo_root/python/Cargo.toml" \
  --locked >"$out_dir/python-build.log" 2>&1
printf '%s\n' "$?" >"$out_dir/python-build.log.exit"

python -c 'import lancedb._lancedb as native; print(native.__file__)' \
  >"$out_dir/extension-path.txt"
sha256sum "$(cat "$out_dir/extension-path.txt")" \
  >"$out_dir/extension-sha256.txt" 2>/dev/null || true

set +e
pytest -q \
  python/python/tests/test_remote_db.py \
  -k 'test_remote_function_column_status_passes_pinned_version or test_remote_wait_fresh_retries_only_typed_finalization_pending' \
  >"$out_dir/pytest.log" 2>&1
status=$?
set -e
if [[ "$status" -eq 0 ]] && ! grep -Fq "2 passed" "$out_dir/pytest.log"; then
  printf '%s\n' "expected two Python tests to pass" >"$out_dir/selection-error.txt"
  status=1
fi
printf '%s\n' "$status" >"$out_dir/pytest.log.exit"

git status --porcelain=v1 >"$out_dir/source-status-after.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/finished-at.txt"
printf '%s\n' "$status" >"$out_dir/runner.exit"
exit "$status"
