#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
run_dir=${1:?usage: run-fresh-main.sh RUN_DIR}
mkdir -p "$run_dir"
run_dir=$(cd "$run_dir" && pwd)
exit_file="$run_dir/run.exit"
exit_tmp="$exit_file.tmp"

finish() {
    status=$?
    printf '%s\n' "$status" >"$exit_tmp"
    mv "$exit_tmp" "$exit_file"
    exit "$status"
}
trap finish EXIT

cd "$repo_root"
test -z "$(git status --porcelain)"
git rev-parse HEAD >"$run_dir/source.sha"
rustup show active-toolchain >"$run_dir/rustup.txt"
mbx doctor >"$run_dir/mbx-doctor.txt" 2>&1 || true

task_tmp=${TMPDIR:-/home/ec2-user/.cache/sophon-task-tmp/ent3448/sdk}
mkdir -p "$task_tmp"
export TMPDIR=$task_tmp
export CARGO_BUILD_JOBS=4

mbx cargo test --quiet -p lancedb function_error_traceback --lib
mbx cargo test --quiet -p lancedb job_failure_legacy --lib
mbx cargo test --quiet -p lancedb job_failure_traceback --lib
mbx cargo check --quiet -p lancedb-python -p lancedb-nodejs

cd python
venv="$run_dir/venv"
uv venv --python 3.12 "$venv"
# shellcheck source=/dev/null
source "$venv/bin/activate"
uv pip install maturin
uv pip install -e ".[tests]"
pytest -q python/tests/test_remote_db.py \
    -k 'remote_function_errors_lists_the_rows_a_refresh_skipped or remote_connection_jobs_surface'

cd "$repo_root"
git diff --exit-code
test -z "$(git status --porcelain)"
