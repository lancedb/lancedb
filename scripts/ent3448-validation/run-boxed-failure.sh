#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

expected_source=$1
evidence_dir=$2
venv=$3
cd "$(dirname "$0")/../.."
[[ "$(git rev-parse HEAD)" == "$expected_source" ]]
test -z "$(git status --porcelain)"
mkdir -p "$evidence_dir"
git rev-parse HEAD > "$evidence_dir/source.sha"
trap 'result=$?; printf "%s\n" "$result" > "$evidence_dir/suite.exit"' EXIT

export RUSTUP_TOOLCHAIN=1.97.0
export CARGO_BUILD_JOBS=4
export TMPDIR=/home/ec2-user/.cache/sophon-task-tmp/ent3448/boxed
export PATH="/home/ec2-user/.local/node/node-v24.11.1-linux-x64/bin:$PATH"
export PYO3_PYTHON="$venv/bin/python"
python_lib=$("$PYO3_PYTHON" -c 'import sys; print(sys.base_prefix + "/lib")')
export LD_LIBRARY_PATH="$python_lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p "$TMPDIR"
rustc --version > "$evidence_dir/rustc.version"

run_check() {
    local check_name=$1
    shift
    set +e
    "$@" > "$evidence_dir/$check_name.log" 2>&1
    local result=$?
    set -e
    printf '%s\n' "$result" > "$evidence_dir/$check_name.exit"
    return "$result"
}

run_check clippy mbx clippy --quiet -p lancedb --tests --features remote -- -D warnings
for test_name in test_job_wait_surfaces_failure test_job_wait_reports_the_server_failure_reason test_function_errors_lists_the_rows_a_refresh_skipped test_execute_async_job_reports_failure_to_every_waiter; do
    run_check "$test_name" mbx test --quiet -p lancedb --lib --features remote "$test_name"
    grep -q 'test result: ok. 1 passed;' "$evidence_dir/$test_name.log"
done
run_check python-build uv pip install --python "$PYO3_PYTHON" --no-deps --reinstall-package lancedb -e python
run_check python-tests "$venv/bin/pytest" -q python/python/tests/test_remote_db.py -k 'remote_function_errors_lists_the_rows_a_refresh_skipped or remote_connection_jobs_surface'
"$PYO3_PYTHON" -c 'import hashlib, pathlib, lancedb._lancedb as native; p=pathlib.Path(native.__file__).resolve(); print(p); print(hashlib.sha256(p.read_bytes()).hexdigest())' > "$evidence_dir/python-native.sha256"
cd nodejs
run_check node-build pnpm build
sha256sum dist/*.node > "$evidence_dir/node-native.sha256"
run_check node-docs pnpm run docs
git diff --exit-code
