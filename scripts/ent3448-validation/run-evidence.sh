#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
run_dir=${1:?usage: run-evidence.sh RUN_DIR}
venv=${ENT3448_SDK_VENV:?set ENT3448_SDK_VENV to the built SDK virtualenv}
native_source=${ENT3448_NATIVE_SOURCE:?set ENT3448_NATIVE_SOURCE to the source used to build the extension}
mkdir -p "$run_dir"
run_dir=$(cd "$run_dir" && pwd)
exit_file="$run_dir/suite.exit"
exit_tmp="$exit_file.tmp"

finish() {
    status=$?
    printf '%s\n' "$status" >"$exit_tmp"
    mv "$exit_tmp" "$exit_file"
    exit "$status"
}
trap finish EXIT

run_step() {
    name=$1
    shift
    log="$run_dir/$name.log"
    exit="$run_dir/$name.exit"
    if "$@" >"$log" 2>&1; then
        status=0
    else
        status=$?
    fi
    printf '%s\n' "$status" >"$exit"
    cat "$log"
    return "$status"
}

cd "$repo_root"
test -z "$(git status --porcelain)"
git rev-parse HEAD >"$run_dir/source.sha"
printf '%s\n' "$native_source" >"$run_dir/native-source.sha"
printf '%s\n' 'f8e06baad9147437148e68f130ee29dba219b997' >"$run_dir/formal-source.sha"
git merge-base --is-ancestor "$native_source" HEAD

task_tmp=${TMPDIR:-/home/ec2-user/.cache/sophon-task-tmp/ent3448/sdk}
mkdir -p "$task_tmp"
export TMPDIR=$task_tmp
export CARGO_BUILD_JOBS=4

run_step rust-job-wait-surfaces \
    mbx test --quiet -p lancedb --lib --features remote test_job_wait_surfaces_failure
run_step rust-job-wait-reason \
    mbx test --quiet -p lancedb --lib --features remote test_job_wait_reports_the_server_failure_reason
run_step rust-function-errors \
    mbx test --quiet -p lancedb --lib --features remote test_function_errors_lists_the_rows_a_refresh_skipped
run_step nodejs-check \
    mbx check --quiet -p lancedb-nodejs

run_step native-binding \
    "$venv/bin/python" -c '
import hashlib
import pathlib
import lancedb
import lancedb._lancedb as native

path = pathlib.Path(native.__file__).resolve()
print(f"lancedb_version={lancedb.__version__}")
print(f"lancedb_package={pathlib.Path(lancedb.__file__).resolve()}")
print(f"native_binding={path}")
print(f"native_sha256={hashlib.sha256(path.read_bytes()).hexdigest()}")
'
run_step python-remote-tests \
    "$venv/bin/pytest" -q python/python/tests/test_remote_db.py \
    -k 'remote_function_errors_lists_the_rows_a_refresh_skipped or remote_connection_jobs_surface'

git diff --exit-code
test -z "$(git status --porcelain)"
