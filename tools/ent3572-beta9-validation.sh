#!/usr/bin/env bash
set -u

repo=${1:?repo path}
out=${2:?output directory}
mkdir -p "$out"

# The workspace uses PyO3/abi3-py310; the host default python3 is 3.9.
export PYO3_PYTHON=${PYO3_PYTHON:-/home/ec2-user/.local/bin/python3.12}

run() {
    local name=$1
    shift
    local log="$out/$name.log"
    local exit_file="$out/$name.exit"
    "$@" >"$log" 2>&1
    local rc=$?
    printf '%s\n' "$rc" >"$exit_file"
    printf '%s=%s\n' "$name" "$rc"
    return "$rc"
}

cd "$repo"
status=0

run fmt env CARGO_BUILD_JOBS=4 mbx +1.97.1 fmt --all -- --check || status=1
run clippy env CARGO_BUILD_JOBS=4 mbx +1.97.1 clippy --quiet --workspace --tests --all-features -- -D warnings || status=1
run lsm_mem_wal env CARGO_BUILD_JOBS=4 mbx +1.97.1 test --quiet --locked -p lancedb --lib table::merge::lsm_tests::lsm_writer_of_a_retired_mem_wal_is_replaced -- --nocapture || status=1
run ci_tests python3 -m unittest discover -s ci/tests -v || status=1
run uv_lock bash -c 'cd python && uv lock --check' || status=1
run python_ruff bash -c 'cd python && uv run --extra dev ruff format --check . && uv run --extra dev ruff check .' || status=1

printf 'OVERALL_EXIT=%s\n' "$status"
exit "$status"
