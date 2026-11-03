#!/usr/bin/env bash
set -u

repo=${1:?repo path}
out=${2:?output directory}
mkdir -p "$out"
export PYO3_PYTHON=${PYO3_PYTHON:-/home/ec2-user/.local/bin/python3.12}

run() {
    local name=$1
    shift
    "$@" >"$out/$name.log" 2>&1
    local rc=$?
    printf '%s\n' "$rc" >"$out/$name.exit"
    printf '%s=%s\n' "$name" "$rc"
    return "$rc"
}

cd "$repo"
status=0

run clippy_pyo312 env CARGO_BUILD_JOBS=4 mbx +1.97.1 clippy --quiet --workspace --tests --all-features -- -D warnings || status=1
run ci_tests_uv uv run --no-project --python 3.12 --with packaging python -m unittest discover -s ci/tests -v || status=1
run uv_lock bash -c 'cd python && uv lock --check' || status=1
run python_ruff bash -c 'cd python && uv run --no-project --python 3.12 --with ruff==0.9.9 ruff format --check . && uv run --no-project --python 3.12 --with ruff==0.9.9 ruff check .' || status=1

printf 'OVERALL_EXIT=%s\n' "$status"
exit "$status"
