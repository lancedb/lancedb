#!/usr/bin/env bash
set -u

source_root="${1:-/home/ec2-user/Code/wt/ent-3500-sdk/lancedb}"
evidence_root="${2:-/home/ec2-user/Code/wt/ent-3486-sdk-evidence/warm-warning-manual}"
mkdir -p "$evidence_root"
cd "$source_root" || exit 1

export TMPDIR=/home/ec2-user/.cache/sophon-task-tmp/ent3486
export RUST_MIN_STACK=16777216
mkdir -p "$TMPDIR"

overall=0

run_step() {
    local name="$1"
    shift
    printf '%s\n' "$*" >"$evidence_root/$name.command"
    "$@" >"$evidence_root/$name.log" 2>&1
    local status=$?
    printf '%s\n' "$status" >"$evidence_root/$name.exit"
    if [ "$status" -ne 0 ]; then
        overall=1
    fi
    return "$status"
}

run_step_in() {
    local directory="$1"
    local name="$2"
    shift 2
    printf '%s\n' "$*" >"$evidence_root/$name.command"
    (cd "$directory" && "$@") >"$evidence_root/$name.log" 2>&1
    local status=$?
    printf '%s\n' "$status" >"$evidence_root/$name.exit"
    if [ "$status" -ne 0 ]; then
        overall=1
    fi
    return "$status"
}

git status --short >"$evidence_root/source.status"
git rev-parse HEAD >"$evidence_root/source.head"
git rev-parse HEAD^{tree} >"$evidence_root/source.tree"
rustc +1.97.1 --version >"$evidence_root/rustc.version" 2>&1
uv --version >"$evidence_root/uv.version" 2>&1

run_step fmt rustup run 1.97.1 cargo fmt --all -- --check
run_step diff-check git diff --check
run_step rust-test mbx +1.97.1 test --locked --features remote -p lancedb \
    remote::table::tests::test_refresh_column_async_submits_a_backfill_job \
    -- --exact --nocapture
run_step python-lint ruff check \
    python/python/lancedb/job.py \
    python/python/lancedb/table.py \
    python/python/tests/test_remote_db.py
run_step python-format ruff format --check \
    python/python/lancedb/job.py \
    python/python/lancedb/table.py \
    python/python/tests/test_remote_db.py

run_step_in "$source_root/python" python-build \
    uv run --frozen --extra tests --extra dev maturin develop --extras tests,dev
run_step_in "$source_root/python" python-test \
    uv run --frozen --extra tests pytest \
    python/tests/test_remote_db.py::test_remote_refresh_async_returns_typed_terminal_result \
    -q

run_step clippy mbx +1.97.1 clippy --locked --features remote -p lancedb \
    --lib --tests -- -D warnings

printf '%s\n' "$overall" >"$evidence_root/overall.exit"
exit "$overall"
