#!/usr/bin/env bash

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-all}"
STATUS=0

cd "$ROOT"
echo "HEAD=$(git rev-parse HEAD)"
echo "STATUS=$(git status --porcelain)"
echo "RUSTUP_TOOLCHAIN=1.97.0"
if [[ -n "${EXPECTED_SHA:-}" && "$(git rev-parse HEAD)" != "$EXPECTED_SHA" ]]; then
    echo "HEAD does not match EXPECTED_SHA=$EXPECTED_SHA" >&2
    exit 64
fi

if [[ "$MODE" == "all" || "$MODE" == "rust" ]]; then
    echo "=== rust FunctionVersion catalog paths ==="
    (
        cd "$ROOT/rust"
        RUSTUP_TOOLCHAIN=1.97.0 cargo test -j 4 -q -p lancedb --features remote \
            remote::sql::tests::function_creation_and_description_use_sql_query_handles \
            -- --exact
    )
    rust_status=$?
    echo "RUST_STATUS=$rust_status"
    STATUS=$((STATUS == 0 ? rust_status : STATUS))
fi

if [[ "$MODE" == "all" || "$MODE" == "python" ]]; then
    echo "=== build native Python extension ==="
    (
        cd "$ROOT/python"
        RUSTUP_TOOLCHAIN=1.97.0 CARGO_BUILD_JOBS=4 \
            uv run --extra tests --extra dev maturin develop --extras tests,dev
    )
    build_status=$?
    echo "PYTHON_BUILD_STATUS=$build_status"
    STATUS=$((STATUS == 0 ? build_status : STATUS))
    if [[ "$build_status" -eq 0 ]]; then
        echo "=== python FunctionVersion contract paths ==="
        (
            cd "$ROOT/python"
            RUSTUP_TOOLCHAIN=1.97.0 uv run --extra tests pytest -q \
                python/tests/test_first_class_function_slice1.py::test_function_version_preserves_execution_controls \
                python/tests/test_first_class_function_slice2.py::test_async_catalog_path_preserves_execution_controls \
                python/tests/test_first_class_function_slice2.py::test_sync_catalog_path_preserves_execution_controls
        )
        python_status=$?
        echo "PYTHON_TEST_STATUS=$python_status"
        STATUS=$((STATUS == 0 ? python_status : STATUS))
    else
        STATUS=$((STATUS == 0 ? build_status : STATUS))
    fi
fi

echo "FINAL_STATUS=$STATUS"
exit "$STATUS"
