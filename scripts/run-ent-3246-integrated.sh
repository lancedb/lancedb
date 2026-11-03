#!/usr/bin/env bash
set -euo pipefail

target=${1:?target checkout}
output=${2:?new evidence directory}
expected=fd2de091c641cafb0c2d3eda564446659ce616b5
export PATH="$HOME/.local/share/mbx/bin:$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.97.0
export TMPDIR=/var/tmp
mkdir -p "$output"
test ! -e "$output/run.log"
exec > >(tee "$output/run.log") 2>&1
trap 'result=$?; printf "%s\n" "$result" > "$output/exit.tmp"; mv "$output/exit.tmp" "$output/exit"' EXIT
cd "$target"
test "$(git rev-parse HEAD)" = "$expected"
test -z "$(git status --porcelain)"
test -z "${CARGO_TARGET_DIR:-}"
printf 'head=%s\n' "$(git rev-parse HEAD)"
cargo --version
rustc --version
command -v cargo
uv python find 3.12

run_step() {
    local name=$1
    shift
    local result=0
    "$@" > "$output/$name.log" 2>&1 || result=$?
    printf '%s\n' "$result" > "$output/$name.exit"
    printf '%s_exit=%s\n' "$name" "$result"
    tail -6 "$output/$name.log"
    return "$result"
}

run_step fmt cargo fmt --all -- --check
run_step rust-contract cargo test --locked -p lancedb --features remote --test first_class_function_slice2 -- --nocapture
grep -Eq 'test result: ok\. 6 passed; 0 failed; 0 ignored;' "$output/rust-contract.log"
run_step rust-sql cargo test --locked -p lancedb --features remote --lib remote::sql::tests::function_creation_ -- --nocapture
grep -Eq 'test result: ok\. 2 passed; 0 failed; 0 ignored;' "$output/rust-sql.log"

cd python
run_step native uv run --frozen --python 3.12 --extra tests --extra dev maturin develop --extras tests,dev
uv run --frozen --python 3.12 --extra tests --extra dev python -c 'import lancedb, lancedb._lancedb; print(lancedb.__file__); print(lancedb._lancedb.__file__)'
run_step python-contract uv run --frozen --python 3.12 --extra tests --extra dev pytest python/tests/test_first_class_function_slice2.py -q
grep -Eq '[1-9][0-9]* passed' "$output/python-contract.log"
cd ..
run_step ruff-format uv run --project python --frozen --python 3.12 --extra dev ruff format --check .
run_step ruff-check uv run --project python --frozen --python 3.12 --extra dev ruff check .
git diff --check
test -z "$(git status --porcelain)"
