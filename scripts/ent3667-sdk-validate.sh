#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
# Private execution driver; exclude from the product PR.
set -euo pipefail

source_repo=${1:?provide the product checkout}
evidence=${2:?provide an absolute evidence directory}
expected_source=2b6113ff264edca198f742375b967e0ac765c499
driver_repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
mkdir -p "$evidence"
test ! -e "$evidence/run.log"
exec >"$evidence/run.log" 2>&1
trap 'result=$?; printf "%s\n" "$result" >"$evidence/exit.tmp"; mv "$evidence/exit.tmp" "$evidence/exit"' EXIT
git -C "$driver_repo" rev-parse HEAD >"$evidence/driver"
cd "$source_repo"
git rev-parse HEAD >"$evidence/commit"
test "$(cat "$evidence/commit")" = "$expected_source"
test -z "$(git status --porcelain)"
export CARGO_BUILD_JOBS=2
date -u +%FT%TZ
if [ "${3:-all}" = review ]; then
    cargo test --locked -p lancedb --features remote --lib remote::table::tests::test_add_columns_fails_closed_on_newer_function_binding_metadata -- --exact --nocapture >"$evidence/future-version.log" 2>&1
    grep -q 'test result: ok. 1 passed; 0 failed;' "$evidence/future-version.log"
    cargo test --locked -p lancedb --features remote --lib remote::table::tests::test_add_columns_scopes_to_the_columns_a_binding_uses -- --nocapture >"$evidence/metadata-v1-v2.log" 2>&1
    grep -q 'test result: ok. 2 passed; 0 failed;' "$evidence/metadata-v1-v2.log"
    cargo clippy --locked -p lancedb --features remote --lib --tests --no-deps -- -D warnings >"$evidence/clippy.log" 2>&1
    cd python
    # Only Rust test code and Python exports changed; retain the verified a42 native extension.
    uv run --no-sync python -c 'import hashlib,lancedb._lancedb as n; from pathlib import Path; p=Path(n.__file__); h=hashlib.sha256(p.read_bytes()).hexdigest(); print(p,h); assert h=="6392ee2d37865e657bcb759e7df9d8a950fb5f4edae6c91898b89ed274b43384"' >"$evidence/native-extension.txt"
    uv run --no-sync pytest python/tests/test_first_class_function_slice1.py::test_public_function_values_are_in_api_reference -q --junitxml="$evidence/python.xml" >"$evidence/python.log" 2>&1
    uv run --no-sync python -c 'import sys,xml.etree.ElementTree as E; suites=list(E.parse(sys.argv[1]).getroot().iter("testsuite")); assert sum(int(s.get("tests",0)) for s in suites)==1; assert not sum(int(s.get(k,0)) for s in suites for k in ("errors","failures","skipped"))' "$evidence/python.xml"
    cd "$source_repo"
    git diff --exit-code
    git diff --cached --exit-code
    exit 0
fi
if [ "${3:-all}" = lint ]; then
    cargo test --locked -p lancedb --features remote --lib table::computed_columns::tests::test_literal_function_metadata_survives_schema_mutation_guards -- --exact --nocapture >"$evidence/test.log" 2>&1
    grep -q 'test result: ok. 1 passed; 0 failed;' "$evidence/test.log"
    cargo clippy --locked -p lancedb --features remote --lib --tests --no-deps -- -D warnings >"$evidence/clippy.log" 2>&1
    git diff --exit-code
    git diff --cached --exit-code
    exit 0
fi
cargo test --locked -p lancedb --features remote --lib table::computed_columns::tests -- --nocapture 2>&1 | tee "$evidence/rust.log"
for selector in \
    test_literal_function_application_keeps_only_column_dependencies \
    test_literal_function_metadata_survives_schema_mutation_guards \
    test_literal_function_binding_requires_complete_input_schema; do
    grep -Fq "tests::$selector ... ok" "$evidence/rust.log"
done
cargo test --locked -p lancedb --features remote --lib remote::table::tests::test_add_scalar_function_column_sends_atomic_null_declaration -- --nocapture 2>&1 | tee "$evidence/transport.log"
grep -q 'test result: ok. 4 passed; 0 failed;' "$evidence/transport.log"
cd python
uv sync --locked --no-install-project --extra tests --extra dev
uv run --no-sync maturin develop --locked --extras tests,dev 2>&1 | tee "$evidence/native-python-build.log"
uv run --no-sync python -c 'import lancedb._lancedb as native; print(native.__file__)' >"$evidence/native-extension"
sha256sum "$(cat "$evidence/native-extension")" >"$evidence/native-extension.sha256"
uv run --no-sync pytest python/tests/test_first_class_function_slice1.py -q --junitxml="$evidence/python.xml" 2>&1 | tee "$evidence/python.log"
uv run --no-sync python -c 'import sys,xml.etree.ElementTree as E; suites=list(E.parse(sys.argv[1]).getroot().iter("testsuite")); assert sum(int(s.get("tests",0)) for s in suites)>=9; assert not sum(int(s.get(k,0)) for s in suites for k in ("errors","failures","skipped"))' "$evidence/python.xml"
cd "$source_repo"
cargo clippy --locked -p lancedb --features remote --lib --tests --no-deps -- -D warnings 2>&1 | tee "$evidence/clippy.log"
git diff --exit-code
git diff --cached --exit-code
date -u +%FT%TZ
