#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
# Private execution driver; exclude from the product PR.
set -euo pipefail

source_repo=${1:?provide the product checkout}
evidence=${2:?provide an absolute evidence directory}
expected_source=a42d6800fe5ea5bef4eb64a9b51e8df2c2584249
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
