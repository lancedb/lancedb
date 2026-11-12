#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
# Private task validation; exclude this script from the product PR.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
evidence=${1:?provide an absolute evidence directory outside build caches}
mkdir -p "$evidence"
exec >"$evidence/run.log" 2>&1
trap 'result=$?; printf "%s\n" "$result" >"$evidence/exit.tmp"; mv "$evidence/exit.tmp" "$evidence/exit"' EXIT
git -C "$repo" rev-parse HEAD >"$evidence/commit"
git -C "$repo" diff --exit-code
git -C "$repo" diff --cached --exit-code
date -u +%FT%TZ
cd "$repo"
cargo test --quiet -p lancedb --features remote --lib server_info -- --nocapture 2>&1 | tee "$evidence/rust.log"
grep -q 'test result: ok. 3 passed; 0 failed;' "$evidence/rust.log"
date -u +%FT%TZ
cd "$repo/python"
uv run --extra tests --extra dev maturin develop --extras tests,dev
uv run --extra tests pytest python/tests/test_remote_db.py -k remote_server_info -q --junitxml="$evidence/python.xml"
date -u +%FT%TZ
