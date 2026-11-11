#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="${1:?usage: verify_rust_pinned_delta.sh OUTPUT_DIR}"
mkdir -p "$out_dir"

cd "$repo_root"
git rev-parse HEAD >"$out_dir/source-head.txt"
git status --porcelain=v1 >"$out_dir/source-status.txt"
sha256sum "${BASH_SOURCE[0]}" >"$out_dir/runner-sha256.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/started-at.txt"

if [[ -s "$out_dir/source-status.txt" ]]; then
  printf 'source checkout is dirty\n' >"$out_dir/runner.error"
  exit 2
fi

filter="remote::table::tests::test_function_column_status_passes_a_pinned_version"
log="$out_dir/pinned-version.log"
set +e
cargo test \
  --locked \
  --profile ci \
  -p lancedb \
  --features remote \
  --lib \
  "$filter" \
  -- \
  --exact 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e
printf '%s\n' "$status" >"$log.exit"

if [[ "$status" -eq 0 ]] && ! grep -Fq "test result: ok. 1 passed; 0 failed" "$log"; then
  printf '%s\n' "expected exactly one actual test, got:" >"$out_dir/selection-error.txt"
  grep -F "test result:" "$log" >>"$out_dir/selection-error.txt" || true
  status=1
fi

date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/finished-at.txt"
printf '%s\n' "$status" >"$out_dir/runner.exit"
exit "$status"
