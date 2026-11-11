#!/usr/bin/env bash

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="${1:?usage: verify_rust.sh OUTPUT_DIR}"
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

tests=(
  "table::freshness::tests::a_first_partial_write_keeps_untouched_legacy_rows_unknown"
  "table::freshness::tests::a_successful_retry_clears_only_its_failed_rows"
  "table::freshness::tests::an_old_attempt_cannot_rewrite_a_newer_signature_evidence"
  "remote::table::tests::test_wait_for_function_column_retries_only_typed_finalization_pending"
  "remote::table::tests::test_finalization_code_on_other_status_is_not_retried"
  "remote::table::tests::test_wait_for_function_column_does_not_retry_generic_500"
  "remote::table::tests::test_function_column_status_decodes_agent_up_to_date"
  "remote::table::tests::test_function_column_status_decodes_blocked_scheduler_fields"
  "remote::table::tests::test_function_column_status_rejects_a_pinned_table_reference"
)

overall=0
for filter in "${tests[@]}"; do
  name="${filter##*::}"
  log="$out_dir/$name.log"
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
  printf '%s\n' "$status" >"$log.exit"
  if [[ "$status" -ne 0 ]]; then
    overall=1
  elif ! grep -Fq "test result: ok. 1 passed; 0 failed" "$log"; then
    printf '%s\n' "expected exactly one actual test, got:" >"$out_dir/$name.selection-error"
    grep -F "test result:" "$log" >>"$out_dir/$name.selection-error" || true
    overall=1
  fi
done

date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/finished-at.txt"
printf '%s\n' "$overall" >"$out_dir/runner.exit"
exit "$overall"
