#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
head="$(git -C "$repo_root" rev-parse HEAD)"
evidence_root="${ENT3577_EVIDENCE_ROOT:-/home/ec2-user/ent3577-call-cap-evidence/$head-rust-remote}"
mkdir -p "$evidence_root"

printf '%s\n' "$head" >"$evidence_root/source-head.txt"
git -C "$repo_root" rev-parse 'HEAD^{tree}' >"$evidence_root/source-tree.txt"
git -C "$repo_root" status --porcelain=v1 >"$evidence_root/source-status-before.txt"

run_stage() {
  local stage="$1"
  shift

  local started
  local finished
  local status
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\tstarted\t%s\n' "$stage" "$started" |
    tee -a "$evidence_root/status.tsv"
  set +e
  (
    cd "$repo_root"
    "$@"
  ) >"$evidence_root/$stage.log" 2>&1
  status=$?
  set -e
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\n' "$status" >"$evidence_root/$stage.exit"
  printf '%s\tfinished\t%s\tstatus=%s\n' "$stage" "$finished" "$status" |
    tee -a "$evidence_root/status.tsv"
  return "$status"
}

run_stage rust_registration_sql \
  cargo test -p lancedb --features remote --lib \
  function_creation_and_description_use_sql_query_handles
run_stage rust_registration_invalid \
  cargo test -p lancedb --features remote --lib \
  function_creation_rejects_invalid_execution_controls_before_sql
run_stage rust_refresh_wire \
  cargo test -p lancedb --features remote --lib \
  test_refresh_column_async_filter_and_failure_options
run_stage rust_refresh_invalid \
  cargo test -p lancedb --features remote --lib \
  test_refresh_column_rejects_invalid_filter_and_limit_options_before_request

git -C "$repo_root" status --porcelain=v1 >"$evidence_root/source-status-after.txt"
printf '0\n' >"$evidence_root/runner.exit"
