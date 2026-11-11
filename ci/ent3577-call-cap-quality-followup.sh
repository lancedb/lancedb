#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
head="$(git -C "$repo_root" rev-parse HEAD)"
evidence_root="${ENT3577_EVIDENCE_ROOT:-/home/ec2-user/ent3577-call-cap-evidence/$head-quality}"
mkdir -p "$evidence_root"
node_bin_dir="${ENT3577_NODE_BIN_DIR:-/home/ec2-user/tools/node-v24.22.2-linux-x64/bin}"
if [[ -x "$node_bin_dir/node" ]]; then
  export PATH="$node_bin_dir:$PATH"
fi

printf '%s\n' "$head" >"$evidence_root/source-head.txt"
git -C "$repo_root" rev-parse 'HEAD^{tree}' >"$evidence_root/source-tree.txt"
git -C "$repo_root" status --porcelain=v1 >"$evidence_root/source-status-before.txt"

run_stage_in() {
  local stage="$1"
  local directory="$2"
  shift 2

  local started
  local finished
  local status
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\tstarted\t%s\t%s\n' "$stage" "$started" "$directory" |
    tee -a "$evidence_root/status.tsv"
  set +e
  (
    cd "$directory"
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

python_extension="$(
  cd "$repo_root/python"
  uv run --extra tests python -c \
    'import lancedb._lancedb as native; print(native.__file__)'
)"
printf '%s\n' "$python_extension" >"$evidence_root/python-extension-path.txt"
sha256sum "$python_extension" >"$evidence_root/python-extension-before.sha256"

run_stage_in python_ruff "$repo_root/python" \
  uv run --extra tests --extra dev ruff check \
  python/lancedb/functions.py python/lancedb/table.py \
  python/lancedb/remote/table.py \
  python/tests/test_first_class_function_slice1.py \
  python/tests/test_first_class_function_slice2.py \
  python/tests/test_remote_db.py python/tests/test_table.py
run_stage_in python_ruff_format "$repo_root/python" \
  uv run --extra tests --extra dev ruff format --check \
  python/lancedb/functions.py python/lancedb/table.py \
  python/lancedb/remote/table.py \
  python/tests/test_first_class_function_slice1.py \
  python/tests/test_first_class_function_slice2.py \
  python/tests/test_remote_db.py python/tests/test_table.py
run_stage_in python_types "$repo_root/python" \
  uv run --extra tests --extra dev pyright

run_stage_in node_build "$repo_root/nodejs" pnpm build
run_stage_in node_lint "$repo_root/nodejs" pnpm lint-ci
run_stage_in node_remote_refresh "$repo_root/nodejs" \
  pnpm test __test__/remote.test.ts --runInBand
run_stage_in node_local_rejection "$repo_root/nodejs" \
  pnpm test __test__/table.test.ts --runInBand \
  -t 'rejects Enterprise refresh options on local tables'

sha256sum "$python_extension" >"$evidence_root/python-extension-after.sha256"
find "$repo_root/nodejs" -name '*.node' -type f -print0 |
  sort -z |
  xargs -0 sha256sum >"$evidence_root/node-addons.sha256"
git -C "$repo_root" status --porcelain=v1 >"$evidence_root/source-status-after.txt"
printf '0\n' >"$evidence_root/runner.exit"
