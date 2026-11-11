#!/usr/bin/env bash

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="${1:?usage: verify_node.sh OUTPUT_DIR}"
mkdir -p "$out_dir"

export PATH="/home/ec2-user/.local/node/node-v24.11.1-linux-x64/bin:/home/ec2-user/.local/bin:$PATH"
export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=/home/ec2-user/Code/targets/ent3563-freshness-bb527bb

cd "$repo_root"
git rev-parse HEAD >"$out_dir/source-head.txt"
git status --porcelain=v1 >"$out_dir/source-status-before.txt"
sha256sum "${BASH_SOURCE[0]}" >"$out_dir/runner-sha256.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/started-at.txt"

if [[ -s "$out_dir/source-status-before.txt" ]]; then
  printf 'source checkout is dirty\n' >"$out_dir/runner.error"
  exit 2
fi

finish() {
  local status="$1"
  git status --porcelain=v1 >"$out_dir/source-status-after.txt"
  date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/finished-at.txt"
  printf '%s\n' "$status" >"$out_dir/runner.exit"
  exit "$status"
}

capture() {
  local name="$1"
  shift
  "$@" >"$out_dir/$name.log" 2>&1
  local status=$?
  printf '%s\n' "$status" >"$out_dir/$name.log.exit"
  return "$status"
}

cd "$repo_root/nodejs"
capture pnpm-install pnpm install --frozen-lockfile --reporter append-only \
  || finish 1

capture node-build pnpm build:debug --profile ci --features remote \
  || finish 1

capture biome pnpm exec biome check lancedb/table.ts __test__/remote.test.ts \
  || finish 1

capture tsc pnpm tsc || finish 1

find lancedb -maxdepth 1 -name '*.node' -type f -print \
  >"$out_dir/addon-paths.txt"
if [[ ! -s "$out_dir/addon-paths.txt" ]]; then
  printf '%s\n' "expected a built Node addon" >"$out_dir/selection-error.txt"
  finish 1
fi
xargs -r sha256sum <"$out_dir/addon-paths.txt" \
  >"$out_dir/addon-sha256.txt" || finish 1

capture jest pnpm exec jest \
  __test__/remote.test.ts \
  --runInBand \
  --json \
  --outputFile "$out_dir/jest-results.json" \
  -t 'passes a pinned version and decodes Function scheduler status' \
  || finish 1
if ! node -e \
  'const result = require(process.argv[1]); process.exit(result.numPassedTests === 1 && result.numFailedTests === 0 ? 0 : 1)' \
  "$out_dir/jest-results.json"; then
  printf '%s\n' "expected one Node test to pass with zero failures" \
    >"$out_dir/selection-error.txt"
  finish 1
fi

finish 0
