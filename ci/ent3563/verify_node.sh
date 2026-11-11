#!/usr/bin/env bash

set -euo pipefail

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

cd "$repo_root/nodejs"
pnpm install --frozen-lockfile --reporter append-only \
  >"$out_dir/pnpm-install.log" 2>&1
printf '%s\n' "$?" >"$out_dir/pnpm-install.log.exit"

pnpm build:debug --profile ci --features remote \
  >"$out_dir/node-build.log" 2>&1
printf '%s\n' "$?" >"$out_dir/node-build.log.exit"

pnpm exec biome check lancedb/table.ts __test__/remote.test.ts \
  >"$out_dir/biome.log" 2>&1
printf '%s\n' "$?" >"$out_dir/biome.log.exit"

pnpm tsc -b >"$out_dir/tsc.log" 2>&1
printf '%s\n' "$?" >"$out_dir/tsc.log.exit"

find lancedb -maxdepth 1 -name '*.node' -type f -print \
  >"$out_dir/addon-paths.txt"
xargs -r sha256sum <"$out_dir/addon-paths.txt" \
  >"$out_dir/addon-sha256.txt"

set +e
pnpm exec jest \
  __test__/remote.test.ts \
  --runInBand \
  -t 'passes a pinned version and decodes Function scheduler status' \
  >"$out_dir/jest.log" 2>&1
status=$?
set -e
if [[ "$status" -eq 0 ]] && ! grep -Eq "Tests:.*1 passed, 1 total" "$out_dir/jest.log"; then
  printf '%s\n' "expected one Node test to pass" >"$out_dir/selection-error.txt"
  status=1
fi
printf '%s\n' "$status" >"$out_dir/jest.log.exit"

git status --porcelain=v1 >"$out_dir/source-status-after.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$out_dir/finished-at.txt"
printf '%s\n' "$status" >"$out_dir/runner.exit"
exit "$status"
