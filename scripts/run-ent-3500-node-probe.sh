#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <output_dir>" >&2
  exit 2
fi

out=$1
expected_sha=${EXPECTED_SHA:-3bc7173d84f70ac2afaba52d79b5a2e86663ff9c}
repo=${SOURCE_DIR:?SOURCE_DIR is required}
repo=$(cd "$repo" && pwd)
cd "$repo"

actual_sha=$(git rev-parse HEAD)
if [[ $actual_sha != "$expected_sha" ]]; then
  echo "source mismatch: expected $expected_sha, found $actual_sha" >&2
  exit 3
fi

mkdir -p "$out"
run_status=0
export TMPDIR=/var/tmp
export RUSTFLAGS=""
export RUSTUP_TOOLCHAIN=1.97.0
export PATH="/home/ec2-user/tools/node-v24.22.2-linux-x64/bin:$PATH"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0

cd nodejs
set +e
pnpm test -- __test__/remote.test.ts \
  -t "keeps a thrown progress observer from escaping wait" --runInBand \
  2>&1 | tee "$out/probe.log"
pipeline_status=("${PIPESTATUS[@]}")
set -e
run_status=${pipeline_status[0]}
if [[ $run_status -eq 0 && ${pipeline_status[1]} -ne 0 ]]; then
  run_status=${pipeline_status[1]}
fi
printf '%s\n' "$run_status" > "$out/probe.exit"
exit "$run_status"
